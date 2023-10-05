// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::fmt::Debug;
use std::io;
use std::sync::Arc;

use common_meta_kvapi::kvapi;
use common_meta_kvapi::kvapi::GetKVReply;
use common_meta_kvapi::kvapi::ListKVReply;
use common_meta_kvapi::kvapi::MGetKVReply;
use common_meta_kvapi::kvapi::UpsertKVReply;
use common_meta_kvapi::kvapi::UpsertKVReq;
use common_meta_stoerr::MetaBytesError;
use common_meta_types::AppliedState;
use common_meta_types::Entry;
use common_meta_types::LogId;
use common_meta_types::MatchSeqExt;
use common_meta_types::Node;
use common_meta_types::NodeId;
use common_meta_types::Operation;
use common_meta_types::SeqV;
use common_meta_types::SeqValue;
use common_meta_types::SnapshotData;
use common_meta_types::StoredMembership;
use common_meta_types::TxnReply;
use common_meta_types::TxnRequest;
use common_meta_types::UpsertKV;
use futures::Stream;
use futures_util::StreamExt;
use log::debug;
use log::info;
use log::warn;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio::sync::RwLock;

use crate::applier_v003::ApplierV003;
use crate::key_spaces::RaftStoreEntry;
use crate::sm_v003::leveled_store::level_data::LevelData;
use crate::sm_v003::leveled_store::leveled_map::LeveledMap;
use crate::sm_v003::leveled_store::map_api::MapApi;
use crate::sm_v003::leveled_store::map_api::MapApiRO;
use crate::sm_v003::leveled_store::sys_data_api::SysDataApiRO;
use crate::sm_v003::marked::Marked;
use crate::sm_v003::sm_v003;
use crate::sm_v003::Importer;
use crate::sm_v003::SnapshotViewV003;
use crate::state_machine::sm::BlockingConfig;
use crate::state_machine::ExpireKey;
use crate::state_machine::StateMachineSubscriber;

/// A wrapper that implements KVApi **readonly** methods for the state machine.
pub struct SMV003KVApi<'a> {
    sm: &'a SMV003,
}

#[async_trait::async_trait]
impl<'a> kvapi::KVApi for SMV003KVApi<'a> {
    type Error = Infallible;

    async fn upsert_kv(&self, _req: UpsertKVReq) -> Result<UpsertKVReply, Self::Error> {
        unreachable!("write operation SM2KVApi::upsert_kv is disabled")
    }

    async fn get_kv(&self, key: &str) -> Result<GetKVReply, Self::Error> {
        let got = self.sm.get_kv(key).await;

        let local_now_ms = SeqV::<()>::now_ms();
        let got = Self::non_expired(got, local_now_ms);
        Ok(got)
    }

    async fn mget_kv(&self, keys: &[String]) -> Result<MGetKVReply, Self::Error> {
        let local_now_ms = SeqV::<()>::now_ms();

        let mut values = Vec::with_capacity(keys.len());

        for k in keys {
            let got = self.sm.get_kv(k.as_str()).await;
            let v = Self::non_expired(got, local_now_ms);
            values.push(v);
        }

        Ok(values)
    }

    async fn prefix_list_kv(&self, prefix: &str) -> Result<ListKVReply, Self::Error> {
        let local_now_ms = SeqV::<()>::now_ms();

        let kvs = self
            .sm
            .prefix_list_kv(prefix)
            .await
            .into_iter()
            .filter(|(_k, v)| !v.is_expired(local_now_ms));

        Ok(kvs.collect())
    }

    async fn transaction(&self, _txn: TxnRequest) -> Result<TxnReply, Self::Error> {
        unreachable!("write operation SM2KVApi::transaction is disabled")
    }
}

impl<'a> SMV003KVApi<'a> {
    fn non_expired<V>(seq_value: Option<SeqV<V>>, now_ms: u64) -> Option<SeqV<V>> {
        if seq_value.is_expired(now_ms) {
            None
        } else {
            seq_value
        }
    }
}

#[derive(Debug, Default)]
pub struct SMV003 {
    pub(in crate::sm_v003) levels: LeveledMap,

    blocking_config: BlockingConfig,

    /// The expiration key since which for next clean.
    pub(in crate::sm_v003) expire_cursor: ExpireKey,

    /// subscriber of state machine data
    pub(crate) subscriber: Option<Box<dyn StateMachineSubscriber>>,
}

impl SMV003 {
    pub fn kv_api(&self) -> SMV003KVApi {
        SMV003KVApi { sm: self }
    }

    /// Install and replace state machine with the content of a snapshot
    ///
    /// After install, the state machine has only one level of data.
    pub async fn install_snapshot(
        state_machine: Arc<RwLock<Self>>,
        data: Box<SnapshotData>,
    ) -> Result<(), io::Error> {
        //
        let data_size = data.data_size().await?;
        info!("snapshot data len: {}", data_size);

        let mut importer = sm_v003::SMV003::new_importer();

        let br = BufReader::new(data);
        let mut lines = AsyncBufReadExt::lines(br);

        while let Some(l) = lines.next_line().await? {
            let ent: RaftStoreEntry = serde_json::from_str(&l)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            importer
                .import(ent)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        }

        let level_data = importer.commit();
        let new_last_applied = *level_data.last_applied_ref();

        {
            let mut sm = state_machine.write().await;

            // When rebuilding the state machine, last_applied is empty,
            // and it should always install the snapshot.
            //
            // And the snapshot may contain data when its last_applied is None,
            // when importing data with metactl:
            // The snapshot is empty but contains Nodes data that are manually added.
            //
            // See: `databend_metactl::snapshot`
            if &new_last_applied <= sm.last_applied_ref() && sm.last_applied_ref().is_some() {
                info!(
                    "no need to install: snapshot({:?}) <= sm({:?})",
                    new_last_applied,
                    sm.last_applied_ref()
                );
                return Ok(());
            }

            sm.replace(LeveledMap::new(level_data));
        }

        info!(
            "installed state machine from snapshot, last_applied: {:?}",
            new_last_applied,
        );

        Ok(())
    }

    pub fn import(data: impl Iterator<Item = RaftStoreEntry>) -> Result<LevelData, MetaBytesError> {
        let mut importer = Self::new_importer();

        for ent in data {
            importer.import(ent)?;
        }

        Ok(importer.commit())
    }

    pub fn new_importer() -> Importer {
        Importer::default()
    }

    /// Return a Arc of the blocking config. It is only used for testing.
    pub fn blocking_config_mut(&mut self) -> &mut BlockingConfig {
        &mut self.blocking_config
    }

    pub fn blocking_config(&self) -> &BlockingConfig {
        &self.blocking_config
    }

    pub async fn apply_entries<'a>(
        &mut self,
        entries: impl IntoIterator<Item = &'a Entry>,
    ) -> Vec<AppliedState> {
        let mut applier = ApplierV003::new(self);

        let mut res = vec![];

        for l in entries.into_iter() {
            let r = applier.apply(l).await;
            res.push(r);
        }
        res
    }

    /// Get a cloned value by key.
    ///
    /// It does not check expiration of the returned entry.
    pub async fn get_kv(&self, key: &str) -> Option<SeqV> {
        let got = MapApiRO::<String>::get(&self.levels, key).await;
        Into::<Option<SeqV>>::into(got)
    }

    // TODO(1): when get an applier, pass in a now_ms to ensure all expired are cleaned.
    /// Update or insert a kv entry.
    ///
    /// If the input entry has expired, it performs a delete operation.
    pub(crate) async fn upsert_kv(&mut self, upsert_kv: UpsertKV) -> (Option<SeqV>, Option<SeqV>) {
        let (prev, result) = self.upsert_kv_primary_index(&upsert_kv).await;

        self.update_expire_index(&upsert_kv.key, &prev, &result)
            .await;

        let prev = Into::<Option<SeqV>>::into(prev);
        let result = Into::<Option<SeqV>>::into(result);

        (prev, result)
    }

    /// List kv entries by prefix.
    ///
    /// If a value is expired, it is not returned.
    pub async fn prefix_list_kv(&self, prefix: &str) -> Vec<(String, SeqV)> {
        let p = prefix.to_string();
        let mut res = Vec::new();
        let strm = MapApiRO::<String>::range(&self.levels, p..).await;

        {
            let mut strm = std::pin::pin!(strm);

            while let Some((k, marked)) = strm.next().await {
                if k.starts_with(prefix) {
                    let seqv = Into::<Option<SeqV>>::into(marked.clone());

                    if let Some(x) = seqv {
                        res.push((k.clone(), x));
                    }
                } else {
                    break;
                }
            }
        }

        res
    }

    pub(crate) fn update_expire_cursor(&mut self, log_time_ms: u64) {
        if log_time_ms < self.expire_cursor.time_ms {
            warn!(
                "update_last_cleaned: log_time_ms {} < last_cleaned_expire.time_ms {}",
                log_time_ms, self.expire_cursor.time_ms
            );
            return;
        }

        self.expire_cursor = ExpireKey::new(log_time_ms, 0);
    }

    /// List expiration index by expiration time.
    pub(crate) async fn list_expire_index(&self) -> impl Stream<Item = (ExpireKey, String)> + '_ {
        self.levels
            .range::<ExpireKey, _>(&self.expire_cursor..)
            .await
            // Return only non-deleted records
            .filter_map(|(k, v)| async move {
                //
                v.unpack().map(|(v, _v_meta)| (k, v))
            })
    }

    pub fn curr_seq(&self) -> u64 {
        self.levels.writable_ref().curr_seq()
    }

    pub fn last_applied_ref(&self) -> &Option<LogId> {
        self.levels.writable_ref().last_applied_ref()
    }

    pub fn last_membership_ref(&self) -> &StoredMembership {
        self.levels.writable_ref().last_membership_ref()
    }

    pub fn nodes_ref(&self) -> &BTreeMap<NodeId, Node> {
        self.levels.writable_ref().nodes_ref()
    }

    pub fn last_applied_mut(&mut self) -> &mut Option<LogId> {
        self.levels.writable_mut().sys_data_mut().last_applied_mut()
    }

    pub fn last_membership_mut(&mut self) -> &mut StoredMembership {
        self.levels
            .writable_mut()
            .sys_data_mut()
            .last_membership_mut()
    }

    pub fn nodes_mut(&mut self) -> &mut BTreeMap<NodeId, Node> {
        self.levels.writable_mut().sys_data_mut().nodes_mut()
    }

    pub fn set_subscriber(&mut self, subscriber: Box<dyn StateMachineSubscriber>) {
        self.subscriber = Some(subscriber);
    }

    /// Creates a snapshot view that contains the latest state.
    ///
    /// Internally, the state machine creates a new empty writable level and makes all current states immutable.
    ///
    /// This operation is fast because it does not copy any data.
    pub fn full_snapshot_view(&mut self) -> SnapshotViewV003 {
        let frozen = self.levels.freeze_writable();

        SnapshotViewV003::new(frozen.clone())
    }

    /// Replace all of the state machine data with the given one.
    /// The input is a multi-level data.
    pub fn replace(&mut self, level: LeveledMap) {
        let applied = self.levels.writable_ref().last_applied_ref();
        let new_applied = level.writable_ref().last_applied_ref();

        assert!(
            new_applied >= applied,
            "the state machine({:?}) can not be replaced with an older one({:?})",
            applied,
            new_applied
        );

        self.levels = level;

        // The installed data may not cleaned up all expired keys, if it is built with an older state machine.
        // So we need to reset the cursor then the next time applying a log it will cleanup all expired.
        self.expire_cursor = ExpireKey::new(0, 0);
    }

    /// Keep the top(writable) level, replace the base level and all levels below it.
    pub fn replace_base(&mut self, snapshot: &SnapshotViewV003) {
        assert!(
            Arc::ptr_eq(
                self.levels.frozen_ref().newest().unwrap(),
                snapshot.original_ref().newest().unwrap()
            ),
            "the base must not be changed"
        );

        self.levels.replace_frozen_levels(snapshot.compacted());
    }

    /// It returns 2 entries: the previous one and the new one after upsert.
    async fn upsert_kv_primary_index(
        &mut self,
        upsert_kv: &UpsertKV,
    ) -> (Marked<Vec<u8>>, Marked<Vec<u8>>) {
        let prev = MapApiRO::<String>::get(&self.levels, &upsert_kv.key)
            .await
            .clone();

        if upsert_kv.seq.match_seq(prev.seq()).is_err() {
            return (prev.clone(), prev);
        }

        let (prev, mut result) = match &upsert_kv.value {
            Operation::Update(v) => {
                self.levels
                    .set(
                        upsert_kv.key.clone(),
                        Some((v.clone(), upsert_kv.value_meta.clone())),
                    )
                    .await
            }
            Operation::Delete => self.levels.set(upsert_kv.key.clone(), None).await,
            Operation::AsIs => {
                self.levels
                    .update_meta(upsert_kv.key.clone(), upsert_kv.value_meta.clone())
                    .await
            }
        };

        let expire_ms = upsert_kv.get_expire_at_ms().unwrap_or(u64::MAX);
        if expire_ms < self.expire_cursor.time_ms {
            // The record has expired, delete it at once.
            //
            // Note that it must update first then delete,
            // in order to keep compatibility with the old state machine.
            // Old SM will just insert an expired record, and that causes the system seq increase by 1.
            let (_p, r) = self.levels.set(upsert_kv.key.clone(), None).await;
            result = r;
        };

        debug!(
            "applied upsert: {:?}; prev: {:?}; res: {:?}",
            upsert_kv, prev, result
        );

        (prev, result)
    }

    /// Update the secondary index for speeding up expiration operation.
    ///
    /// Remove the expiration index for the removed record, and add a new one for the new record.
    async fn update_expire_index(
        &mut self,
        key: impl ToString,
        removed: &Marked<Vec<u8>>,
        added: &Marked<Vec<u8>>,
    ) {
        // No change, no need to update expiration index
        if removed == added {
            return;
        }

        // Remove previous expiration index, add a new one.

        if let Some(exp_ms) = removed.expire_at_ms() {
            self.levels
                .set(ExpireKey::new(exp_ms, removed.internal_seq().seq()), None)
                .await;
        }

        if let Some(exp_ms) = added.expire_at_ms() {
            let k = ExpireKey::new(exp_ms, added.internal_seq().seq());
            let v = key.to_string();
            self.levels.set(k, Some((v, None))).await;
        }
    }
}
