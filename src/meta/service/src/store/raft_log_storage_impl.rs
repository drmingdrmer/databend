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

use std::fmt::Debug;
use std::io::ErrorKind;
use std::ops::RangeBounds;

use databend_common_base::base::tokio::io;
use databend_common_meta_sled_store::openraft::storage::LogFlushed;
use databend_common_meta_sled_store::openraft::storage::RaftLogStorage;
use databend_common_meta_sled_store::openraft::ErrorSubject;
use databend_common_meta_sled_store::openraft::ErrorVerb;
use databend_common_meta_sled_store::openraft::LogState;
use databend_common_meta_sled_store::openraft::OptionalSend;
use databend_common_meta_sled_store::openraft::RaftLogReader;
use databend_common_meta_types::Entry;
use databend_common_meta_types::LogId;
use databend_common_meta_types::StorageError;
use databend_common_meta_types::TypeConfig;
use databend_common_meta_types::Vote;
use log::debug;
use log::error;
use log::info;

use crate::metrics::raft_metrics;
use crate::store::RaftStore;
use crate::store::ToStorageError;

impl RaftLogReader<TypeConfig> for RaftStore {
    #[minitrace::trace]
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, StorageError> {
        debug!(
            "try_get_log_entries: self.id={}, range: {:?}",
            self.id, range
        );

        match self
            .log
            .read()
            .await
            .range_values(range)
            .map_to_sto_err(ErrorSubject::Logs, ErrorVerb::Read)
        {
            Ok(entries) => Ok(entries),
            Err(err) => {
                raft_metrics::storage::incr_raft_storage_fail("try_get_log_entries", false);
                Err(err)
            }
        }
    }
}

impl RaftLogStorage<TypeConfig> for RaftStore {
    type LogReader = RaftStore;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError> {
        let last_purged_log_id = match self
            .log
            .read()
            .await
            .get_last_purged()
            .map_to_sto_err(ErrorSubject::Logs, ErrorVerb::Read)
        {
            Err(err) => {
                raft_metrics::storage::incr_raft_storage_fail("get_log_state", false);
                return Err(err);
            }
            Ok(r) => r,
        };

        let last = match self
            .log
            .read()
            .await
            .logs()
            .last()
            .map_to_sto_err(ErrorSubject::Logs, ErrorVerb::Read)
        {
            Err(err) => {
                raft_metrics::storage::incr_raft_storage_fail("get_log_state", false);
                return Err(err);
            }
            Ok(r) => r,
        };

        let last_log_id = match last {
            None => last_purged_log_id,
            Some(x) => Some(x.1.log_id),
        };

        debug!(
            "get_log_state: ({:?},{:?}]",
            last_purged_log_id, last_log_id
        );

        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_committed(&mut self, committed: Option<LogId>) -> Result<(), StorageError> {
        self.raft_state
            .write()
            .await
            .save_committed(committed)
            .await
            .map_to_sto_err(ErrorSubject::Store, ErrorVerb::Write)
    }

    async fn read_committed(&mut self) -> Result<Option<LogId>, StorageError> {
        self.raft_state
            .read()
            .await
            .read_committed()
            .map_to_sto_err(ErrorSubject::Store, ErrorVerb::Read)
    }

    #[minitrace::trace]
    async fn save_vote(&mut self, hs: &Vote) -> Result<(), StorageError> {
        info!(id = self.id, hs :? =(hs); "save_vote");

        match self
            .raft_state
            .write()
            .await
            .save_vote(hs)
            .await
            .map_to_sto_err(ErrorSubject::Vote, ErrorVerb::Write)
        {
            Err(err) => {
                raft_metrics::storage::incr_raft_storage_fail("save_vote", true);
                Err(err)
            }
            Ok(_) => Ok(()),
        }
    }

    #[minitrace::trace]
    async fn read_vote(&mut self) -> Result<Option<Vote>, StorageError> {
        match self
            .raft_state
            .read()
            .await
            .read_vote()
            .map_to_sto_err(ErrorSubject::Vote, ErrorVerb::Read)
        {
            Err(err) => {
                raft_metrics::storage::incr_raft_storage_fail("read_vote", false);
                Err(err)
            }
            Ok(vote) => Ok(vote),
        }
    }

    #[minitrace::trace]
    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError>
    where
        I: IntoIterator<Item = Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        // TODO: it is bad: allocates a new vec.
        let entries = entries
            .into_iter()
            .map(|x| {
                info!("append_to_log: {}", x.log_id);
                x
            })
            .collect::<Vec<_>>();

        let res = match self.log.write().await.append(entries).await {
            Err(err) => {
                raft_metrics::storage::incr_raft_storage_fail("append_to_log", true);
                Err(err)
            }
            Ok(_) => Ok(()),
        };

        callback.log_io_completed(res.map_err(|e| io::Error::new(ErrorKind::InvalidData, e)));

        Ok(())
    }

    #[minitrace::trace]
    async fn truncate(&mut self, log_id: LogId) -> Result<(), StorageError> {
        info!(id = self.id; "truncate: {}", log_id);

        match self
            .log
            .write()
            .await
            .range_remove(log_id.index..)
            .await
            .map_to_sto_err(ErrorSubject::Log(log_id), ErrorVerb::Delete)
        {
            Ok(_) => Ok(()),
            Err(err) => {
                raft_metrics::storage::incr_raft_storage_fail("delete_conflict_logs_since", true);
                Err(err)
            }
        }
    }

    #[minitrace::trace]
    async fn purge(&mut self, log_id: LogId) -> Result<(), StorageError> {
        info!(id = self.id, log_id :? =(&log_id); "purge upto: start");

        if let Err(err) = self
            .log
            .write()
            .await
            .set_last_purged(log_id)
            .await
            .map_to_sto_err(ErrorSubject::Logs, ErrorVerb::Write)
        {
            raft_metrics::storage::incr_raft_storage_fail("purge_logs_upto", true);
            return Err(err);
        };

        info!(id = self.id, log_id :? =(&log_id); "purge_logs_upto: Done: set_last_purged()");

        let log = self.log.write().await.clone();

        // Purge can be done in another task safely, because:
        //
        // - Next time when raft starts, it will read last_purged_log_id without examining the actual first log.
        //   And junk can be removed next time purge_logs_upto() is called.
        //
        // - Purging operates the start of the logs, and only committed logs are purged;
        //   while append and truncate operates on the end of the logs,
        //   it is safe to run purge && (append || truncate) concurrently.
        databend_common_base::runtime::spawn({
            let id = self.id;
            async move {
                info!(id = id, log_id :? =(&log_id); "purge_logs_upto: Start: asynchronous range_remove()");

                let res = log.range_remove(..=log_id.index).await;

                if let Err(err) = res {
                    error!(id = id, log_id :? =(&log_id); "purge_logs_upto: in asynchronous error: {}", err);
                    raft_metrics::storage::incr_raft_storage_fail("purge_logs_upto", true);
                }

                info!(id = id, log_id :? =(&log_id); "purge_logs_upto: Done: asynchronous range_remove()");
            }
        });

        Ok(())
    }
}
