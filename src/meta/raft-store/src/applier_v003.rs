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

use std::time::Duration;
use std::time::SystemTime;

use common_meta_types::protobuf as pb;
use common_meta_types::txn_condition;
use common_meta_types::txn_op;
use common_meta_types::txn_op_response;
use common_meta_types::AppliedState;
use common_meta_types::Change;
use common_meta_types::Cmd;
use common_meta_types::ConditionResult;
use common_meta_types::Entry;
use common_meta_types::EntryPayload;
use common_meta_types::KVMeta;
use common_meta_types::MatchSeq;
use common_meta_types::Node;
use common_meta_types::SeqV;
use common_meta_types::SeqValue;
use common_meta_types::StoredMembership;
use common_meta_types::TxnCondition;
use common_meta_types::TxnDeleteByPrefixRequest;
use common_meta_types::TxnDeleteByPrefixResponse;
use common_meta_types::TxnDeleteRequest;
use common_meta_types::TxnDeleteResponse;
use common_meta_types::TxnGetRequest;
use common_meta_types::TxnGetResponse;
use common_meta_types::TxnOp;
use common_meta_types::TxnOpResponse;
use common_meta_types::TxnPutRequest;
use common_meta_types::TxnPutResponse;
use common_meta_types::TxnReply;
use common_meta_types::TxnRequest;
use common_meta_types::UpsertKV;
use common_meta_types::With;
use futures_util::StreamExt;
use log::as_debug;
use log::as_display;
use log::debug;
use log::error;
use log::info;
use num::FromPrimitive;

use crate::sm_v003::SMV003;

/// A helper that applies raft log `Entry` to the state machine.
pub struct ApplierV003<'a> {
    sm: &'a mut SMV003,

    /// The changes has been made by the applying one log entry
    changes: Vec<Change<Vec<u8>, String>>,
}

impl<'a> ApplierV003<'a> {
    pub fn new(sm: &'a mut SMV003) -> Self {
        Self {
            sm,
            changes: Vec::new(),
        }
    }

    /// Apply an log entry to state machine.
    ///
    /// And publish kv change events to subscriber.
    #[minitrace::trace]
    pub async fn apply(&mut self, entry: &Entry) -> AppliedState {
        info!("apply: entry: {}", entry,);

        let log_id = &entry.log_id;
        let log_time_ms = Self::get_log_time(entry);

        self.clean_expired_kvs(log_time_ms).await;

        // TODO: it could persist the last_applied log id so that when starting up,
        //       it could re-apply the logs without waiting for the `committed` message from a leader.
        *self.sm.last_applied_mut() = Some(*log_id);

        let applied_state = match entry.payload {
            EntryPayload::Blank => {
                info!("apply: blank");

                AppliedState::None
            }
            EntryPayload::Normal(ref data) => {
                info!("apply: normal: {}", data);
                assert!(data.txid.is_none(), "txid is disabled");

                self.apply_cmd(&data.cmd).await
            }
            EntryPayload::Membership(ref mem) => {
                info!("apply: membership: {:?}", mem);

                *self.sm.last_membership_mut() = StoredMembership::new(Some(*log_id), mem.clone());
                AppliedState::None
            }
        };

        // Send queued change events to subscriber
        if let Some(subscriber) = &self.sm.subscriber {
            for event in self.changes.drain(..) {
                subscriber.kv_changed(event);
            }
        }

        applied_state
    }

    /// Apply a `Cmd` to state machine.
    ///
    /// Already applied log should be filtered out before passing into this function.
    /// This is the only entry to modify state machine.
    /// The `cmd` is always committed by raft before applying.
    #[minitrace::trace]
    pub async fn apply_cmd(&mut self, cmd: &Cmd) -> AppliedState {
        info!("apply_cmd: {}", cmd);

        let res = match cmd {
            Cmd::AddNode {
                node_id,
                node,
                overriding,
            } => self.apply_add_node(node_id, node, *overriding),

            Cmd::RemoveNode { ref node_id } => self.apply_remove_node(node_id),

            Cmd::UpsertKV(ref upsert_kv) => self.apply_upsert_kv(upsert_kv).await,

            Cmd::Transaction(txn) => self.apply_txn(txn).await,
        };

        info!("apply_result: cmd: {}; res: {}", cmd, res);

        res
    }

    /// Insert a node only when it does not exist or `overriding` is true.
    #[minitrace::trace]
    fn apply_add_node(&mut self, node_id: &u64, node: &Node, overriding: bool) -> AppliedState {
        let prev = self.sm.nodes_mut().get(node_id).cloned();

        if prev.is_none() {
            self.sm.nodes_mut().insert(*node_id, node.clone());
            info!("applied AddNode(non-overriding): {}={:?}", node_id, node);
            return (prev, Some(node.clone())).into();
        }

        if overriding {
            self.sm.nodes_mut().insert(*node_id, node.clone());
            info!("applied AddNode(overriding): {}={:?}", node_id, node);
            (prev, Some(node.clone())).into()
        } else {
            (prev.clone(), prev).into()
        }
    }

    #[minitrace::trace]
    fn apply_remove_node(&mut self, node_id: &u64) -> AppliedState {
        let prev = self.sm.nodes_mut().remove(node_id);
        info!("applied RemoveNode: {}={:?}", node_id, prev);

        (prev, None).into()
    }

    /// Execute an upsert-kv operation.
    ///
    /// KV has two indexes:
    /// - The primary index: `key -> (seq, meta(expire_time), value)`,
    /// - and a secondary expiration index: `(expire_time, seq) -> key`.
    ///
    /// Thus upsert a kv entry is done in two steps:
    /// update the primary index and optionally update the secondary index.
    #[minitrace::trace]
    async fn apply_upsert_kv(&mut self, upsert_kv: &UpsertKV) -> AppliedState {
        debug!(upsert_kv = as_debug!(upsert_kv); "apply_update_kv_cmd");

        let (prev, result) = self.upsert_kv(upsert_kv).await;

        Change::new(prev, result).into()
    }

    #[minitrace::trace]
    async fn upsert_kv(&mut self, upsert_kv: &UpsertKV) -> (Option<SeqV>, Option<SeqV>) {
        debug!(upsert_kv = as_debug!(upsert_kv); "upsert_kv");

        let (prev, result) = self.sm.upsert_kv(upsert_kv.clone()).await;

        debug!(
            "applied UpsertKV: {:?}; prev: {:?}; result: {:?}",
            upsert_kv, prev, result
        );

        // dbg!("push_change", &upsert_kv.key, prev.clone(), result.clone());
        self.push_change(&upsert_kv.key, prev.clone(), result.clone());

        (prev, result)
    }

    #[minitrace::trace]

    async fn apply_txn(&mut self, req: &TxnRequest) -> AppliedState {
        debug!(txn = as_display!(req); "apply txn cmd");

        let success = self.eval_txn_conditions(&req.condition).await;

        let ops = if success {
            &req.if_then
        } else {
            &req.else_then
        };

        let mut resp: TxnReply = TxnReply {
            success,
            error: "".to_string(),
            responses: vec![],
        };

        for op in ops {
            self.txn_execute_operation(op, &mut resp).await;
        }

        AppliedState::TxnReply(resp)
    }

    #[minitrace::trace]
    async fn eval_txn_conditions(&mut self, condition: &Vec<TxnCondition>) -> bool {
        for cond in condition {
            debug!(condition = as_display!(cond); "txn_execute_condition");

            if !self.eval_one_condition(cond).await {
                return false;
            }
        }

        true
    }

    #[minitrace::trace]
    async fn eval_one_condition(&self, cond: &TxnCondition) -> bool {
        debug!(cond = as_display!(cond); "txn_execute_one_condition");

        let key = &cond.key;
        let seqv = self.sm.get_kv(key).await;

        debug!(
            "txn_execute_one_condition: key: {} curr: seq:{} value:{:?}",
            key,
            seqv.seq(),
            seqv.value()
        );

        let target = if let Some(target) = &cond.target {
            target
        } else {
            return false;
        };

        match target {
            txn_condition::Target::Seq(right) => {
                Self::eval_seq_condition(seqv.seq(), cond.expected, right)
            }
            txn_condition::Target::Value(right) => {
                if let Some(v) = seqv.value() {
                    Self::eval_value_condition(v, cond.expected, right)
                } else {
                    false
                }
            }
        }
    }

    fn eval_seq_condition(left: u64, op: i32, right: &u64) -> bool {
        match FromPrimitive::from_i32(op) {
            Some(ConditionResult::Eq) => left == *right,
            Some(ConditionResult::Gt) => left > *right,
            Some(ConditionResult::Lt) => left < *right,
            Some(ConditionResult::Ne) => left != *right,
            Some(ConditionResult::Ge) => left >= *right,
            Some(ConditionResult::Le) => left <= *right,
            _ => false,
        }
    }

    fn eval_value_condition(left: &Vec<u8>, op: i32, right: &Vec<u8>) -> bool {
        match FromPrimitive::from_i32(op) {
            Some(ConditionResult::Eq) => left == right,
            Some(ConditionResult::Gt) => left > right,
            Some(ConditionResult::Lt) => left < right,
            Some(ConditionResult::Ne) => left != right,
            Some(ConditionResult::Ge) => left >= right,
            Some(ConditionResult::Le) => left <= right,
            _ => false,
        }
    }

    #[minitrace::trace]
    async fn txn_execute_operation(&mut self, op: &TxnOp, resp: &mut TxnReply) {
        debug!(op = as_display!(op); "txn execute TxnOp");
        match &op.request {
            Some(txn_op::Request::Get(get)) => {
                self.txn_execute_get(get, resp).await;
            }
            Some(txn_op::Request::Put(put)) => {
                self.txn_execute_put(put, resp).await;
            }
            Some(txn_op::Request::Delete(delete)) => {
                self.txn_execute_delete(delete, resp).await;
            }
            Some(txn_op::Request::DeleteByPrefix(delete_by_prefix)) => {
                self.txn_execute_delete_by_prefix(delete_by_prefix, resp)
                    .await;
            }
            None => {}
        }
    }

    async fn txn_execute_get(&self, get: &TxnGetRequest, resp: &mut TxnReply) {
        let sv = self.sm.get_kv(&get.key).await;
        let value = sv.map(Self::into_pb_seq_v);
        let get_resp = TxnGetResponse {
            key: get.key.clone(),
            value,
        };

        resp.responses.push(TxnOpResponse {
            response: Some(txn_op_response::Response::Get(get_resp)),
        });
    }

    async fn txn_execute_put(&mut self, put: &TxnPutRequest, resp: &mut TxnReply) {
        let upsert = UpsertKV::update(&put.key, &put.value).with(KVMeta {
            expire_at: put.expire_at,
        });

        let (prev, _result) = self.upsert_kv(&upsert).await;

        let put_resp = TxnPutResponse {
            key: put.key.clone(),
            prev_value: if put.prev_value {
                prev.map(Self::into_pb_seq_v)
            } else {
                None
            },
        };

        resp.responses.push(TxnOpResponse {
            response: Some(txn_op_response::Response::Put(put_resp)),
        });
    }

    async fn txn_execute_delete(&mut self, delete: &TxnDeleteRequest, resp: &mut TxnReply) {
        let upsert = UpsertKV::delete(&delete.key);

        // If `delete.match_seq` is `Some`, only delete the entry with the exact `seq`.
        let upsert = if let Some(seq) = delete.match_seq {
            upsert.with(MatchSeq::Exact(seq))
        } else {
            upsert
        };

        let (prev, result) = self.upsert_kv(&upsert).await;
        let is_deleted = prev.is_some() && result.is_none();

        let del_resp = TxnDeleteResponse {
            key: delete.key.clone(),
            success: is_deleted,
            prev_value: if delete.prev_value {
                prev.map(Self::into_pb_seq_v)
            } else {
                None
            },
        };

        resp.responses.push(TxnOpResponse {
            response: Some(txn_op_response::Response::Delete(del_resp)),
        });
    }

    async fn txn_execute_delete_by_prefix(
        &mut self,
        delete_by_prefix: &TxnDeleteByPrefixRequest,
        resp: &mut TxnReply,
    ) {
        let kvs = self.sm.prefix_list_kv(&delete_by_prefix.prefix).await;
        let count = kvs.len() as u32;

        for (key, _seq_v) in kvs {
            let (prev, res) = self.upsert_kv(&UpsertKV::delete(&key)).await;
            self.push_change(key, prev, res);
        }

        let del_resp = TxnDeleteByPrefixResponse {
            prefix: delete_by_prefix.prefix.clone(),
            count,
        };

        resp.responses.push(TxnOpResponse {
            response: Some(txn_op_response::Response::DeleteByPrefix(del_resp)),
        });
    }

    /// Before applying, list expired keys to clean.
    ///
    /// All expired keys will be removed before applying a log.
    /// This is different from the sled based implementation.
    #[minitrace::trace]
    async fn clean_expired_kvs(&mut self, log_time_ms: u64) {
        if log_time_ms == 0 {
            return;
        }

        info!("to clean expired kvs, log_time_ts: {}", log_time_ms);

        let mut to_clean = vec![];
        let mut strm = self.sm.list_expire_index().await;

        {
            let mut strm = std::pin::pin!(strm);
            while let Some((expire_key, expire_value)) = strm.next().await {
                // dbg!("check expired", &expire_key, &expire_value);
                // dbg!(expire_key.is_expired(log_time_ms));

                if !expire_key.is_expired(log_time_ms) {
                    break;
                }
                to_clean.push((expire_key.clone(), expire_value.clone()));
            }
        }

        for (expire_key, key) in to_clean {
            let curr = self.sm.get_kv(&key).await;
            if let Some(seq_v) = &curr {
                assert_eq!(expire_key.seq, seq_v.seq);
                info!("clean expired: {}, {}", key, expire_key);

                self.sm.upsert_kv(UpsertKV::delete(key.clone())).await;
                // dbg!("clean_expired", &key, &curr);
                self.push_change(key, curr, None);
            } else {
                unreachable!(
                    "trying to remove un-cleanable: {}, {}, kv-entry: {:?}",
                    key, expire_key, curr
                );
            }
        }

        self.sm.update_expire_cursor(log_time_ms);
    }

    /// Push a **change** that is applied to `key`.
    ///
    /// It does nothing if `prev == result`
    pub fn push_change(&mut self, key: impl ToString, prev: Option<SeqV>, result: Option<SeqV>) {
        if prev == result {
            return;
        }

        self.changes
            .push(Change::new(prev, result).with_id(key.to_string()))
    }

    /// Retrieve the proposing time from a raft-log.
    ///
    /// Only `Normal` log has a time embedded.
    #[minitrace::trace]
    fn get_log_time(entry: &Entry) -> u64 {
        match &entry.payload {
            EntryPayload::Normal(data) => match data.time_ms {
                None => {
                    error!(
                        "log has no time: {}, treat every entry with non-none `expire` as timed out",
                        entry
                    );
                    0
                }
                Some(ms) => {
                    let t = SystemTime::UNIX_EPOCH + Duration::from_millis(ms);
                    info!("apply: raft-log time: {:?}", t);
                    ms
                }
            },
            _ => 0,
        }
    }

    /// Convert SeqV defined in rust types to SeqV defined in protobuf.
    fn into_pb_seq_v(seq_v: SeqV) -> pb::SeqV {
        pb::SeqV {
            seq: seq_v.seq,
            data: seq_v.data,
        }
    }
}
