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

//! Defines transaction related types

use crate::protobuf::txn_request_item::TxnRequestPayload;
use crate::LogId;

/// TransactionId
pub type TxnId = u64;

/// An command to submit a transaction via stream of multiple commands.
enum TxnCmd {
    /// Begin a transaction.
    Begin,

    /// Append transaction conditions or operations
    Payload(Vec<TxnRequestPayload>),

    /// Finish a transaction by committing it or abort it.
    End {
        /// commit or abort
        commit: bool,
    },
}

/// Transaction meta data to be stored in transaction buffer in state machine.
pub struct TxnMeta {
    /// The id of the raft log by which the transaction is created.
    log_id: LogId,

    /// The time in millisecond of the raft log.
    log_time_ms: u64,

    /// The time in millisecond of the last update, e.g. the time of the raft log that append a TxnPayload
    last_update_time_ms: u64,
}

/// The value of a buffer in state machine to store uncommitted transaction.
pub struct TxnBufferValue {
    /// The id of the raft log that append the transaction operation.
    log_id: LogId,

    /// The time in millisecond of the raft log.
    log_time_ms: u64,

    /// A condition or an `then/else` operation.
    payload: TxnRequestPayload,
}
