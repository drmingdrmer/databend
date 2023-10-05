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

use common_meta_stoerr::MetaBytesError;
use common_meta_types::anyerror::AnyError;
use common_meta_types::LogId;
use common_meta_types::StoredMembership;

use crate::key_spaces::RaftStoreEntry;
use crate::sm_v003::leveled_store::level_data::LevelData;
use crate::sm_v003::leveled_store::sys_data_api::SysDataApiRO;
use crate::sm_v003::marked::Marked;
use crate::state_machine::ExpireKey;
use crate::state_machine::StateMachineMetaKey;

/// A container of temp data that are imported to a LevelData.
#[derive(Debug, Default)]
pub struct Importer {
    level_data: LevelData,

    kv: BTreeMap<String, Marked>,
    expire: BTreeMap<ExpireKey, Marked<String>>,

    greatest_seq: u64,
}

impl Importer {
    // TODO(1): consider returning IO error
    pub fn import(&mut self, entry: RaftStoreEntry) -> Result<(), MetaBytesError> {
        let d = &mut self.level_data;

        match entry {
            RaftStoreEntry::DataHeader { .. } => {
                // Not part of state machine
            }
            RaftStoreEntry::Logs { .. } => {
                // Not part of state machine
            }
            RaftStoreEntry::LogMeta { .. } => {
                // Not part of state machine
            }
            RaftStoreEntry::RaftStateKV { .. } => {
                // Not part of state machine
            }
            RaftStoreEntry::ClientLastResps { .. } => {
                unreachable!("client last resp is not supported")
            }
            RaftStoreEntry::Nodes { key, value } => {
                d.sys_data_mut().nodes_mut().insert(key, value);
            }
            RaftStoreEntry::StateMachineMeta { key, value } => {
                match key {
                    StateMachineMetaKey::LastApplied => {
                        let lid = TryInto::<LogId>::try_into(value).map_err(|e| {
                            MetaBytesError::new(&AnyError::error(format_args!(
                                "{} when import StateMachineMetaKey::LastApplied",
                                e
                            )))
                        })?;

                        *d.sys_data_mut().last_applied_mut() = Some(lid);
                    }
                    StateMachineMetaKey::Initialized => {
                        // This field is no longer used by in-memory state machine
                    }
                    StateMachineMetaKey::LastMembership => {
                        let membership =
                            TryInto::<StoredMembership>::try_into(value).map_err(|e| {
                                MetaBytesError::new(&AnyError::error(format_args!(
                                    "{} when import StateMachineMetaKey::LastMembership",
                                    e
                                )))
                            })?;
                        *d.sys_data_mut().last_membership_mut() = membership;
                    }
                }
            }
            RaftStoreEntry::Expire { key, mut value } => {
                // Old version ExpireValue has seq to be 0. replace it with 1.
                // `1` is a valid seq. `0` is used by tombstone.
                // 2023-06-06: by drdr.xp@gmail.com
                if value.seq == 0 {
                    value.seq = 1;
                }

                self.greatest_seq = std::cmp::max(self.greatest_seq, value.seq);
                self.expire.insert(key, Marked::from(value));
            }
            RaftStoreEntry::GenericKV { key, value } => {
                self.greatest_seq = std::cmp::max(self.greatest_seq, value.seq);
                self.kv.insert(key, Marked::from(value));
            }
            RaftStoreEntry::Sequences { key: _, value } => d.sys_data_mut().update_seq(value.0),
        }

        Ok(())
    }

    pub fn commit(mut self) -> LevelData {
        let d = &mut self.level_data;

        d.replace_kv(self.kv);
        d.replace_expire(self.expire);

        assert!(
            self.greatest_seq <= d.curr_seq(),
            "greatest_seq {} must be LE curr_seq {}, otherwise seq may be reused",
            self.greatest_seq,
            d.curr_seq()
        );

        self.level_data
    }
}
