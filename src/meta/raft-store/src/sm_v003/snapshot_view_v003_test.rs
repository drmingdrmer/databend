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

use std::sync::Arc;

use common_meta_types::Endpoint;
use common_meta_types::KVMeta;
use common_meta_types::Membership;
use common_meta_types::Node;
use common_meta_types::StoredMembership;
use common_meta_types::UpsertKV;
use futures_util::StreamExt;
use maplit::btreemap;
use openraft::testing::log_id;
use pretty_assertions::assert_eq;

use crate::key_spaces::RaftStoreEntry;
use crate::sm_v003::leveled_store::leveled_map::LeveledMap;
use crate::sm_v003::leveled_store::map_api::MapApi;
use crate::sm_v003::leveled_store::map_api::MapApiRO;
use crate::sm_v003::leveled_store::static_leveled_map::StaticLeveledMap;
use crate::sm_v003::leveled_store::sys_data_api::SysDataApiRO;
use crate::sm_v003::marked::Marked;
use crate::sm_v003::sm_v003::SMV003;
use crate::sm_v003::SnapshotViewV003;
use crate::state_machine::ExpireKey;

#[tokio::test]
async fn test_compact_copied_value_and_kv() -> anyhow::Result<()> {
    let mut l = build_3_levels().await;

    let frozen = l.freeze_writable().clone();

    let mut snapshot = SnapshotViewV003::new(frozen);

    snapshot.compact_mem_levels().await;

    let top_level = snapshot.compacted();

    let d = top_level.newest().unwrap().as_ref();

    assert_eq!(top_level.iter_levels().count(), 1);
    assert_eq!(
        d.last_membership_ref(),
        &StoredMembership::new(Some(log_id(3, 3, 3)), Membership::new(vec![], ()))
    );
    assert_eq!(d.last_applied_ref(), &Some(log_id(3, 3, 3)));
    assert_eq!(
        d.nodes_ref(),
        &btreemap! {3=>Node::new("3", Endpoint::new("3", 3))}
    );

    let got = MapApiRO::<String>::range::<String, _>(d, ..)
        .await
        .collect::<Vec<_>>()
        .await;
    assert_eq!(got, vec![
        //
        (s("a"), Marked::new_normal(1, b("a0"), None)),
        (s("d"), Marked::new_normal(7, b("d2"), None)),
        (s("e"), Marked::new_normal(6, b("e1"), None)),
    ]);

    let got = MapApiRO::<ExpireKey>::range(d, ..)
        .await
        .collect::<Vec<_>>()
        .await;
    assert_eq!(got, vec![]);

    Ok(())
}

#[tokio::test]
async fn test_compact_expire_index() -> anyhow::Result<()> {
    let mut sm = build_sm_with_expire().await;

    let mut snapshot = sm.full_snapshot_view();

    snapshot.compact_mem_levels().await;

    let compacted = snapshot.compacted();

    let d = compacted.newest().unwrap().as_ref();

    let got = MapApiRO::<String>::range::<String, _>(d, ..)
        .await
        .collect::<Vec<_>>()
        .await;
    assert_eq!(got, vec![
        //
        (
            s("a"),
            Marked::new_normal(
                4,
                b("a1"),
                Some(KVMeta {
                    expire_at: Some(15)
                })
            )
        ),
        (
            s("b"),
            Marked::new_normal(2, b("b0"), Some(KVMeta { expire_at: Some(5) }))
        ),
        (
            s("c"),
            Marked::new_normal(
                3,
                b("c0"),
                Some(KVMeta {
                    expire_at: Some(20)
                })
            )
        ),
    ]);

    let got = MapApiRO::<ExpireKey>::range(d, ..)
        .await
        .collect::<Vec<_>>()
        .await;
    assert_eq!(got, vec![
        //
        (
            ExpireKey::new(5_000, 2),
            Marked::new_normal(2, s("b"), None)
        ),
        (
            ExpireKey::new(15_000, 4),
            Marked::new_normal(4, s("a"), None)
        ),
        (
            ExpireKey::new(20_000, 3),
            Marked::new_normal(3, s("c"), None)
        ),
    ]);

    Ok(())
}

#[tokio::test]
async fn test_export_3_level() -> anyhow::Result<()> {
    let mut l = build_3_levels().await;

    let frozen = l.freeze_writable().clone();

    let snapshot = SnapshotViewV003::new(frozen);
    let got = snapshot
        .export()
        .await
        .map(|x| serde_json::to_string(&x).unwrap())
        .collect::<Vec<_>>()
        .await;

    // TODO(1): add tree name: ["state_machine/0",{"Sequences":{"key":"generic-kv","value":159}}]

    assert_eq!(got, vec![
        r#"{"DataHeader":{"key":"header","value":{"version":"V003","upgrading":null}}}"#,
        r#"{"StateMachineMeta":{"key":"LastApplied","value":{"LogId":{"leader_id":{"term":3,"node_id":3},"index":3}}}}"#,
        r#"{"StateMachineMeta":{"key":"LastMembership","value":{"Membership":{"log_id":{"leader_id":{"term":3,"node_id":3},"index":3},"membership":{"configs":[],"nodes":{}}}}}}"#,
        r#"{"Sequences":{"key":"generic-kv","value":7}}"#,
        r#"{"Nodes":{"key":3,"value":{"name":"3","endpoint":{"addr":"3","port":3},"grpc_api_advertise_address":null}}}"#,
        r#"{"GenericKV":{"key":"a","value":{"seq":1,"meta":null,"data":[97,48]}}}"#,
        r#"{"GenericKV":{"key":"d","value":{"seq":7,"meta":null,"data":[100,50]}}}"#,
        r#"{"GenericKV":{"key":"e","value":{"seq":6,"meta":null,"data":[101,49]}}}"#,
    ]);

    Ok(())
}

#[tokio::test]
async fn test_export_2_level_with_meta() -> anyhow::Result<()> {
    let mut sm = build_sm_with_expire().await;

    let snapshot = sm.full_snapshot_view();

    let got = snapshot
        .export()
        .await
        .map(|x| serde_json::to_string(&x).unwrap())
        .collect::<Vec<_>>()
        .await;

    assert_eq!(got, vec![
        r#"{"DataHeader":{"key":"header","value":{"version":"V003","upgrading":null}}}"#,
        r#"{"StateMachineMeta":{"key":"LastMembership","value":{"Membership":{"log_id":null,"membership":{"configs":[],"nodes":{}}}}}}"#,
        r#"{"Sequences":{"key":"generic-kv","value":4}}"#,
        r#"{"GenericKV":{"key":"a","value":{"seq":4,"meta":{"expire_at":15},"data":[97,49]}}}"#,
        r#"{"GenericKV":{"key":"b","value":{"seq":2,"meta":{"expire_at":5},"data":[98,48]}}}"#,
        r#"{"GenericKV":{"key":"c","value":{"seq":3,"meta":{"expire_at":20},"data":[99,48]}}}"#,
        r#"{"Expire":{"key":{"time_ms":5000,"seq":2},"value":{"seq":2,"key":"b"}}}"#,
        r#"{"Expire":{"key":{"time_ms":15000,"seq":4},"value":{"seq":4,"key":"a"}}}"#,
        r#"{"Expire":{"key":{"time_ms":20000,"seq":3},"value":{"seq":3,"key":"c"}}}"#,
    ]);

    Ok(())
}

#[tokio::test]
async fn test_import() -> anyhow::Result<()> {
    let exported = vec![
        r#"{"DataHeader":{"key":"header","value":{"version":"V003","upgrading":null}}}"#,
        r#"{"StateMachineMeta":{"key":"LastApplied","value":{"LogId":{"leader_id":{"term":3,"node_id":3},"index":3}}}}"#,
        r#"{"StateMachineMeta":{"key":"LastMembership","value":{"Membership":{"log_id":{"leader_id":{"term":3,"node_id":3},"index":3},"membership":{"configs":[],"nodes":{}}}}}}"#,
        r#"{"Sequences":{"key":"generic-kv","value":9}}"#,
        r#"{"Nodes":{"key":3,"value":{"name":"3","endpoint":{"addr":"3","port":3},"grpc_api_advertise_address":null}}}"#,
        r#"{"GenericKV":{"key":"a","value":{"seq":7,"meta":{"expire_at":15},"data":[97,49]}}}"#,
        r#"{"GenericKV":{"key":"b","value":{"seq":3,"meta":{"expire_at":5},"data":[98,48]}}}"#,
        r#"{"GenericKV":{"key":"c","value":{"seq":5,"meta":{"expire_at":20},"data":[99,48]}}}"#,
        r#"{"Expire":{"key":{"time_ms":5000,"seq":3},"value":{"seq":3,"key":"b"}}}"#,
        r#"{"Expire":{"key":{"time_ms":15000,"seq":7},"value":{"seq":7,"key":"a"}}}"#,
        r#"{"Expire":{"key":{"time_ms":20000,"seq":5},"value":{"seq":5,"key":"c"}}}"#,
    ];
    let data = exported
        .iter()
        .map(|x| serde_json::from_str::<RaftStoreEntry>(x).unwrap());

    let d = SMV003::import(data)?;

    let snapshot = SnapshotViewV003::new(StaticLeveledMap::new([Arc::new(d)]));

    let got = snapshot
        .export()
        .await
        .map(|x| serde_json::to_string(&x).unwrap())
        .collect::<Vec<_>>()
        .await;

    assert_eq!(got, exported);

    Ok(())
}

/// Create multi levels store:
///
/// l2 |         c(D) d
/// l1 |    b(D) c        e
/// l0 | a  b    c    d
async fn build_3_levels() -> LeveledMap {
    let mut l = LeveledMap::default();
    let sd = l.writable_mut().sys_data_mut();

    *sd.last_membership_mut() =
        StoredMembership::new(Some(log_id(1, 1, 1)), Membership::new(vec![], ()));
    *sd.last_applied_mut() = Some(log_id(1, 1, 1));
    *sd.nodes_mut() = btreemap! {1=>Node::new("1", Endpoint::new("1", 1))};

    // internal_seq: 0
    MapApi::<String>::set(&mut l, s("a"), Some((b("a0"), None))).await;
    MapApi::<String>::set(&mut l, s("b"), Some((b("b0"), None))).await;
    MapApi::<String>::set(&mut l, s("c"), Some((b("c0"), None))).await;
    MapApi::<String>::set(&mut l, s("d"), Some((b("d0"), None))).await;

    l.freeze_writable();
    let sd = l.writable_mut().sys_data_mut();

    *sd.last_membership_mut() =
        StoredMembership::new(Some(log_id(2, 2, 2)), Membership::new(vec![], ()));
    *sd.last_applied_mut() = Some(log_id(2, 2, 2));
    *sd.nodes_mut() = btreemap! {2=>Node::new("2", Endpoint::new("2", 2))};

    // internal_seq: 4
    MapApi::<String>::set(&mut l, s("b"), None).await;
    MapApi::<String>::set(&mut l, s("c"), Some((b("c1"), None))).await;
    MapApi::<String>::set(&mut l, s("e"), Some((b("e1"), None))).await;

    l.freeze_writable();
    let sd = l.writable_mut().sys_data_mut();

    *sd.last_membership_mut() =
        StoredMembership::new(Some(log_id(3, 3, 3)), Membership::new(vec![], ()));
    *sd.last_applied_mut() = Some(log_id(3, 3, 3));
    *sd.nodes_mut() = btreemap! {3=>Node::new("3", Endpoint::new("3", 3))};

    // internal_seq: 6
    MapApi::<String>::set(&mut l, s("c"), None).await;
    MapApi::<String>::set(&mut l, s("d"), Some((b("d2"), None))).await;

    l
}

/// The subscript is internal_seq:
///
///    | kv             | expire
///    | ---            | ---
/// l1 | a₄       c₃    |               10,1₄ -> ø    15,4₄ -> a  20,3₃ -> c          
/// ------------------------------------------------------------
/// l0 | a₁  b₂         |  5,2₂ -> b    10,1₁ -> a
async fn build_sm_with_expire() -> SMV003 {
    let mut sm = SMV003::default();

    sm.upsert_kv(UpsertKV::update("a", b"a0").with_expire_sec(10))
        .await;
    sm.upsert_kv(UpsertKV::update("b", b"b0").with_expire_sec(5))
        .await;

    sm.levels.freeze_writable();

    sm.upsert_kv(UpsertKV::update("c", b"c0").with_expire_sec(20))
        .await;
    sm.upsert_kv(UpsertKV::update("a", b"a1").with_expire_sec(15))
        .await;

    sm
}

fn s(x: impl ToString) -> String {
    x.to_string()
}

fn b(x: impl ToString) -> Vec<u8> {
    x.to_string().as_bytes().to_vec()
}
