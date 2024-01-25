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

#![allow(clippy::uninlined_format_args)]

mod grpc;
use grpc::export_meta;

pub(crate) mod reading;
mod snapshot;

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io;

use clap::Parser;
use databend_common_base::base::tokio;
use databend_common_meta_api::deserialize_struct;
use databend_common_meta_api::deserialize_u64;
use databend_common_meta_app::schema::CountTablesKey;
use databend_common_meta_app::schema::DBIdTableName;
use databend_common_meta_app::schema::DatabaseId;
use databend_common_meta_app::schema::DatabaseIdToName;
use databend_common_meta_app::schema::DatabaseNameIdent;
use databend_common_meta_app::schema::DbIdListKey;
use databend_common_meta_app::schema::IndexId;
use databend_common_meta_app::schema::IndexIdToName;
use databend_common_meta_app::schema::IndexNameIdent;
use databend_common_meta_app::schema::LeastVisibleTimeKey;
use databend_common_meta_app::schema::TableCopiedFileNameIdent;
use databend_common_meta_app::schema::TableId;
use databend_common_meta_app::schema::TableIdListKey;
use databend_common_meta_app::schema::TableIdToName;
use databend_common_meta_app::schema::TableLockKey;
use databend_common_meta_app::schema::VirtualColumnNameIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_app::AppKey;
use databend_common_meta_client::MetaGrpcClient;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::KVApi;
use databend_common_meta_kvapi::kvapi::Key;
use databend_common_meta_raft_store::config::RaftConfig;
use databend_common_meta_raft_store::key_spaces::RaftStoreEntry;
use databend_common_meta_types::SeqV;
use databend_common_tracing::init_logging;
use databend_common_tracing::Config as LogConfig;
use databend_common_tracing::FileConfig;
use databend_meta::version::METASRV_COMMIT_VERSION;
use log::info;
use serde::Deserialize;
use serde::Serialize;

// TODO(xuanwo)
//
// We should make metactl config keeps backward compatibility too.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Parser)]
#[clap(about, version = &**METASRV_COMMIT_VERSION, author)]
pub struct Config {
    /// Run a command.
    ///
    /// `--cmd bench-client-conn-num` to benchmark connection to meta-service.
    ///
    /// `--cmd retain{"tenant":"hero"}` to retain only the meta data that belongs to tenant `hero`.
    #[clap(long, default_value = "")]
    pub cmd: String,

    #[clap(long, default_value = "INFO")]
    pub log_level: String,

    #[clap(long)]
    pub status: bool,

    #[clap(long)]
    pub import: bool,

    #[clap(long)]
    pub export: bool,

    /// The N.O. json strings in a export stream item.
    ///
    /// Set this to a smaller value if you get gRPC message body too large error.
    /// This requires meta-service >= 1.2.315; For older version, this argument is ignored.
    ///
    /// By default it is 32.
    #[clap(long)]
    pub export_chunk_size: Option<u64>,

    #[clap(
        long,
        env = "METASRV_GRPC_API_ADDRESS",
        default_value = "127.0.0.1:9191"
    )]
    pub grpc_api_address: String,

    /// The dir to store persisted meta state, including raft logs, state machine etc.
    #[clap(long)]
    #[serde(alias = "kvsrv_raft_dir")]
    pub raft_dir: Option<String>,

    /// When export raft data, this is the name of the save db file.
    /// If `db` is empty, output the exported data as json to stdout instead.
    /// When import raft data, this is the name of the restored db file.
    /// If `db` is empty, the restored data is from stdin instead.
    #[clap(long, default_value = "")]
    pub db: String,

    /// initial_cluster format: node_id=endpoint,grpc_api_addr
    #[clap(long)]
    pub initial_cluster: Vec<String>,

    /// The node id. Used in these cases:
    /// 1. when this server is not initialized, e.g. --boot or --single for the first time.
    /// 2. --initial_cluster with new cluster node id.
    ///  Otherwise this argument is ignored.
    #[clap(long, default_value = "0")]
    #[serde(alias = "kvsrv_id")]
    pub id: u64,
}

impl From<Config> for RaftConfig {
    #[allow(clippy::field_reassign_with_default)]
    fn from(value: Config) -> Self {
        let mut c = Self::default();

        c.raft_dir = value.raft_dir.unwrap_or_default();
        c.id = value.id;
        c
    }
}

/// Predicate to retain.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
struct Predicate {
    tenant: String,
}

/// Usage:
/// - To dump a sled db: `$0 --raft-dir ./_your_meta_dir/`:
///   ```
///   ["header",{"DataHeader":{"key":"header","value":{"version":"V002","upgrading":null}}}]
///   ["raft_state",{"RaftStateKV":{"key":"Id","value":{"NodeId":1}}}]
///   ["raft_state",{"RaftStateKV":{"key":"HardState","value":{"HardState":{"leader_id":{"term":1,"node_id":1},"committed":false}}}}]
///   ["raft_log",{"Logs":{"key":0,"value":{"log_id":{"leader_id":{"term":0,"node_id":0},"index":0},"payload":{"Membership":{"configs":[[1]],"nodes":{"1":{}}}}}}}]
///   ["raft_log",{"Logs":{"key":1,"value":{"log_id":{"leader_id":{"term":1,"node_id":0},"index":1},"payload":"Blank"}}}]
///   ```
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::parse();

    let log_config = LogConfig {
        file: FileConfig {
            on: true,
            level: config.log_level.clone(),
            dir: ".databend/logs".to_string(),
            format: "text".to_string(),
            limit: 48,
            prefix_filter: "databend_".to_string(),
        },
        ..Default::default()
    };

    let _guards = init_logging("metactl", &log_config, BTreeMap::new());

    if config.status {
        return show_status(&config).await;
    }

    eprintln!();
    eprintln!("╔╦╗╔═╗╔╦╗╔═╗   ╔═╗╔╦╗╦  ");
    eprintln!("║║║║╣  ║ ╠═╣───║   ║ ║  ");
    eprintln!("╩ ╩╚═╝ ╩ ╩ ╩   ╚═╝ ╩ ╩═╝ Databend");
    eprintln!();

    // eprintln!("███╗   ███╗███████╗████████╗ █████╗        ██████╗████████╗██╗     ");
    // eprintln!("████╗ ████║██╔════╝╚══██╔══╝██╔══██╗      ██╔════╝╚══██╔══╝██║     ");
    // eprintln!("██╔████╔██║█████╗     ██║   ███████║█████╗██║        ██║   ██║     ");
    // eprintln!("██║╚██╔╝██║██╔══╝     ██║   ██╔══██║╚════╝██║        ██║   ██║     ");
    // eprintln!("██║ ╚═╝ ██║███████╗   ██║   ██║  ██║      ╚██████╗   ██║   ███████╗");
    // eprintln!("╚═╝     ╚═╝╚══════╝   ╚═╝   ╚═╝  ╚═╝       ╚═════╝   ╚═╝   ╚══════╝");

    // ██████╗  █████╗ ████████╗ █████╗ ██████╗ ███████╗███╗   ██╗██████╗
    // ██╔══██╗██╔══██╗╚══██╔══╝██╔══██╗██╔══██╗██╔════╝████╗  ██║██╔══██╗
    // ██║  ██║███████║   ██║   ███████║██████╔╝█████╗  ██╔██╗ ██║██║  ██║
    // ██║  ██║██╔══██║   ██║   ██╔══██║██╔══██╗██╔══╝  ██║╚██╗██║██║  ██║
    // ██████╔╝██║  ██║   ██║   ██║  ██║██████╔╝███████╗██║ ╚████║██████╔╝
    // ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝╚═════╝ ╚══════╝╚═╝  ╚═══╝╚═════╝
    // ╔╦╗╔═╗╔╦╗╔═╗   ╔═╗╔╦╗╦
    // ║║║║╣  ║ ╠═╣───║   ║ ║
    // ╩ ╩╚═╝ ╩ ╩ ╩   ╚═╝ ╩ ╩═╝

    eprintln!("Version: {}", METASRV_COMMIT_VERSION.as_str());
    eprintln!();
    eprintln!("Config: {}", pretty(&config)?);

    if !config.cmd.is_empty() {
        let cmd_and_param = config.cmd.splitn(2, ':').collect::<Vec<_>>();
        let cmd = cmd_and_param[0].to_string();
        // param is json format
        let param = cmd_and_param.get(1).unwrap_or(&"{}").to_string();

        return match cmd.as_str() {
            "retain" => {
                // retain meta data that matches the predicate
                retain(&param).await?;
                Ok(())
            }
            "bench-client-conn-num" => {
                bench_client_num_conn(&config).await?;
                Ok(())
            }

            _ => {
                eprintln!("valid commands are");
                eprintln!("  --cmd bench-client-conn-num");
                eprintln!("    Keep create new connections to metasrv.");
                eprintln!("    Requires --grpc-api-address.");

                Err(anyhow::anyhow!("unknown cmd: {}", config.cmd))
            }
        };
    }

    if config.export {
        eprintln!();
        eprintln!("Export:");
        return snapshot::export_data(&config).await;
    }

    if config.import {
        eprintln!();
        eprintln!("Import:");
        return snapshot::import_data(&config).await;
    }

    Err(anyhow::anyhow!("Nothing to do"))
}

fn pretty<T>(v: &T) -> Result<String, serde_json::Error>
where T: Serialize {
    serde_json::to_string_pretty(v)
}

async fn bench_client_num_conn(conf: &Config) -> anyhow::Result<()> {
    let addr = &conf.grpc_api_address;

    println!(
        "loop: connect to metasrv {}, get_kv('foo'), do not drop the connection",
        addr
    );

    let mut clients = vec![];
    let mut i = 0;

    loop {
        i += 1;
        let client =
            MetaGrpcClient::try_create(vec![addr.to_string()], "root", "xxx", None, None, None)?;

        let res = client.get_kv("foo").await;
        println!("{}-th: get_kv(foo): {:?}", i, res);

        clients.push(client);
    }
}

/// Read exported meta data from stdin,
/// and retain only the meta data that matches the predicate, output to stdout.
///
/// `retain` will:
/// - Discard all raft log because there must not be a hole. Keep only state machine records
/// - Discard the `purged` log marker and let Openraft to restore it to the latest applied log.
/// - Remove kv records from state machine by `predicate`
async fn retain(param: &str) -> anyhow::Result<()> {
    let predicate = serde_json::from_str::<Predicate>(param)?;

    let lines = io::stdin().lines();

    #[allow(clippy::useless_conversion)]
    let mut it = lines.into_iter().peekable();
    let version = reading::validate_version(&mut it)?;

    // Dump all data into memory

    let mut meta = Vec::new();
    let mut kvs = BTreeMap::new();
    let mut expire = BTreeMap::new();

    let mut retained = BTreeMap::new();
    retained.insert(
        Tenant {
            tenant: predicate.tenant.clone(),
        }
        .to_string_key(),
        ("".to_string(), SeqV::new(0, vec![])),
    );

    // feed state machine entries to map, feed non-state-machine entries to meta
    for line in it {
        let line = line?;
        let (tree_name, kv_entry): (String, RaftStoreEntry) = serde_json::from_str(&line)?;
        match &kv_entry {
            RaftStoreEntry::DataHeader { .. } => {
                meta.push((tree_name, kv_entry));
            }
            RaftStoreEntry::Logs { .. } => {
                // Logs are discarded
            }
            RaftStoreEntry::Nodes { .. } => {
                meta.push((tree_name, kv_entry));
            }
            RaftStoreEntry::StateMachineMeta { .. } => {
                meta.push((tree_name, kv_entry));
            }
            RaftStoreEntry::RaftStateKV { .. } => {
                meta.push((tree_name, kv_entry));
            }
            RaftStoreEntry::Expire { key, value } => {
                expire.insert(key.clone(), (tree_name, value.clone()));
            }
            RaftStoreEntry::GenericKV { key, value } => {
                kvs.insert(key.clone(), (tree_name, value.clone()));
            }
            RaftStoreEntry::Sequences { .. } => {
                meta.push((tree_name, kv_entry));
            }
            RaftStoreEntry::ClientLastResps { .. } => {
                unreachable!("ClientLastResps is not supported");
            }
            RaftStoreEntry::LogMeta { key, value } => {
                // `purged` is discarded. Openraft will restore it to the latest applied log.
                info!("LogMeta: discarded: {} {:?}", key, value);
            }
        }
    }

    // Find db ids
    let mut db_ids = BTreeSet::new();
    let mut table_ids = BTreeSet::new();
    let mut index_ids = BTreeSet::new();

    for (key, (tree_name, seqv)) in kvs.iter() {
        //
        let app_key = AppKey::from_str_key(key)?;
    }

    // Round 1: search from tenant
    for (key, (tree_name, seqv)) in kvs.iter() {
        if key.starts_with(&DatabaseNameIdent::root_prefix()) {
            let k = DatabaseNameIdent::from_str_key(key).unwrap();
            let v: <DatabaseNameIdent as kvapi::Key>::ValueType =
                deserialize_u64(&seqv.data).unwrap().0.into();
            if k.tenant == predicate.tenant {
                // tenant/db_name -> db_id
                retained.insert(key.clone(), (tree_name.clone(), seqv.clone()));

                // db_id -> db_meta
                retained.insert(
                    v.to_string_key(),
                    kvs.get(&v.to_string_key()).unwrap().clone(),
                );

                db_ids.insert(v.db_id);
            }
        }

        // DbIdListKey -> DbIdList
        if key.starts_with(&DbIdListKey::root_prefix()) {
            let k = DbIdListKey::from_str_key(key).unwrap();
            let v: <DbIdListKey as kvapi::Key>::ValueType = deserialize_struct(&seqv.data).unwrap();
            if k.tenant == predicate.tenant {
                retained.insert(key.clone(), (tree_name.clone(), seqv.clone()));
                for db_id in v.id_list {
                    db_ids.insert(db_id);
                }
            }
        }

        // CountTablesKey -> u64
        if key.starts_with(&CountTablesKey::root_prefix()) {
            let k = CountTablesKey::from_str_key(key).unwrap();
            let v: <CountTablesKey as kvapi::Key>::ValueType =
                deserialize_u64(&seqv.data).unwrap().0.into();
            if k.tenant == predicate.tenant {
                retained.insert(key.clone(), (tree_name.clone(), seqv.clone()));
            }
        }

        // IndexNameIdent -> IndexId
        if key.starts_with(&IndexNameIdent::root_prefix()) {
            let k = IndexNameIdent::from_str_key(key).unwrap();
            let v: <IndexNameIdent as kvapi::Key>::ValueType =
                deserialize_u64(&seqv.data).unwrap().0.into();
            if k.tenant == predicate.tenant {
                retained.insert(key.clone(), (tree_name.clone(), seqv.clone()));
                index_ids.insert(v.index_id);
            }
        }
    }

    // Round 2: search from db_ids
    for (key, (tree_name, seqv)) in kvs.iter() {
        // TODO: DatabaseMeta depends on Share
        // Find db_id -> DatabaseMeta
        if key.starts_with(&DatabaseId::root_prefix()) {
            let k = DatabaseId::from_str_key(key).unwrap();
            let v: <DatabaseId as kvapi::Key>::ValueType = deserialize_struct(&seqv.data).unwrap();
            if db_ids.contains(&k.db_id) {
                retained.insert(key.clone(), (tree_name.clone(), seqv.clone()));
            }
        }

        // Find db_id -> DatabaseNameIdent
        if key.starts_with(&DatabaseIdToName::root_prefix()) {
            let k = DatabaseIdToName::from_str_key(key).unwrap();
            let v: <DatabaseIdToName as kvapi::Key>::ValueType =
                deserialize_struct(&seqv.data).unwrap();
            if db_ids.contains(&k.db_id) {
                retained.insert(key.clone(), (tree_name.clone(), seqv.clone()));
            }
        }

        // Find db_id/table_name -> TableId
        // Save table ids
        if key.starts_with(&DBIdTableName::root_prefix()) {
            let k = DBIdTableName::from_str_key(key).unwrap();
            let v: <DBIdTableName as kvapi::Key>::ValueType =
                deserialize_u64(&seqv.data).unwrap().0.into();
            if db_ids.contains(&k.db_id) {
                retained.insert(key.clone(), (tree_name.clone(), seqv.clone()));

                table_ids.insert(v.table_id);
            }
        }

        // TableIdListKey -> TableIdList
        if key.starts_with(&TableIdListKey::root_prefix()) {
            let k = TableIdListKey::from_str_key(key).unwrap();
            let v: <TableIdListKey as kvapi::Key>::ValueType =
                deserialize_struct(&seqv.data).unwrap();
            if db_ids.contains(&k.db_id) {
                retained.insert(key.clone(), (tree_name.clone(), seqv.clone()));
            }
        }
    }

    // Round 3: search from table_ids
    for (key, (tree_name, seqv)) in kvs.iter() {
        // Find table_id -> TableMeta
        if key.starts_with(&TableId::root_prefix()) {
            let k = TableId::from_str_key(key).unwrap();
            let v: <TableId as kvapi::Key>::ValueType =
                deserialize_u64(&seqv.data).unwrap().0.into();
            if table_ids.contains(&k.table_id) {
                retained.insert(key.clone(), (tree_name.clone(), seqv.clone()));
            }
        }

        // TableIdToName -> DbIdTableName
        if key.starts_with(&TableIdToName::root_prefix()) {
            let k = TableIdToName::from_str_key(key).unwrap();
            let v: <TableIdToName as kvapi::Key>::ValueType =
                deserialize_struct(&seqv.data).unwrap();
            if table_ids.contains(&k.table_id) {
                retained.insert(key.clone(), (tree_name.clone(), seqv.clone()));
            }
        }

        // TableCopiedFileNameIdent -> TableCopiedFileInfo
        if key.starts_with(&TableCopiedFileNameIdent::root_prefix()) {
            let k = TableCopiedFileNameIdent::from_str_key(key).unwrap();
            let v: <TableCopiedFileNameIdent as kvapi::Key>::ValueType =
                deserialize_struct(&seqv.data).unwrap();
            if table_ids.contains(&k.table_id) {
                retained.insert(key.clone(), (tree_name.clone(), seqv.clone()));
            }
        }

        // LeastVisibleTimeKey -> LeastVisibleTime
        if key.starts_with(&LeastVisibleTimeKey::root_prefix()) {
            let k = LeastVisibleTimeKey::from_str_key(key).unwrap();
            let v: <LeastVisibleTimeKey as kvapi::Key>::ValueType =
                deserialize_struct(&seqv.data).unwrap();
            if table_ids.contains(&k.table_id) {
                retained.insert(key.clone(), (tree_name.clone(), seqv.clone()));
            }
        }

        // TableLockKey -> LockMeta
        if key.starts_with(&TableLockKey::root_prefix()) {
            let k = TableLockKey::from_str_key(key).unwrap();
            let v: <TableLockKey as kvapi::Key>::ValueType =
                deserialize_struct(&seqv.data).unwrap();
            if table_ids.contains(&k.table_id) {
                retained.insert(key.clone(), (tree_name.clone(), seqv.clone()));
            }
        }

        // VirtualColumnNameIdent -> VirtualColumnMeta
        if key.starts_with(&VirtualColumnNameIdent::root_prefix()) {
            let k = VirtualColumnNameIdent::from_str_key(key).unwrap();
            let v: <VirtualColumnNameIdent as kvapi::Key>::ValueType =
                deserialize_struct(&seqv.data).unwrap();
            if k.tenant == predicate.tenant && table_ids.contains(&k.table_id) {
                retained.insert(key.clone(), (tree_name.clone(), seqv.clone()));
            }
        }

        // IndexId -> IndexMeta
        if key.starts_with(&IndexId::root_prefix()) {
            let k = IndexId::from_str_key(key).unwrap();
            let v: <IndexId as kvapi::Key>::ValueType = deserialize_struct(&seqv.data).unwrap();
            if index_ids.contains(&k.index_id) {
                retained.insert(key.clone(), (tree_name.clone(), seqv.clone()));
            }
        }

        // IndexIdToName -> IndexNameIdent
        if key.starts_with(&IndexIdToName::root_prefix()) {
            let k = IndexIdToName::from_str_key(key).unwrap();
            let v: <IndexIdToName as kvapi::Key>::ValueType =
                deserialize_struct(&seqv.data).unwrap();
            if index_ids.contains(&k.index_id) {
                retained.insert(key.clone(), (tree_name.clone(), seqv.clone()));
            }
        }
    }

    // Output:

    for line in meta {
        println!("{}", serde_json::to_string(&line)?);
    }
    for (key, (tree_name, seqv)) in retained.into_iter() {
        let line = (tree_name, RaftStoreEntry::GenericKV { key, value: seqv });
        println!("{}", serde_json::to_string(&line)?);
    }

    // TODO:
    //
    // background_job.rs
    //     impl kvapi::Key for BackgroundJobIdent {
    //     impl kvapi::Key for BackgroundJobId {
    // background_task.rs
    //     impl kvapi::Key for BackgroundTaskIdent {
    // data_mask/mod.rs
    //     impl kvapi::Key for DatamaskNameIdent {
    //     impl kvapi::Key for DatamaskId {
    //     impl kvapi::Key for MaskpolicyTableIdListKey {
    // schema/catalog.rs
    //     impl kvapi::Key for CatalogNameIdent {
    //     impl kvapi::Key for CatalogId {
    //     impl kvapi::Key for CatalogIdToName {
    // database.rs
    // [x] impl kvapi::Key for DatabaseNameIdent {
    // [x] impl kvapi::Key for DatabaseId {
    // [x] impl kvapi::Key for DatabaseIdToName {
    // [x] impl kvapi::Key for DbIdListKey {
    // index.rs
    // [x] impl kvapi::Key for IndexNameIdent {
    // [x] impl kvapi::Key for IndexId {
    // [x] impl kvapi::Key for IndexIdToName {
    // lock.rs
    // [x] impl kvapi::Key for TableLockKey {
    // table.rs
    // [x] impl kvapi::Key for DBIdTableName {
    // [x] impl kvapi::Key for TableIdToName {
    // [x] impl kvapi::Key for TableId {
    // [x] impl kvapi::Key for TableIdListKey {
    // [x] impl kvapi::Key for CountTablesKey {
    // [x] impl kvapi::Key for TableCopiedFileNameIdent {
    // [x] impl kvapi::Key for LeastVisibleTimeKey {
    // virtual_column.rs
    // [x] impl kvapi::Key for VirtualColumnNameIdent {
    // share/share.rs
    //     impl kvapi::Key for ShareGrantObject {
    //     impl kvapi::Key for ShareNameIdent {
    //     impl kvapi::Key for ShareId {
    //     impl kvapi::Key for ShareAccountNameIdent {
    //     impl kvapi::Key for ShareIdToName {
    //     impl kvapi::Key for ShareEndpointIdent {
    //     impl kvapi::Key for ShareEndpointId {
    //     impl kvapi::Key for ShareEndpointIdToName {
    // kvapi/src/kvapi/key.rs
    //     impl kvapi::Key for String {

    Ok(())
}

fn get_dependency_keys(key: &AppKey, value: &[u8]) -> Vec<String> {
    match key {
        AppKey::Tenant(_) => {
            vec![]
        }
        AppKey::BackgroundJobIdent(_) => {}
        AppKey::BackgroundJobId(_) => {}
        AppKey::BackgroundTaskIdent(_) => {}
        AppKey::DatamaskNameIdent(_) => {}
        AppKey::DatamaskId(_) => {}
        AppKey::MaskpolicyTableIdListKey(_) => {}
        AppKey::CatalogNameIdent(_) => {}
        AppKey::CatalogId(_) => {}
        AppKey::CatalogIdToName(_) => {}
        AppKey::DatabaseNameIdent(_) => {}
        AppKey::DatabaseId(_) => {}
        AppKey::DatabaseIdToName(_) => {}
        AppKey::DbIdListKey(_) => {}
        AppKey::IndexNameIdent(_) => {}
        AppKey::IndexId(_) => {}
        AppKey::IndexIdToName(_) => {}
        AppKey::TableLockKey(_) => {}
        AppKey::DBIdTableName(_) => {}
        AppKey::TableIdToName(_) => {}
        AppKey::TableId(_) => {}
        AppKey::TableIdListKey(_) => {}
        AppKey::CountTablesKey(_) => {}
        AppKey::TableCopiedFileNameIdent(_) => {}
        AppKey::LeastVisibleTimeKey(_) => {}
        AppKey::VirtualColumnNameIdent(_) => {}
        AppKey::ShareGrantObject(_) => {}
        AppKey::ShareNameIdent(_) => {}
        AppKey::ShareId(_) => {}
        AppKey::ShareAccountNameIdent(_) => {}
        AppKey::ShareIdToName(_) => {}
        AppKey::ShareEndpointIdent(_) => {}
        AppKey::ShareEndpointId(_) => {}
        AppKey::ShareEndpointIdToName(_) => {}
    }
}

async fn show_status(conf: &Config) -> anyhow::Result<()> {
    let addr = &conf.grpc_api_address;

    let client =
        MetaGrpcClient::try_create(vec![addr.to_string()], "root", "xxx", None, None, None)?;

    let res = client.get_cluster_status().await?;
    println!("BinaryVersion: {}", res.binary_version);
    println!("DataVersion: {}", res.data_version);
    println!("DBSize: {}", res.db_size);
    println!("Node: id={} raft={}", res.id, res.endpoint);
    println!("State: {}", res.state);
    if let Some(leader) = res.leader {
        println!("Leader: {}", leader);
    }
    println!("CurrentTerm: {}", res.current_term);
    println!("LastSeq: {:?}", res.last_seq);
    println!("LastLogIndex: {}", res.last_log_index);
    println!("LastApplied: {}", res.last_applied);
    if let Some(last_log_id) = res.snapshot_last_log_id {
        println!("SnapshotLastLogID: {}", last_log_id);
    }
    if let Some(purged) = res.purged {
        println!("Purged: {}", purged);
    }
    if !res.replication.is_empty() {
        println!("Replication:");
        for (k, v) in res.replication {
            if v != res.last_applied {
                println!("  - [{}] {} *", k, v);
            } else {
                println!("  - [{}] {}", k, v);
            }
        }
    }
    if !res.voters.is_empty() {
        println!("Voters:");
        for v in res.voters {
            println!("  - {}", v);
        }
    }
    if !res.non_voters.is_empty() {
        println!("NonVoters:");
        for v in res.non_voters {
            println!("  - {}", v);
        }
    }
    Ok(())
}
