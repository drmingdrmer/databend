// Copyright 2021 Datafuse Labs.
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
//

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::CreateDatabaseReply;
use common_meta_types::CreateDatabaseReq;
use common_meta_types::DropDatabaseReq;

use crate::catalogs::catalog::Catalog1;
use crate::catalogs::Database1;
use crate::configs::Config;
use crate::datasources::database::system::SystemDatabase1;

/// System Catalog contains ... all the system databases (no surprise :)
/// Currently, this is only one database here, the "system" db.
/// "information_schema" db is supposed to held here
#[derive(Clone)]
pub struct ImmutableCatalog {
    sys_db: Arc<SystemDatabase1>,
}

impl ImmutableCatalog {
    pub fn try_create_with_config(conf: &Config) -> Result<Self> {
        // Here we only register a system database here.
        let sys_db = Arc::new(SystemDatabase1::create(conf));
        Ok(Self { sys_db })
    }
}

#[async_trait::async_trait]
impl Catalog1 for ImmutableCatalog {
    async fn get_databases(&self) -> Result<Vec<Arc<dyn Database1>>> {
        Ok(vec![self.sys_db.clone()])
    }

    async fn get_database(&self, db_name: &str) -> Result<Arc<dyn Database1>> {
        if db_name == "system" {
            return Ok(self.sys_db.clone());
        }
        Err(ErrorCode::UnknownDatabase(format!(
            "Unknown database {}",
            db_name
        )))
    }

    async fn create_database(&self, _req: CreateDatabaseReq) -> Result<CreateDatabaseReply> {
        Err(ErrorCode::UnImplement("Cannot create system database"))
    }

    async fn drop_database(&self, _req: DropDatabaseReq) -> Result<()> {
        Err(ErrorCode::UnImplement("Cannot drop system database"))
    }
}
