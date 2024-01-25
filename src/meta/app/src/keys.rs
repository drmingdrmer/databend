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

use databend_common_meta_kvapi::kvapi;

use crate::background::BackgroundJobId;
use crate::background::BackgroundJobIdent;
use crate::background::BackgroundTaskIdent;
use crate::data_mask::DatamaskId;
use crate::data_mask::DatamaskNameIdent;
use crate::data_mask::MaskpolicyTableIdListKey;
use crate::principal::connection_ident::ConnectionIdent;
use crate::principal::user_defined_file_format_ident::UserDefinedFileFormatIdent;
use crate::principal::NetworkPolicyIdent;
use crate::principal::PasswordPolicyIdent;
use crate::principal::RoleIdent;
use crate::principal::SettingIdent;
use crate::principal::StageFileIdent;
use crate::principal::StageIdent;
use crate::principal::TenantOwnershipObject;
use crate::principal::TenantUserIdent;
use crate::principal::UdfName;
use crate::schema::CatalogId;
use crate::schema::CatalogIdToName;
use crate::schema::CatalogNameIdent;
use crate::schema::CountTablesKey;
use crate::schema::DBIdTableName;
use crate::schema::DatabaseId;
use crate::schema::DatabaseIdToName;
use crate::schema::DatabaseNameIdent;
use crate::schema::DbIdListKey;
use crate::schema::IndexId;
use crate::schema::IndexIdToName;
use crate::schema::IndexNameIdent;
use crate::schema::LeastVisibleTimeKey;
use crate::schema::TableCopiedFileNameIdent;
use crate::schema::TableId;
use crate::schema::TableIdListKey;
use crate::schema::TableIdToName;
use crate::schema::TableLockKey;
use crate::schema::VirtualColumnNameIdent;
use crate::share::ShareAccountNameIdent;
use crate::share::ShareConsumer;
use crate::share::ShareEndpointId;
use crate::share::ShareEndpointIdToName;
use crate::share::ShareEndpointIdent;
use crate::share::ShareGrantObject;
use crate::share::ShareId;
use crate::share::ShareIdToName;
use crate::share::ShareNameIdent;
use crate::tenant::Tenant;
use crate::tenant::TenantQuotaIdent;

/// All key variants used in meta-service.
#[derive(Debug, Clone, PartialEq, Eq, Hash, derive_more::From, derive_more::TryInto)]
pub enum AppKey {
    Tenant(Tenant),

    BackgroundJobId(BackgroundJobId),
    BackgroundJobIdent(BackgroundJobIdent),
    BackgroundTaskIdent(BackgroundTaskIdent),
    CatalogId(CatalogId),
    CatalogIdToName(CatalogIdToName),
    CatalogNameIdent(CatalogNameIdent),
    CountTablesKey(CountTablesKey),
    DBIdTableName(DBIdTableName),
    DatabaseId(DatabaseId),
    DatabaseIdToName(DatabaseIdToName),
    DatabaseNameIdent(DatabaseNameIdent),
    DatamaskId(DatamaskId),
    DatamaskNameIdent(DatamaskNameIdent),
    DbIdListKey(DbIdListKey),
    IdGenerator(IdGenerator),
    IndexId(IndexId),
    IndexIdToName(IndexIdToName),
    IndexNameIdent(IndexNameIdent),
    LeastVisibleTimeKey(LeastVisibleTimeKey),
    MaskpolicyTableIdListKey(MaskpolicyTableIdListKey),
    RoleIdent(RoleIdent),
    ShareConsumer(ShareConsumer),
    ShareEndpointId(ShareEndpointId),
    ShareEndpointIdToName(ShareEndpointIdToName),
    ShareEndpointIdent(ShareEndpointIdent),
    ShareGrantObject(ShareGrantObject),
    ShareId(ShareId),
    ShareIdToName(ShareIdToName),
    ShareNameIdent(ShareNameIdent),
    StageFileIdent(StageFileIdent),
    TableCopiedFileNameIdent(TableCopiedFileNameIdent),
    TableId(TableId),
    TableIdListKey(TableIdListKey),
    TableIdToName(TableIdToName),
    TableLockKey(TableLockKey),
    TenantOwnershipObject(TenantOwnershipObject),
    TenantQuotaIdent(TenantQuotaIdent),
    TenantUserIdent(TenantUserIdent),
    UdfName(UdfName),
    VirtualColumnNameIdent(VirtualColumnNameIdent),

    ConnectionIdent(ConnectionIdent),
    NetworkPolicyIdent(NetworkPolicyIdent),
    PasswordPolicyIdent(PasswordPolicyIdent),
    SettingIdent(SettingIdent),
    StageIdent(StageIdent),
    UserDefinedFileFormatIdent(UserDefinedFileFormatIdent),
}

impl kvapi::Key for AppKey {
    const PREFIX: &'static str = "";
    type ValueType = ();

    fn parent(&self) -> Option<String> {
        match self {
            Self::Tenant(k) => k.parent(),

            Self::BackgroundJobId(k) => k.parent(),
            Self::BackgroundJobIdent(k) => k.parent(),
            Self::BackgroundTaskIdent(k) => k.parent(),
            Self::CatalogId(k) => k.parent(),
            Self::CatalogIdToName(k) => k.parent(),
            Self::CatalogNameIdent(k) => k.parent(),
            Self::CountTablesKey(k) => k.parent(),
            Self::DBIdTableName(k) => k.parent(),
            Self::DatabaseId(k) => k.parent(),
            Self::DatabaseIdToName(k) => k.parent(),
            Self::DatabaseNameIdent(k) => k.parent(),
            Self::DatamaskId(k) => k.parent(),
            Self::DatamaskNameIdent(k) => k.parent(),
            Self::DbIdListKey(k) => k.parent(),
            Self::IdGenerator(k) => k.parent(),
            Self::IndexId(k) => k.parent(),
            Self::IndexIdToName(k) => k.parent(),
            Self::IndexNameIdent(k) => k.parent(),
            Self::LeastVisibleTimeKey(k) => k.parent(),
            Self::MaskpolicyTableIdListKey(k) => k.parent(),
            Self::RoleIdent(k) => k.parent(),
            Self::ShareConsumer(k) => k.parent(),
            Self::ShareEndpointId(k) => k.parent(),
            Self::ShareEndpointIdToName(k) => k.parent(),
            Self::ShareEndpointIdent(k) => k.parent(),
            Self::ShareGrantObject(k) => k.parent(),
            Self::ShareId(k) => k.parent(),
            Self::ShareIdToName(k) => k.parent(),
            Self::ShareNameIdent(k) => k.parent(),
            Self::StageFileIdent(k) => k.parent(),
            Self::TableCopiedFileNameIdent(k) => k.parent(),
            Self::TableId(k) => k.parent(),
            Self::TableIdListKey(k) => k.parent(),
            Self::TableIdToName(k) => k.parent(),
            Self::TableLockKey(k) => k.parent(),
            Self::TenantOwnershipObject(k) => k.parent(),
            Self::TenantQuotaIdent(k) => k.parent(),
            Self::TenantUserIdent(k) => k.parent(),
            Self::UdfName(k) => k.parent(),
            Self::VirtualColumnNameIdent(k) => k.parent(),

            Self::ConnectionIdent(k) => k.parent(),
            Self::NetworkPolicyIdent(k) => k.parent(),
            Self::PasswordPolicyIdent(k) => k.parent(),
            Self::SettingIdent(k) => k.parent(),
            Self::StageIdent(k) => k.parent(),
            Self::UserDefinedFileFormatIdent(k) => k.parent(),
        }
    }

    fn to_string_key(&self) -> String {
        match self {
            Self::Tenant(k) => k.to_string_key(),

            Self::BackgroundJobId(k) => k.to_string_key(),
            Self::BackgroundJobIdent(k) => k.to_string_key(),
            Self::BackgroundTaskIdent(k) => k.to_string_key(),
            Self::CatalogId(k) => k.to_string_key(),
            Self::CatalogIdToName(k) => k.to_string_key(),
            Self::CatalogNameIdent(k) => k.to_string_key(),
            Self::CountTablesKey(k) => k.to_string_key(),
            Self::DBIdTableName(k) => k.to_string_key(),
            Self::DatabaseId(k) => k.to_string_key(),
            Self::DatabaseIdToName(k) => k.to_string_key(),
            Self::DatabaseNameIdent(k) => k.to_string_key(),
            Self::DatamaskId(k) => k.to_string_key(),
            Self::DatamaskNameIdent(k) => k.to_string_key(),
            Self::DbIdListKey(k) => k.to_string_key(),
            Self::IdGenerator(k) => k.to_string_key(),
            Self::IndexId(k) => k.to_string_key(),
            Self::IndexIdToName(k) => k.to_string_key(),
            Self::IndexNameIdent(k) => k.to_string_key(),
            Self::LeastVisibleTimeKey(k) => k.to_string_key(),
            Self::MaskpolicyTableIdListKey(k) => k.to_string_key(),
            Self::RoleIdent(k) => k.to_string_key(),
            Self::ShareConsumer(k) => k.to_string_key(),
            Self::ShareEndpointId(k) => k.to_string_key(),
            Self::ShareEndpointIdToName(k) => k.to_string_key(),
            Self::ShareEndpointIdent(k) => k.to_string_key(),
            Self::ShareGrantObject(k) => k.to_string_key(),
            Self::ShareId(k) => k.to_string_key(),
            Self::ShareIdToName(k) => k.to_string_key(),
            Self::ShareNameIdent(k) => k.to_string_key(),
            Self::StageFileIdent(k) => k.to_string_key(),
            Self::TableCopiedFileNameIdent(k) => k.to_string_key(),
            Self::TableId(k) => k.to_string_key(),
            Self::TableIdListKey(k) => k.to_string_key(),
            Self::TableIdToName(k) => k.to_string_key(),
            Self::TableLockKey(k) => k.to_string_key(),
            Self::TenantOwnershipObject(k) => k.to_string_key(),
            Self::TenantQuotaIdent(k) => k.to_string_key(),
            Self::TenantUserIdent(k) => k.to_string_key(),
            Self::UdfName(k) => k.to_string_key(),
            Self::VirtualColumnNameIdent(k) => k.to_string_key(),

            Self::ConnectionIdent(k) => k.to_string_key(),
            Self::NetworkPolicyIdent(k) => k.to_string_key(),
            Self::PasswordPolicyIdent(k) => k.to_string_key(),
            Self::SettingIdent(k) => k.to_string_key(),
            Self::StageIdent(k) => k.to_string_key(),
            Self::UserDefinedFileFormatIdent(k) => k.to_string_key(),
        }
    }

    fn from_str_key(key: &str) -> Result<Self, KeyError> {
        // Try to parse a string key if it matches prefix.
        macro_rules! by_prefix {
            ($key: expr, $($typ: tt),+ ) => {
                $(
                    if let Ok(k) = try_from_str::<$typ>($key) {
                        return Ok(k);
                    }
                )+
            };
        }

        by_prefix!(
            key,
            // key types:
            Tenant,
            BackgroundJobId,
            BackgroundJobIdent,
            BackgroundTaskIdent,
            CatalogId,
            CatalogIdToName,
            CatalogNameIdent,
            CountTablesKey,
            DBIdTableName,
            DatabaseId,
            DatabaseIdToName,
            DatabaseNameIdent,
            DatamaskId,
            DatamaskNameIdent,
            DbIdListKey,
            IdGenerator,
            IndexId,
            IndexIdToName,
            IndexNameIdent,
            LeastVisibleTimeKey,
            MaskpolicyTableIdListKey,
            RoleIdent,
            ShareConsumer,
            ShareEndpointId,
            ShareEndpointIdToName,
            ShareEndpointIdent,
            ShareGrantObject,
            ShareId,
            ShareIdToName,
            ShareNameIdent,
            StageFileIdent,
            TableCopiedFileNameIdent,
            TableId,
            TableIdListKey,
            TableIdToName,
            TableLockKey,
            TenantOwnershipObject,
            TenantQuotaIdent,
            TenantUserIdent,
            UdfName,
            VirtualColumnNameIdent,
            ConnectionIdent,
            NetworkPolicyIdent,
            PasswordPolicyIdent,
            SettingIdent,
            StageIdent,
            UserDefinedFileFormatIdent,
        );

        Err(KeyError::UnknownPrefix {
            prefix: key.to_string(),
        })
    }
}

fn try_from_str<K: kvapi::Key>(key: &str) -> Result<AppKey, kvapi::KeyError> {
    let k = K::from_str_key(key)?;
    Ok(AppKey::from(k))
}
