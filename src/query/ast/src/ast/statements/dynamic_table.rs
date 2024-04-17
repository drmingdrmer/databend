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
use std::fmt::Display;
use std::fmt::Formatter;

use databend_common_meta_app::schema::CreateOption;
use derive_visitor::Drive;
use derive_visitor::DriveMut;

use crate::ast::write_comma_separated_list;
use crate::ast::write_dot_separated_list;
use crate::ast::write_space_separated_string_map;
use crate::ast::CreateTableSource;
use crate::ast::Expr;
use crate::ast::Identifier;
use crate::ast::Query;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum TargetLag {
    IntervalSecs(#[drive(skip)] u64),
    Downstream,
}

impl Display for TargetLag {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TargetLag::IntervalSecs(secs) => {
                write!(f, "{} SECOND", secs)
            }
            TargetLag::Downstream => {
                write!(f, "DOWNSTREAM")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct CreateDynamicTableStmt {
    #[drive(skip)]
    pub create_option: CreateOption,
    #[drive(skip)]
    pub transient: bool,
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub table: Identifier,
    pub source: Option<CreateTableSource>,
    pub cluster_by: Vec<Expr>,
    pub target_lag: TargetLag,
    #[drive(skip)]
    pub table_options: BTreeMap<String, String>,
    pub as_query: Box<Query>,
}

impl Display for CreateDynamicTableStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CREATE ")?;
        if let CreateOption::CreateOrReplace = self.create_option {
            write!(f, "OR REPLACE ")?;
        }
        if self.transient {
            write!(f, "TRANSIENT ")?;
        }
        write!(f, "DYNAMIC TABLE ")?;
        if let CreateOption::CreateIfNotExists = self.create_option {
            write!(f, "IF NOT EXISTS ")?;
        }
        write_dot_separated_list(
            f,
            self.catalog
                .iter()
                .chain(&self.database)
                .chain(Some(&self.table)),
        )?;

        if let Some(source) = &self.source {
            write!(f, " {source}")?;
        }

        if !self.cluster_by.is_empty() {
            write!(f, " CLUSTER BY (")?;
            write_comma_separated_list(f, &self.cluster_by)?;
            write!(f, ")")?
        }

        write!(f, " TARGET_LAG = {}", self.target_lag)?;

        // Format table options
        if !self.table_options.is_empty() {
            write!(f, " ")?;
            write_space_separated_string_map(f, &self.table_options)?;
        }

        write!(f, " AS {}", self.as_query)?;
        Ok(())
    }
}
