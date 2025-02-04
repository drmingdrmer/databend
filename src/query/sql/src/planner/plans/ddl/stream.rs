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

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum StreamNavigation {
    AtStream { database: String, name: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateStreamPlan {
    pub if_not_exists: bool,
    pub tenant: String,
    pub catalog: String,
    pub database: String,
    pub stream_name: String,
    pub table_database: String,
    pub table_name: String,
    pub navigation: Option<StreamNavigation>,
    pub comment: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropStreamPlan {
    pub if_exists: bool,
    pub tenant: String,
    pub catalog: String,
    pub database: String,
    pub stream_name: String,
}
