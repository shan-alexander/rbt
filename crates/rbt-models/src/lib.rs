//! `rbt-models`: Star-schema metadata representations (dimensions, facts, business keys, grains, and relationships).

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ModelKind {
    BronzeExtract,
    SilverClean,
    Dimension,
    Fact,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelationshipSpec {
    pub column: String,
    pub to_model: String,
    pub target_field: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StarModelMeta {
    pub name: String,
    pub kind: ModelKind,
    pub business_key: Option<String>,
    pub grain: Vec<String>,
    pub relationships: Vec<RelationshipSpec>,
}
