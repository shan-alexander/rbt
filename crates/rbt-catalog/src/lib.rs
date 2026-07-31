//! `rbt-catalog`: Iceberg Catalog wrapper interface supporting REST, Glue, Hive, Polaris & Nessie catalogs.

use anyhow::Result;
use iceberg::Catalog;
use iceberg::table::Table;
use iceberg::TableIdent;
use std::sync::Arc;

/// Unified catalog provider wrapping official `iceberg` catalog instances.
pub struct IcebergCatalogManager {
    pub catalog_name: String,
    pub catalog: Option<Arc<dyn Catalog>>,
}

impl IcebergCatalogManager {
    pub fn new(catalog_name: impl Into<String>) -> Self {
        Self {
            catalog_name: catalog_name.into(),
            catalog: None,
        }
    }

    pub fn with_catalog(catalog_name: impl Into<String>, catalog: Arc<dyn Catalog>) -> Self {
        Self {
            catalog_name: catalog_name.into(),
            catalog: Some(catalog),
        }
    }

    pub async fn load_table(&self, namespace: &str, table_name: &str) -> Result<Table> {
        tracing::info!("Resolving Iceberg catalog metadata for {}.{}", namespace, table_name);
        if let Some(catalog) = &self.catalog {
            let ident = TableIdent::from_strs([namespace, table_name])?;
            let table = catalog.load_table(&ident).await?;
            Ok(table)
        } else {
            anyhow::bail!("Catalog instance not initialized for catalog '{}'", self.catalog_name);
        }
    }
}
