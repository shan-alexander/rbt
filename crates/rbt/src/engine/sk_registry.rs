//! Durable NK → Int64 surrogate-key registry for MIISK (ADR-009).
//!
//! Path: `{project}/.rbt/sk_registry/{model}.parquet`
//! Schema: `_rbt_sk_key` (Utf8 grain encoding) + `sk` (Int64).
//! Reserved: `sk = 0` is the Unknown member (never allocated to real grain).

use anyhow::{bail, Context, Result};
use arrow::array::{Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::surrogate_key::{cell_to_sk_str, SK_NULL_TOKEN};

pub const REGISTRY_KEY_COL: &str = "_rbt_sk_key";
pub const REGISTRY_SK_COL: &str = "sk";

/// First allocatable SK (0 reserved for Unknown).
pub const MIISK_FIRST_SK: i64 = 1;

/// Encode grain field strings into a stable registry lookup key.
pub fn grain_key(fields: &[&str]) -> String {
    fields.join("\u{1f}")
}

pub fn default_registry_path(project_dir: &Path, model: &str) -> PathBuf {
    let safe: String = model
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect();
    project_dir
        .join(".rbt")
        .join("sk_registry")
        .join(format!("{safe}.parquet"))
}

#[derive(Debug, Default, Clone)]
pub struct SkRegistry {
    /// grain_key → sk (sk never 0 for real members)
    map: HashMap<String, i64>,
    next_sk: i64,
}

impl SkRegistry {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
            next_sk: MIISK_FIRST_SK,
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn get(&self, key: &str) -> Option<i64> {
        self.map.get(key).copied()
    }

    /// Lookup or allocate a new positive SK for `key`.
    pub fn get_or_assign(&mut self, key: &str) -> i64 {
        if let Some(&sk) = self.map.get(key) {
            return sk;
        }
        let sk = self.next_sk;
        self.next_sk = self.next_sk.saturating_add(1);
        self.map.insert(key.to_string(), sk);
        sk
    }

    pub fn load(path: &Path) -> Result<Self> {
        if !path.is_file() {
            return Ok(Self::new());
        }
        let file = File::open(path)
            .with_context(|| format!("E_RBT_SK: open MIISK registry {}", path.display()))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .with_context(|| format!("E_RBT_SK: registry parquet {}", path.display()))?
            .build()
            .with_context(|| format!("E_RBT_SK: registry reader {}", path.display()))?;
        let mut map = HashMap::new();
        let mut max_sk = 0i64;
        for batch in reader {
            let batch = batch.context("E_RBT_SK: registry batch")?;
            let key_idx = batch.schema().index_of(REGISTRY_KEY_COL).with_context(|| {
                format!("E_RBT_SK: registry missing {REGISTRY_KEY_COL} in {}", path.display())
            })?;
            let sk_idx = batch.schema().index_of(REGISTRY_SK_COL).with_context(|| {
                format!("E_RBT_SK: registry missing {REGISTRY_SK_COL} in {}", path.display())
            })?;
            let keys = batch
                .column(key_idx)
                .as_any()
                .downcast_ref::<StringArray>()
                .context("E_RBT_SK: registry key column must be Utf8")?;
            let sks = batch
                .column(sk_idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("E_RBT_SK: registry sk column must be Int64")?;
            for i in 0..batch.num_rows() {
                if keys.is_null(i) || sks.is_null(i) {
                    continue;
                }
                let sk = sks.value(i);
                if sk == 0 {
                    continue; // never store Unknown in registry map
                }
                if sk > max_sk {
                    max_sk = sk;
                }
                map.insert(keys.value(i).to_string(), sk);
            }
        }
        Ok(Self {
            map,
            next_sk: max_sk.max(0).saturating_add(1).max(MIISK_FIRST_SK),
        })
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("E_RBT_SK: create registry dir {}", parent.display())
            })?;
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new(REGISTRY_KEY_COL, DataType::Utf8, false),
            Field::new(REGISTRY_SK_COL, DataType::Int64, false),
        ]));
        let mut keys = Vec::with_capacity(self.map.len());
        let mut sks = Vec::with_capacity(self.map.len());
        // Stable write order
        let mut pairs: Vec<(&String, &i64)> = self.map.iter().collect();
        pairs.sort_by(|a, b| a.0.cmp(b.0));
        for (k, sk) in pairs {
            keys.push(k.as_str());
            sks.push(*sk);
        }
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(Int64Array::from(sks)),
            ],
        )
        .context("E_RBT_SK: build registry batch")?;

        let partial = path.with_extension("parquet.rbt-partial");
        let _ = fs::remove_file(&partial);
        let file = File::create(&partial)
            .with_context(|| format!("E_RBT_SK: create {}", partial.display()))?;
        let buf = BufWriter::with_capacity(1024 * 1024, file);
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer = ArrowWriter::try_new(buf, schema, Some(props))
            .context("E_RBT_SK: registry ArrowWriter")?;
        writer.write(&batch).context("E_RBT_SK: registry write")?;
        writer.close().context("E_RBT_SK: registry close")?;
        fs::rename(&partial, path).with_context(|| {
            format!(
                "E_RBT_SK: publish registry {} → {}",
                partial.display(),
                path.display()
            )
        })?;
        Ok(())
    }
}

/// Assign MIISK Int64 SKs for every row; persist registry. Returns Int64 array.
///
/// New grains get the next positive id; existing grains reuse registry values.
pub fn assign_miisk_column(
    batch: &RecordBatch,
    grain_cols: &[String],
    registry_path: &Path,
) -> Result<(Arc<Int64Array>, SkRegistry)> {
    if grain_cols.is_empty() {
        bail!("E_RBT_SK: MIISK requires grain columns");
    }
    let mut reg = SkRegistry::load(registry_path)?;
    let mut idxs = Vec::with_capacity(grain_cols.len());
    for name in grain_cols {
        let idx = batch.schema().index_of(name).with_context(|| {
            format!(
                "E_RBT_SK: MIISK grain column '{name}' missing from batch \
                 (registry {})",
                registry_path.display()
            )
        })?;
        idxs.push(idx);
    }

    let n = batch.num_rows();
    let mut out = Vec::with_capacity(n);
    let mut field_bufs = vec![String::new(); grain_cols.len()];
    for row in 0..n {
        for (i, &col_idx) in idxs.iter().enumerate() {
            field_bufs[i] = cell_to_sk_str(batch.column(col_idx).as_ref(), row)?;
        }
        // All-null grain → treat as Unknown sentinel request? Prefer assign only
        // real keys; if every field is null token, map to 0 without storing.
        if field_bufs.iter().all(|s| s == SK_NULL_TOKEN) {
            out.push(0i64);
            continue;
        }
        let refs: Vec<&str> = field_bufs.iter().map(|s| s.as_str()).collect();
        let key = grain_key(&refs);
        out.push(reg.get_or_assign(&key));
    }
    reg.save(registry_path)?;
    Ok((Arc::new(Int64Array::from(out)), reg))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use tempfile::tempdir;

    #[test]
    fn assign_stable_across_reload() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("dim.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec!["a", "b", "a"]))],
        )
        .unwrap();
        let (sk1, _) = assign_miisk_column(&batch, &["id".into()], &path).unwrap();
        assert_eq!(sk1.value(0), 1);
        assert_eq!(sk1.value(1), 2);
        assert_eq!(sk1.value(2), 1); // same grain → same sk

        let batch2 = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["b", "c"]))],
        )
        .unwrap();
        let (sk2, _) = assign_miisk_column(&batch2, &["id".into()], &path).unwrap();
        assert_eq!(sk2.value(0), 2); // b reused
        assert_eq!(sk2.value(1), 3); // c new
    }

    #[test]
    fn unknown_all_null_is_zero() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("u.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![None::<&str>]))],
        )
        .unwrap();
        let (sk, reg) = assign_miisk_column(&batch, &["id".into()], &path).unwrap();
        assert_eq!(sk.value(0), 0);
        assert!(reg.is_empty());
    }
}
