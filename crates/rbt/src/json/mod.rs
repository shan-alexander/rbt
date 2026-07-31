//! `rbt::json`: `jshift` parse-avoiding JSONL path projection, field stamping, and filter kernels.

use anyhow::{anyhow, Result};
use arrow::array::{ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;

pub struct JsonExtractSpec {
    pub paths: Vec<String>,
}

pub struct JShiftExtractor {
    pub spec: JsonExtractSpec,
}

impl JShiftExtractor {
    pub fn new(paths: Vec<String>) -> Self {
        Self {
            spec: JsonExtractSpec { paths },
        }
    }

    /// Extracts target JSON paths from a JSONL byte stream (lines separated by `\n`)
    /// and constructs a RecordBatch according to the provided target schema.
    pub fn extract_jsonl(&self, jsonl_bytes: &[u8], schema: SchemaRef) -> Result<RecordBatch> {
        let lines: Vec<&[u8]> = jsonl_bytes
            .split(|&b| b == b'\n')
            .map(trim_bytes) // Remove trailing \r or whitespaces
            .filter(|line| !line.is_empty())
            .collect();

        let num_rows = lines.len();

        // 1. Parse target paths using jshift
        let parsed_paths: Vec<Vec<jshift::PathSegment<'_>>> = self
            .spec
            .paths
            .iter()
            .map(|p| {
                jshift::try_parse_path(p)
                    .map_err(|e| anyhow!("Failed to parse JSON path '{}': {:?}", p, e))
            })
            .collect::<Result<_>>()?;

        // 2. Initialize builders for each field in schema
        let mut builders: Vec<Box<dyn arrow::array::ArrayBuilder>> = Vec::new();
        for field in schema.fields() {
            match field.data_type() {
                DataType::Int64 => builders.push(Box::new(Int64Builder::with_capacity(num_rows))),
                DataType::Float64 => {
                    builders.push(Box::new(Float64Builder::with_capacity(num_rows)))
                }
                DataType::Boolean => {
                    builders.push(Box::new(BooleanBuilder::with_capacity(num_rows)))
                }
                DataType::Utf8 | DataType::LargeUtf8 => builders.push(Box::new(
                    StringBuilder::with_capacity(num_rows, num_rows * 16),
                )),
                other => anyhow::bail!(
                    "Unsupported target Arrow data type for JSON extraction: {:?}",
                    other
                ),
            }
        }

        // Map field name in schema to the index in self.spec.paths
        let path_indices: Vec<Option<usize>> = schema
            .fields()
            .iter()
            .map(|field| self.spec.paths.iter().position(|p| p == field.name()))
            .collect();

        // 3. For each JSONL line, parse target fields
        for line in lines {
            for (col_idx, &path_idx) in path_indices.iter().enumerate() {
                let builder = &mut builders[col_idx];
                let field = schema.field(col_idx);

                if let Some(p_idx) = path_idx {
                    let path = &parsed_paths[p_idx];
                    match jshift::find_value(line, path) {
                        Ok(val_bytes) => {
                            append_value(builder, field.data_type(), val_bytes)?;
                        }
                        Err(_) => {
                            append_null(builder, field.data_type())?;
                        }
                    }
                } else {
                    append_null(builder, field.data_type())?;
                }
            }
        }

        // 4. Construct arrays and RecordBatch
        let arrays: Vec<ArrayRef> = builders.into_iter().map(|mut b| b.finish()).collect();
        let batch = RecordBatch::try_new(schema, arrays)?;
        Ok(batch)
    }
}

fn trim_bytes(mut s: &[u8]) -> &[u8] {
    while !s.is_empty() && (s[0] == b' ' || s[0] == b'\t' || s[0] == b'\r' || s[0] == b'\n') {
        s = &s[1..];
    }
    while !s.is_empty()
        && (s[s.len() - 1] == b' '
            || s[s.len() - 1] == b'\t'
            || s[s.len() - 1] == b'\r'
            || s[s.len() - 1] == b'\n')
    {
        s = &s[..s.len() - 1];
    }
    s
}

fn unescape_json_string(val_bytes: &[u8]) -> Result<String> {
    if val_bytes.len() >= 2 && val_bytes[0] == b'"' && val_bytes[val_bytes.len() - 1] == b'"' {
        let inner = &val_bytes[1..val_bytes.len() - 1];
        let mut s = String::with_capacity(inner.len());
        let mut chars = std::str::from_utf8(inner)?.chars();
        while let Some(c) = chars.next() {
            if c == '\\' {
                if let Some(next_c) = chars.next() {
                    match next_c {
                        '"' => s.push('"'),
                        '\\' => s.push('\\'),
                        '/' => s.push('/'),
                        'b' => s.push('\x08'),
                        'f' => s.push('\x0c'),
                        'n' => s.push('\n'),
                        'r' => s.push('\r'),
                        't' => s.push('\t'),
                        other => {
                            s.push('\\');
                            s.push(other);
                        }
                    }
                } else {
                    s.push('\\');
                }
            } else {
                s.push(c);
            }
        }
        Ok(s)
    } else {
        Ok(std::str::from_utf8(val_bytes)?.to_string())
    }
}

fn append_value(
    builder: &mut Box<dyn arrow::array::ArrayBuilder>,
    data_type: &DataType,
    val_bytes: &[u8],
) -> Result<()> {
    match data_type {
        DataType::Int64 => {
            let s = std::str::from_utf8(val_bytes)?;
            let val = s.trim().parse::<i64>()?;
            builder
                .as_any_mut()
                .downcast_mut::<Int64Builder>()
                .ok_or_else(|| anyhow!("Failed to downcast builder to Int64Builder"))?
                .append_value(val);
        }
        DataType::Float64 => {
            let s = std::str::from_utf8(val_bytes)?;
            let val = s.trim().parse::<f64>()?;
            builder
                .as_any_mut()
                .downcast_mut::<Float64Builder>()
                .ok_or_else(|| anyhow!("Failed to downcast builder to Float64Builder"))?
                .append_value(val);
        }
        DataType::Boolean => {
            let val = match val_bytes {
                b"true" => true,
                b"false" => false,
                other => {
                    let s = std::str::from_utf8(other)?;
                    s.trim().parse::<bool>()?
                }
            };
            builder
                .as_any_mut()
                .downcast_mut::<BooleanBuilder>()
                .ok_or_else(|| anyhow!("Failed to downcast builder to BooleanBuilder"))?
                .append_value(val);
        }
        DataType::Utf8 | DataType::LargeUtf8 => {
            let s = unescape_json_string(val_bytes)?;
            builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .ok_or_else(|| anyhow!("Failed to downcast builder to StringBuilder"))?
                .append_value(&s);
        }
        other => anyhow::bail!("Unsupported target Arrow data type: {:?}", other),
    }
    Ok(())
}

fn append_null(
    builder: &mut Box<dyn arrow::array::ArrayBuilder>,
    data_type: &DataType,
) -> Result<()> {
    match data_type {
        DataType::Int64 => {
            builder
                .as_any_mut()
                .downcast_mut::<Int64Builder>()
                .ok_or_else(|| anyhow!("Failed to downcast builder to Int64Builder"))?
                .append_null();
        }
        DataType::Float64 => {
            builder
                .as_any_mut()
                .downcast_mut::<Float64Builder>()
                .ok_or_else(|| anyhow!("Failed to downcast builder to Float64Builder"))?
                .append_null();
        }
        DataType::Boolean => {
            builder
                .as_any_mut()
                .downcast_mut::<BooleanBuilder>()
                .ok_or_else(|| anyhow!("Failed to downcast builder to BooleanBuilder"))?
                .append_null();
        }
        DataType::Utf8 | DataType::LargeUtf8 => {
            builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .ok_or_else(|| anyhow!("Failed to downcast builder to StringBuilder"))?
                .append_null();
        }
        other => anyhow::bail!("Unsupported target Arrow data type: {:?}", other),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_extract_jsonl() -> Result<()> {
        let jsonl = b"
            {\"id\": 1, \"name\": \"Alice\", \"active\": true, \"score\": 98.5}
            {\"id\": 2, \"name\": \"Bob\", \"active\": false, \"score\": 85.0}
            {\"id\": 3, \"name\": \"Charlie\", \"active\": true}
        ";

        let paths = vec![
            "id".to_string(),
            "name".to_string(),
            "active".to_string(),
            "score".to_string(),
        ];
        let extractor = JShiftExtractor::new(paths);

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, true),
            Field::new("score", DataType::Float64, true),
        ]));

        let batch = extractor.extract_jsonl(jsonl, schema)?;

        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 4);

        // Verify values
        let id_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(id_array.value(0), 1);
        assert_eq!(id_array.value(1), 2);
        assert_eq!(id_array.value(2), 3);

        let name_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(name_array.value(0), "Alice");
        assert_eq!(name_array.value(1), "Bob");
        assert_eq!(name_array.value(2), "Charlie");

        let active_array = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();
        assert_eq!(active_array.value(0), true);
        assert_eq!(active_array.value(1), false);
        assert_eq!(active_array.value(2), true);

        let score_array = batch
            .column(3)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert_eq!(score_array.value(0), 98.5);
        assert_eq!(score_array.value(1), 85.0);
        assert!(score_array.is_null(2)); // Charlie score is missing/null

        Ok(())
    }
}
