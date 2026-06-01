use crate::config::TableRef;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartitionStrategy {
    None,
    Range {
        column: String,
        start: String,
        end: String,
        partitions: usize,
    },
    PredicateList {
        predicates: Vec<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Partition {
    pub id: String,
    pub predicate: Option<String>,
}

impl PartitionStrategy {
    pub fn plan(&self, table: Option<&TableRef>) -> Vec<Partition> {
        match self {
            PartitionStrategy::None => vec![Partition {
                id: "single".into(),
                predicate: None,
            }],
            PartitionStrategy::PredicateList { predicates } => predicates
                .iter()
                .enumerate()
                .map(|(idx, predicate)| Partition {
                    id: format!("p{idx:08}"),
                    predicate: Some(predicate.clone()),
                })
                .collect(),
            PartitionStrategy::Range {
                column,
                start,
                end,
                partitions,
            } => {
                let label = table
                    .map(|t| t.path_name())
                    .unwrap_or_else(|| "query".into());
                (0..*partitions).map(|idx| Partition { id: format!("{label}/p{idx:08}"), predicate: Some(format!("{column} >= {start} and {column} <= {end} and 1 = 1 -- slice {idx} of {partitions}")) }).collect()
            }
        }
    }
}
