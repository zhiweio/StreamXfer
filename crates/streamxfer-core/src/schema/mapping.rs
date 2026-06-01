use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArrowType {
    Boolean,
    UInt8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Decimal128 { precision: u8, scale: i8 },
    Date32,
    TimestampNanos,
    Binary,
    Utf8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlColumn {
    pub name: String,
    pub sql_type: String,
    pub nullable: bool,
    pub precision: Option<u8>,
    pub scale: Option<i8>,
}

impl SqlColumn {
    pub fn arrow_type(&self) -> ArrowType {
        sql_type_to_arrow(&self.sql_type, self.precision, self.scale)
    }
}

pub fn sql_type_to_arrow(sql_type: &str, precision: Option<u8>, scale: Option<i8>) -> ArrowType {
    match sql_type.to_ascii_lowercase().as_str() {
        "bit" => ArrowType::Boolean,
        "tinyint" => ArrowType::UInt8,
        "smallint" => ArrowType::Int16,
        "int" => ArrowType::Int32,
        "bigint" => ArrowType::Int64,
        "real" => ArrowType::Float32,
        "float" => ArrowType::Float64,
        "decimal" | "numeric" | "money" | "smallmoney" => ArrowType::Decimal128 {
            precision: precision.unwrap_or(38),
            scale: scale.unwrap_or(10),
        },
        "date" => ArrowType::Date32,
        "datetime" | "datetime2" | "smalldatetime" | "datetimeoffset" | "time" => {
            ArrowType::TimestampNanos
        }
        "binary" | "varbinary" | "image" | "timestamp" | "rowversion" => ArrowType::Binary,
        "uniqueidentifier" | "char" | "nchar" | "varchar" | "nvarchar" | "text" | "ntext"
        | "xml" | "sql_variant" => ArrowType::Utf8,
        _ => ArrowType::Utf8,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn maps_decimal_and_strings() {
        assert_eq!(
            sql_type_to_arrow("decimal", Some(18), Some(2)),
            ArrowType::Decimal128 {
                precision: 18,
                scale: 2
            }
        );
        assert_eq!(sql_type_to_arrow("nvarchar", None, None), ArrowType::Utf8);
    }
}
