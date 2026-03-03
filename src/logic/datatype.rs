use derive_more::{Debug, Display};
use serde::{Deserialize, Serialize};

#[derive(
    Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Display, Deserialize, Serialize,
)]
pub enum DataType {
    // Boolean
    Boolean,

    // Integer types
    #[display(
        "TinyInt({})",
        "match _0 { Some(v) => v.to_string(), None => \"None\".to_string() }"
    )]
    TinyInt(Option<usize>),
    #[display(
        "SmallInt({})",
        "match _0 { Some(v) => v.to_string(), None => \"None\".to_string() }"
    )]
    SmallInt(Option<usize>),
    #[display(
        "Integer({})",
        "match _0 { Some(v) => v.to_string(), None => \"None\".to_string() }"
    )]
    Integer(Option<usize>),
    #[display(
        "Int({})",
        "match _0 { Some(v) => v.to_string(), None => \"None\".to_string() }"
    )]
    Int(Option<usize>),
    #[display(
        "BigInt({})",
        "match _0 { Some(v) => v.to_string(), None => \"None\".to_string() }"
    )]
    BigInt(Option<usize>),

    // Serial types (auto-incrementing integers)
    SmallSerial,
    Serial,
    BigSerial,

    // Character types
    #[display(
        "Char({})",
        "match _0 { Some(v) => v.to_string(), None => \"None\".to_string() }"
    )]
    Char(Option<usize>),
    #[display(
        "VarChar({})",
        "match _0 { Some(v) => v.to_string(), None => \"None\".to_string() }"
    )]
    VarChar(Option<usize>),
    #[display(
        "TinyText({})",
        "match _0 { Some(v) => v.to_string(), None => \"None\".to_string() }"
    )]
    TinyText(Option<usize>),
    #[display(
        "MediumText({})",
        "match _0 { Some(v) => v.to_string(), None => \"None\".to_string() }"
    )]
    MediumText(Option<usize>),
    #[display(
        "Text({})",
        "match _0 { Some(v) => v.to_string(), None => \"None\".to_string() }"
    )]
    Text(Option<usize>),
    #[display(
        "LongText({})",
        "match _0 { Some(v) => v.to_string(), None => \"None\".to_string() }"
    )]
    LongText(Option<usize>),
    Name, // PostgreSQL internal type for identifiers

    // Enum and Set
    #[display("Enum([{}])", "_0.join(\", \")")]
    Enum(Vec<String>),
    #[display("Set([{}])", "_0.join(\", \")")]
    Set(Vec<String>),

    // Floating point types
    Real,   // float4
    Float8, // double precision
    Float,
    #[display("Double({}, {})", "_0", "_1")]
    Double(Option<usize>, usize),
    #[display("Numeric({}, {})", "_0", "_1")]
    Numeric(usize, usize),
    DoublePrecision,
    Decimal,
    Money,

    // Date/Time types
    #[display(
        "DateTime({})",
        "match _0 { Some(v) => v.to_string(), None => \"None\".to_string() }"
    )]
    DateTime(Option<usize>),
    Timestamp,
    TimestampWithTimeZone,
    TimestampWithoutTimeZone,
    Timestamptz,
    #[display(
        "Time({})",
        "match _0 { Some(v) => v.to_string(), None => \"None\".to_string() }"
    )]
    Time(Option<usize>),
    TimeWithTimeZone,
    TimeWithoutTimeZone,
    Timetz,
    Date,
    Interval,

    // Binary types
    #[display(
        "TinyBlob({})",
        "match _0 { Some(v) => v.to_string(), None => \"None\".to_string() }"
    )]
    TinyBlob(Option<usize>),
    #[display(
        "MediumBlob({})",
        "match _0 { Some(v) => v.to_string(), None => \"None\".to_string() }"
    )]
    MediumBlob(Option<usize>),
    #[display(
        "Blob({})",
        "match _0 { Some(v) => v.to_string(), None => \"None\".to_string() }"
    )]
    Blob(Option<usize>),
    #[display(
        "LongBlob({})",
        "match _0 { Some(v) => v.to_string(), None => \"None\".to_string() }"
    )]
    LongBlob(Option<usize>),
    VarBinary(usize),
    #[display(
        "Binary({})",
        "match _0 { Some(v) => v.to_string(), None => \"None\".to_string() }"
    )]
    Binary(Option<usize>),
    Bytea,

    // Bit string types
    Bit(usize),
    BitVarying,
    Varbit,

    // Network address types
    Inet,
    Inet4,
    Inet6,
    Cidr,
    MacAddr,
    MacAddr8,

    // UUID
    Uuid,

    // JSON types
    Json,
    Jsonb,
    JsonPath,

    // XML
    Xml,

    // Geometric types
    Point,
    Line,
    Lseg,
    Box,
    Path,
    Polygon,
    Circle,

    // Text search types
    TsVector,
    TsQuery,

    // Range types
    Int4Range,
    Int8Range,
    NumRange,
    TsRange,
    TsTzRange,
    DateRange,
    Int4MultiRange,
    Int8MultiRange,
    NumMultiRange,
    TsMultiRange,
    TsTzMultiRange,
    DateMultiRange,

    // Object identifier types
    Oid,
    RegProc,
    RegProcedure,
    RegOper,
    RegOperator,
    RegClass,
    RegType,
    RegRole,
    RegNamespace,
    RegConfig,
    RegDictionary,

    // pg_lsn type
    PgLsn,
    PgSnapshot,

    // Array type
    Array,

    // User-defined type (enums, composite types, etc.)
    UserDefined,

    // Large object
    #[display(
        "Oid({})",
        "match _0 { Some(v) => v.to_string(), None => \"None\".to_string() }"
    )]
    LargeObject(Option<usize>),

    // Internal PostgreSQL types
    Cid,
    Xid,
    Xid8,
    Tid,
    PgNodeTree,
    PgNdistinct,
    PgDependencies,
    PgMcvList,
    PgBrinBloomSummary,
    PgBrinMinmaxMultiSummary,
    AclItem,
    Internal,
    Record,
    Trigger,
    EventTrigger,
    LanguageHandler,
    FdwHandler,
    IndexAmHandler,
    TableAmHandler,
    TsmHandler,
    AnyElement,
    AnyArray,
    AnyNonArray,
    AnyEnum,
    AnyRange,
    AnyMultiRange,
    AnyCompatible,
    AnyCompatibleArray,
    AnyCompatibleNonArray,
    AnyCompatibleRange,
    AnyCompatibleMultiRange,
    CString,
    Void,
    Refcursor,

    // Named (legacy)
    Named,

    #[default]
    Unknown,
}

impl From<&str> for DataType {
    fn from(value: &str) -> Self {
        // Normalize: trim whitespace and convert to lowercase for matching
        let normalized = value.trim().to_lowercase();
        let normalized = normalized.as_str();

        match normalized {
            // Boolean
            "boolean" | "bool" => DataType::Boolean,

            // Integer types
            "smallint" | "int2" => DataType::SmallInt(None),
            "integer" | "int4" | "int" => DataType::Integer(None),
            "bigint" | "int8" => DataType::BigInt(None),
            "tinyint" => DataType::TinyInt(None),

            // Serial types
            "smallserial" | "serial2" => DataType::SmallSerial,
            "serial" | "serial4" => DataType::Serial,
            "bigserial" | "serial8" => DataType::BigSerial,

            // Character types
            "character" | "char" | "bpchar" => DataType::Char(None),
            "character varying" | "varchar" => DataType::VarChar(None),
            "text" => DataType::Text(None),
            "name" => DataType::Name,
            "\"char\"" | "char\"" => DataType::Char(None), // Internal single-byte char type

            // Floating point types
            "real" | "float4" => DataType::Real,
            "double precision" | "float8" => DataType::DoublePrecision,
            "float" => DataType::Float,
            "numeric" | "decimal" => DataType::Decimal,
            "money" => DataType::Money,

            // Date/Time types
            "timestamp with time zone" | "timestamptz" => DataType::TimestampWithTimeZone,
            "timestamp without time zone" | "timestamp" => DataType::TimestampWithoutTimeZone,
            "time with time zone" | "timetz" => DataType::TimeWithTimeZone,
            "time without time zone" | "time" => DataType::TimeWithoutTimeZone,
            "date" => DataType::Date,
            "interval" => DataType::Interval,

            // Binary types
            "bytea" => DataType::Bytea,

            // Bit string types
            "bit" => DataType::Bit(1),
            "bit varying" | "varbit" => DataType::BitVarying,

            // Network address types
            "inet" => DataType::Inet,
            "cidr" => DataType::Cidr,
            "macaddr" => DataType::MacAddr,
            "macaddr8" => DataType::MacAddr8,

            // UUID
            "uuid" => DataType::Uuid,

            // JSON types
            "json" => DataType::Json,
            "jsonb" => DataType::Jsonb,
            "jsonpath" => DataType::JsonPath,

            // XML
            "xml" => DataType::Xml,

            // Geometric types
            "point" => DataType::Point,
            "line" => DataType::Line,
            "lseg" => DataType::Lseg,
            "box" => DataType::Box,
            "path" => DataType::Path,
            "polygon" => DataType::Polygon,
            "circle" => DataType::Circle,

            // Text search types
            "tsvector" => DataType::TsVector,
            "tsquery" => DataType::TsQuery,

            // Range types
            "int4range" => DataType::Int4Range,
            "int8range" => DataType::Int8Range,
            "numrange" => DataType::NumRange,
            "tsrange" => DataType::TsRange,
            "tstzrange" => DataType::TsTzRange,
            "daterange" => DataType::DateRange,

            // Multirange types (PostgreSQL 14+)
            "int4multirange" => DataType::Int4MultiRange,
            "int8multirange" => DataType::Int8MultiRange,
            "nummultirange" => DataType::NumMultiRange,
            "tsmultirange" => DataType::TsMultiRange,
            "tstzmultirange" => DataType::TsTzMultiRange,
            "datemultirange" => DataType::DateMultiRange,

            // Object identifier types
            "oid" => DataType::Oid,
            "regproc" => DataType::RegProc,
            "regprocedure" => DataType::RegProcedure,
            "regoper" => DataType::RegOper,
            "regoperator" => DataType::RegOperator,
            "regclass" => DataType::RegClass,
            "regtype" => DataType::RegType,
            "regrole" => DataType::RegRole,
            "regnamespace" => DataType::RegNamespace,
            "regconfig" => DataType::RegConfig,
            "regdictionary" => DataType::RegDictionary,

            // pg_lsn type
            "pg_lsn" => DataType::PgLsn,
            "pg_snapshot" => DataType::PgSnapshot,

            // Array type (information_schema reports this for array columns)
            "array" => DataType::Array,

            // User-defined type (enums, composite types, domains)
            "user-defined" => DataType::UserDefined,

            // Internal PostgreSQL types
            "cid" => DataType::Cid,
            "xid" => DataType::Xid,
            "xid8" => DataType::Xid8,
            "tid" => DataType::Tid,
            "pg_node_tree" => DataType::PgNodeTree,
            "pg_ndistinct" => DataType::PgNdistinct,
            "pg_dependencies" => DataType::PgDependencies,
            "pg_mcv_list" => DataType::PgMcvList,
            "pg_brin_bloom_summary" => DataType::PgBrinBloomSummary,
            "pg_brin_minmax_multi_summary" => DataType::PgBrinMinmaxMultiSummary,
            "aclitem" => DataType::AclItem,
            "internal" => DataType::Internal,
            "record" => DataType::Record,
            "trigger" => DataType::Trigger,
            "event_trigger" => DataType::EventTrigger,
            "language_handler" => DataType::LanguageHandler,
            "fdw_handler" => DataType::FdwHandler,
            "index_am_handler" => DataType::IndexAmHandler,
            "table_am_handler" => DataType::TableAmHandler,
            "tsm_handler" => DataType::TsmHandler,
            "anyelement" => DataType::AnyElement,
            "anyarray" => DataType::AnyArray,
            "anynonarray" => DataType::AnyNonArray,
            "anyenum" => DataType::AnyEnum,
            "anyrange" => DataType::AnyRange,
            "anymultirange" => DataType::AnyMultiRange,
            "anycompatible" => DataType::AnyCompatible,
            "anycompatiblearray" => DataType::AnyCompatibleArray,
            "anycompatiblenonarray" => DataType::AnyCompatibleNonArray,
            "anycompatiblerange" => DataType::AnyCompatibleRange,
            "anycompatiblemultirange" => DataType::AnyCompatibleMultiRange,
            "cstring" => DataType::CString,
            "void" => DataType::Void,
            "refcursor" => DataType::Refcursor,

            // Handle types with array suffix (e.g., "integer[]", "_int4")
            s if s.ends_with("[]") || s.starts_with('_') => DataType::Array,

            // Handle types with size specifications in parentheses
            // e.g., "character varying(255)", "numeric(10,2)", "bit(8)"
            s if s.contains('(') => {
                // Extract base type name
                let base_type = s.split('(').next().unwrap_or(s).trim();
                match base_type {
                    "character" | "char" | "bpchar" => DataType::Char(None),
                    "character varying" | "varchar" => DataType::VarChar(None),
                    "numeric" | "decimal" => DataType::Decimal,
                    "bit" => DataType::Bit(1),
                    "bit varying" | "varbit" => DataType::BitVarying,
                    "timestamp with time zone" | "timestamptz" => DataType::TimestampWithTimeZone,
                    "timestamp without time zone" | "timestamp" => {
                        DataType::TimestampWithoutTimeZone
                    }
                    "time with time zone" | "timetz" => DataType::TimeWithTimeZone,
                    "time without time zone" | "time" => DataType::TimeWithoutTimeZone,
                    "interval" => DataType::Interval,
                    _ => DataType::Unknown,
                }
            }

            // Fallback for unknown types
            _ => DataType::Unknown,
        }
    }
}

impl From<String> for DataType {
    fn from(value: String) -> Self {
        DataType::from(value.as_str())
    }
}

impl From<&String> for DataType {
    fn from(value: &String) -> Self {
        DataType::from(value.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_types() {
        assert_eq!(DataType::from("boolean"), DataType::Boolean);
        assert_eq!(DataType::from("bool"), DataType::Boolean);
        assert_eq!(DataType::from("integer"), DataType::Integer(None));
        assert_eq!(DataType::from("int4"), DataType::Integer(None));
        assert_eq!(DataType::from("bigint"), DataType::BigInt(None));
        assert_eq!(DataType::from("smallint"), DataType::SmallInt(None));
    }

    #[test]
    fn test_text_types() {
        assert_eq!(DataType::from("text"), DataType::Text(None));
        assert_eq!(DataType::from("character varying"), DataType::VarChar(None));
        assert_eq!(DataType::from("varchar"), DataType::VarChar(None));
        assert_eq!(DataType::from("character"), DataType::Char(None));
        assert_eq!(DataType::from("char"), DataType::Char(None));
    }

    #[test]
    fn test_timestamp_types() {
        assert_eq!(
            DataType::from("timestamp with time zone"),
            DataType::TimestampWithTimeZone
        );
        assert_eq!(
            DataType::from("timestamp without time zone"),
            DataType::TimestampWithoutTimeZone
        );
        assert_eq!(
            DataType::from("timestamptz"),
            DataType::TimestampWithTimeZone
        );
        assert_eq!(DataType::from("date"), DataType::Date);
    }

    #[test]
    fn test_json_types() {
        assert_eq!(DataType::from("json"), DataType::Json);
        assert_eq!(DataType::from("jsonb"), DataType::Jsonb);
    }

    #[test]
    fn test_network_types() {
        assert_eq!(DataType::from("inet"), DataType::Inet);
        assert_eq!(DataType::from("cidr"), DataType::Cidr);
        assert_eq!(DataType::from("macaddr"), DataType::MacAddr);
    }

    #[test]
    fn test_uuid() {
        assert_eq!(DataType::from("uuid"), DataType::Uuid);
    }

    #[test]
    fn test_array_types() {
        assert_eq!(DataType::from("ARRAY"), DataType::Array);
        assert_eq!(DataType::from("integer[]"), DataType::Array);
        assert_eq!(DataType::from("_int4"), DataType::Array);
    }

    #[test]
    fn test_user_defined() {
        assert_eq!(DataType::from("USER-DEFINED"), DataType::UserDefined);
    }

    #[test]
    fn test_case_insensitive() {
        assert_eq!(DataType::from("BOOLEAN"), DataType::Boolean);
        assert_eq!(DataType::from("Integer"), DataType::Integer(None));
        assert_eq!(DataType::from("TEXT"), DataType::Text(None));
    }

    #[test]
    fn test_unknown() {
        assert_eq!(DataType::from("some_unknown_type"), DataType::Unknown);
    }
}
