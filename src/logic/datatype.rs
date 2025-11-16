use derive_more::{Debug, Display};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Display)]
pub enum DataType {
    Boolean,
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
    #[display("Enum([{}])", "_0.join(\", \")")]
    Enum(Vec<String>),
    #[display("Set([{}])", "_0.join(\", \")")]
    Set(Vec<String>),
    Float8,
    Float,
    #[display("Double({}, {})", "_0", "_1")]
    Double(Option<usize>, usize),
    #[display("Numeric({}, {})", "_0", "_1")]
    Numeric(usize, usize),
    #[display(
        "DateTime({})",
        "match _0 { Some(v) => v.to_string(), None => \"None\".to_string() }"
    )]
    DateTime(Option<usize>),
    Timestamp,
    Timestamptz,
    #[display(
        "Time({})",
        "match _0 { Some(v) => v.to_string(), None => \"None\".to_string() }"
    )]
    Time(Option<usize>),
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
    Date,
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
    Named,
    Json,
    Bit(usize),
    Bytea,
    Inet4,
    Inet6,
    Uuid,
}
