pub mod mysql;
pub mod metadata;
pub mod codec;
pub mod config;
pub mod manager;
pub mod postgres;
pub mod loader;
pub mod dialect;

// 表元数据
#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct TableMeta {
    // 数据库名
    pub schema: String,
    // 表名
    pub name: String,
    // 字段名 -> 字段元数据
    pub columns: fnv::FnvHashMap<String, ColumnMeta>,
    // 表注释
    pub comment: Option<String>,
}

// 字段元数据
#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct ColumnMeta {
    // 字段名
    pub field: String,
    // 字段类型
    pub type_name: String,
    // 是否为空
    pub null: Option<String>,
    // 默认值
    pub default: Option<String>,
    // 字段注释
    pub comment: Option<String>,
    // 索引类型
    pub key: Option<String>,
    // 额外信息
    pub extra: Option<String>,
}