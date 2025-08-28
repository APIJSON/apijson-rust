use lazy_static::lazy_static;
use std::sync::RwLock;
use std::collections::HashMap;
use fnv::FnvHashMap;
use crate::app::datasource::TableMeta;

lazy_static! {
    // 数据源 -> 数据库 -> 数据库大小
    static ref DATASOURCE_DB_CACHE: RwLock<FnvHashMap<String, FnvHashMap<String, f64>>> = RwLock::new(FnvHashMap::default());
    // 数据源 -> 数据库 -> 表名列表
    static ref DATASOURCE_TABLE_CACHE: RwLock<FnvHashMap<String, FnvHashMap<String, Vec<String>>>> = RwLock::new(FnvHashMap::default());
    // 数据源.数据库.表名 -> 表元数据
    static ref TABLE_META_CACHE: RwLock<FnvHashMap<String, TableMeta>> = RwLock::new(FnvHashMap::default());
    // 兼容性缓存（保持向后兼容）
    static ref DB_CACHE: RwLock<FnvHashMap<String, f64>> = RwLock::new(FnvHashMap::default());
    static ref DB_TABLE_CACHE: RwLock<FnvHashMap<String, Vec<String>>> = RwLock::new(FnvHashMap::default());
}

/// 向数据库缓存中添加或更新数据库元数据（兼容性函数）
/// 
/// # 参数
/// * `db_name` - 数据库名称
/// * `db_size` - 数据库大小
pub fn put_db_meta(db_name: String, db_size: f64) {
    if let Ok(mut cache) = DB_CACHE.write() {
        cache.insert(db_name, db_size);
    }
}

/// 向指定数据源的数据库缓存中添加或更新数据库元数据
/// 
/// # 参数
/// * `datasource_name` - 数据源名称
/// * `db_name` - 数据库名称
/// * `db_size` - 数据库大小
pub fn put_datasource_db_meta(datasource_name: &str, db_name: String, db_size: f64) {
    if let Ok(mut cache) = DATASOURCE_DB_CACHE.write() {
        cache.entry(datasource_name.to_string())
            .or_insert_with(FnvHashMap::default)
            .insert(db_name, db_size);
    }
}

/// 向数据库表列表缓存中添加或更新数据库的表列表（兼容性函数）
/// 
/// # 参数
/// * `schema` - 数据库名称
/// * `table_list` - 表名列表
pub fn put_db_tables(schema: String, table_list: Vec<String>) {
    if let Ok(mut cache) = DB_TABLE_CACHE.write() {
        cache.insert(schema, table_list);
    }
}

/// 向指定数据源的数据库表列表缓存中添加或更新表列表
/// 
/// # 参数
/// * `datasource_name` - 数据源名称
/// * `schema` - 数据库名称
/// * `table_list` - 表名列表
pub fn put_datasource_db_tables(datasource_name: &str, schema: String, table_list: Vec<String>) {
    if let Ok(mut cache) = DATASOURCE_TABLE_CACHE.write() {
        cache.entry(datasource_name.to_string())
            .or_insert_with(FnvHashMap::default)
            .insert(schema, table_list);
    }
}

/// 向表元数据缓存中添加或更新表的元数据（兼容性函数）
/// 
/// # 参数
/// * `schema` - 数据库名称
/// * `table` - 表名称
/// * `table_meta` - 表元数据
pub fn put_table_meta(schema: &str, table: &str, table_meta: TableMeta) {
    let table_key = format!("{}.{}", schema, table);
    if let Ok(mut cache) = TABLE_META_CACHE.write() {
        cache.insert(table_key, table_meta);
    }
}

/// 向指定数据源的表元数据缓存中添加或更新表的元数据
/// 
/// # 参数
/// * `datasource_name` - 数据源名称
/// * `schema` - 数据库名称
/// * `table` - 表名称
/// * `table_meta` - 表元数据
pub fn put_datasource_table_meta(datasource_name: &str, schema: &str, table: &str, table_meta: TableMeta) {
    let table_key = format!("{}.{}.{}", datasource_name, schema, table);
    if let Ok(mut cache) = TABLE_META_CACHE.write() {
        cache.insert(table_key, table_meta);
    }
}


/// 检查指定数据库中的表是否存在
/// 
/// # 参数
/// * `schema` - 数据库名称
/// * `table` - 表名称
/// 
/// # 返回值
/// 如果表存在返回 true，否则返回 false
pub fn is_table_exists(schema: &str, table: &str) -> bool {
    let table_key = format!("{}.{}", schema, table);
    TABLE_META_CACHE.read()
        .map(|guard| guard.contains_key(&table_key))
        .unwrap_or(false)
}

/// 获取指定数据库中表的元数据
/// 
/// # 参数
/// * `schema` - 数据库名称
/// * `table` - 表名称
/// 
/// # 返回值
/// 如果表存在，返回包含表元数据的 Some(TableMeta)，否则返回 None
pub fn get_table(schema: &str, table: &str) -> Option<TableMeta> {
    let table_key = format!("{}.{}", schema, table);
    TABLE_META_CACHE.read()
        .ok()?
        .get(&table_key)
        .cloned()
}

/// 获取数据库中所有表的名称和注释映射（兼容性函数）
/// 
/// # 参数
/// * `schema` - 数据库名称
/// 
/// # 返回值
/// 返回一个 HashMap，键为表名称，值为对应的注释字符串。
/// 如果数据库不存在或读取失败，返回空的 HashMap
pub fn get_table_name_list(schema: &str) -> HashMap<String, String> {
    let db_tables_guard = match DB_TABLE_CACHE.read() {
        Ok(guard) => guard,
        Err(_) => return HashMap::new(),
    };
    
    let tables = match db_tables_guard.get(schema) {
        Some(v) => v.clone(),
        None => return HashMap::new(),
    };
    
    let all_tables_guard = match TABLE_META_CACHE.read() {
        Ok(guard) => guard,
        Err(_) => return HashMap::new(),
    };
    
    tables.iter()
        .filter_map(|table_name| {
            all_tables_guard.get(table_name.as_str())
                .map(|table| {
                    let comment = table.comment.as_deref().unwrap_or("");
                    (table_name.clone(), comment.to_string())
                })
        })
        .collect()
}

/// 检查指定数据源中的表是否存在
/// 
/// # 参数
/// * `datasource_name` - 数据源名称
/// * `schema` - 数据库名称
/// * `table` - 表名称
/// 
/// # 返回值
/// 如果表存在返回 true，否则返回 false
pub fn is_datasource_table_exists(datasource_name: &str, schema: &str, table: &str) -> bool {
    let table_key = format!("{}.{}.{}", datasource_name, schema, table);
    TABLE_META_CACHE.read()
        .map(|guard| guard.contains_key(&table_key))
        .unwrap_or(false)
}

/// 获取指定数据源中表的元数据
/// 
/// # 参数
/// * `datasource_name` - 数据源名称
/// * `schema` - 数据库名称
/// * `table` - 表名称
/// 
/// # 返回值
/// 如果表存在，返回包含表元数据的 Some(TableMeta)，否则返回 None
pub fn get_datasource_table(datasource_name: &str, schema: &str, table: &str) -> Option<TableMeta> {
    let table_key = format!("{}.{}.{}", datasource_name, schema, table);
    TABLE_META_CACHE.read()
        .ok()?
        .get(&table_key)
        .cloned()
}

/// 获取指定数据源和数据库中所有表的名称和注释映射
/// 
/// # 参数
/// * `datasource_name` - 数据源名称
/// * `schema` - 数据库名称
/// 
/// # 返回值
/// 返回一个 HashMap，键为表名称，值为对应的注释字符串。
/// 如果数据源或数据库不存在或读取失败，返回空的 HashMap
pub fn get_datasource_table_name_list(datasource_name: &str, schema: &str) -> HashMap<String, String> {
    let ds_tables_guard = match DATASOURCE_TABLE_CACHE.read() {
        Ok(guard) => guard,
        Err(_) => return HashMap::new(),
    };
    
    let db_tables = match ds_tables_guard.get(datasource_name) {
        Some(ds_map) => ds_map,
        None => return HashMap::new(),
    };
    
    let tables = match db_tables.get(schema) {
        Some(v) => v.clone(),
        None => return HashMap::new(),
    };
    
    let all_tables_guard = match TABLE_META_CACHE.read() {
        Ok(guard) => guard,
        Err(_) => return HashMap::new(),
    };
    
    tables.iter()
        .filter_map(|table_name| {
            let table_key = format!("{}.{}.{}", datasource_name, schema, table_name);
            all_tables_guard.get(&table_key)
                .map(|table| {
                    let comment = table.comment.as_deref().unwrap_or("");
                    (table_name.clone(), comment.to_string())
                })
        })
        .collect()
}

/// 获取指定数据源的所有数据库名称
/// 
/// # 参数
/// * `datasource_name` - 数据源名称
/// 
/// # 返回值
/// 返回数据库名称的向量
pub fn get_datasource_database_names(datasource_name: &str) -> Vec<String> {
    let guard = match DATASOURCE_DB_CACHE.read() {
        Ok(guard) => guard,
        Err(_) => return Vec::new(),
    };
    
    match guard.get(datasource_name) {
        Some(db_map) => db_map.keys().cloned().collect(),
        None => Vec::new(),
    }
}

/// 获取指定数据源和数据库的大小
/// 
/// # 参数
/// * `datasource_name` - 数据源名称
/// * `database_name` - 数据库名称
/// 
/// # 返回值
/// 返回数据库大小，如果不存在返回 0.0
pub fn get_datasource_database_size(datasource_name: &str, database_name: &str) -> f64 {
    if let Ok(guard) = DATASOURCE_DB_CACHE.read() {
        if let Some(db_map) = guard.get(datasource_name) {
            if let Some(&size) = db_map.get(database_name) {
                return size;
            }
        }
    }
    0.0
}

/// 获取所有数据源名称
/// 
/// # 返回值
/// 返回所有数据源名称的向量
pub fn get_all_datasource_names() -> Vec<String> {
    let guard = match DATASOURCE_DB_CACHE.read() {
        Ok(guard) => guard,
        Err(_) => return Vec::new(),
    };
    
    let names: Vec<String> = guard.keys().cloned().collect();
    names
}
