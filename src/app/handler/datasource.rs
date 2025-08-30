use std::collections::HashMap;
use log::{info, debug, warn};
use serde_json::{json, Value};
use crate::app::common::rpc::{RpcResult, HttpCode};
use crate::app::datasource::manager::DataSourceManager;
use crate::app::datasource::metadata;

/// 处理数据源相关查询请求的统一入口
/// 
/// 根据action参数区分不同的查询类型：
/// - "info": 返回数据源信息（数据库和表的树形结构）
/// - "tables": 返回指定数据库下所有表的详细信息
/// 
/// # 参数
/// * `manager` - 数据源管理器
/// * `action` - 操作类型（"info" 或 "tables"）
/// * `body_map` - 包含请求参数的HashMap
///   
///   对于action="info"：
///   - `datasource`: 可选，指定数据源名称，如果不提供则返回所有数据源信息
///   
///   对于action="tables"：
///   - `datasource`: 必需，数据源名称
///   - `database`: 必需，数据库名称
pub async fn handle_datasource(
    manager: &DataSourceManager,
    action: &str,
    body_map: HashMap<String, Value>,
) -> RpcResult<HashMap<String, Value>> {
    info!("开始处理数据源查询请求，操作类型: {}", action);
    debug!("请求参数: {:?}", body_map);
    
    match action {
        "info" => handle_datasource_info(manager, body_map).await,
        "tables" => handle_database_tables(manager, body_map).await,
        _ => {
            let rpc_result = RpcResult {
                code: HttpCode::BadRequest,
                msg: Some(format!("不支持的操作类型: {}", action)),
                data: None,
            };
            rpc_result
        }
    }
}

/// 处理数据源信息查询请求
/// 
/// 返回指定数据源下的数据库及数据表信息的树形结构
/// 
/// # 参数
/// * `manager` - 数据源管理器
/// * `body_map` - 包含请求参数的HashMap，可选参数：
///   - `datasource`: 指定数据源名称，如果不提供则返回所有数据源信息
/// 
/// # 返回值
/// 返回树形结构的JSON数据：
/// ```json
/// {
///   "datasource_name": {
///     "type": "datasource",
///     "name": "datasource_name",
///     "databases": {
///       "database_name": {
///         "type": "database",
///         "name": "database_name",
///         "tables": {
///           "table_name": {
///             "type": "table",
///             "name": "table_name",
///             "comment": "table_comment"
///           }
///         }
///       }
///     }
///   }
/// }
/// ```
pub async fn handle_datasource_info(
    manager: &DataSourceManager,
    body_map: HashMap<String, Value>,
) -> RpcResult<HashMap<String, Value>> {
    info!("开始处理数据源信息查询请求");
    debug!("请求参数: {:?}", body_map);
    
    let mut rpc_result = RpcResult {
        code: HttpCode::Ok,
        msg: None,
        data: None,
    };
    
    // 检查是否指定了特定数据源
    let target_datasource = body_map.get("datasource")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    
    let mut result_data = HashMap::new();
    
    // 获取要处理的数据源列表
    let datasource_names = if let Some(ds_name) = target_datasource {
        // 验证指定的数据源是否存在
        let all_datasources = manager.get_datasource_names();
        if all_datasources.contains(&ds_name) {
            vec![ds_name]
        } else {
            rpc_result.code = HttpCode::NotFound;
            rpc_result.msg = Some(format!("数据源 '{}' 不存在", ds_name));
            return rpc_result;
        }
    } else {
        manager.get_datasource_names()
    };
    
    info!("处理 {} 个数据源", datasource_names.len());
    
    // 遍历每个数据源
    for datasource_name in datasource_names {
        debug!("处理数据源: {}", datasource_name);
        
        let mut datasource_info = HashMap::new();
        datasource_info.insert("type".to_string(), json!("datasource"));
        datasource_info.insert("name".to_string(), json!(datasource_name.clone()));
        
        // 获取数据源下的所有数据库
        let database_names = manager.get_database_names(&datasource_name);
        let mut databases_info = HashMap::new();
        
        debug!("数据源 {} 包含 {} 个数据库", datasource_name, database_names.len());
        
        // 遍历每个数据库
        for database_name in database_names {
            debug!("处理数据库: {}.{}", datasource_name, database_name);
            
            let mut database_info = HashMap::new();
            database_info.insert("type".to_string(), json!("database"));
            database_info.insert("name".to_string(), json!(database_name.clone()));
            
            // 获取数据库下的所有表信息
            let table_info_map = metadata::get_datasource_table_name_list(&datasource_name, &database_name);
            let mut tables_info = HashMap::new();
            
            debug!("数据库 {}.{} 包含 {} 个表", datasource_name, database_name, table_info_map.len());
            
            // 遍历每个表
            for (table_name, table_comment) in table_info_map {
                let mut table_info = HashMap::new();
                table_info.insert("type".to_string(), json!("table"));
                table_info.insert("name".to_string(), json!(table_name.clone()));
                table_info.insert("comment".to_string(), json!(table_comment));
                
                tables_info.insert(table_name, json!(table_info));
            }
            
            database_info.insert("tables".to_string(), json!(tables_info));
            databases_info.insert(database_name, json!(database_info));
        }
        
        datasource_info.insert("databases".to_string(), json!(databases_info));
        result_data.insert(datasource_name, json!(datasource_info));
    }
    
    info!("数据源信息查询完成，返回 {} 个数据源的信息", result_data.len());
    
    rpc_result.data = Some(result_data);
    rpc_result
}

/// 处理数据库表详情查询请求
/// 
/// 返回指定数据库下所有表的详细信息，包括表结构、字段信息等
/// 
/// # 参数
/// * `manager` - 数据源管理器
/// * `body_map` - 包含请求参数的HashMap，必需参数：
///   - `datasource`: 数据源名称
///   - `database`: 数据库名称
/// 
/// # 返回值
/// 返回树形结构的JSON数据：
/// ```json
/// {
///   "database_info": {
///     "type": "database",
///     "datasource": "datasource_name",
///     "name": "database_name",
///     "tables": {
///       "table_name": {
///         "type": "table",
///         "name": "table_name",
///         "comment": "table_comment",
///         "columns": {
///           "column_name": {
///             "type": "column",
///             "name": "column_name",
///             "field": "column_name",
///             "type_name": "varchar",
///             "null": "YES",
///             "default": null,
///             "comment": "column_comment",
///             "key": "",
///             "extra": ""
///           }
///         }
///       }
///     }
///   }
/// }
/// ```
pub async fn handle_database_tables(
    manager: &DataSourceManager,
    body_map: HashMap<String, Value>,
) -> RpcResult<HashMap<String, Value>> {
    info!("开始处理数据库表详情查询请求");
    debug!("请求参数: {:?}", body_map);
    
    let mut rpc_result = RpcResult {
        code: HttpCode::Ok,
        msg: None,
        data: None,
    };
    
    // 获取必需的参数
    let datasource_name = match body_map.get("datasource").and_then(|v| v.as_str()) {
        Some(name) => name,
        None => {
            rpc_result.code = HttpCode::BadRequest;
            rpc_result.msg = Some("缺少必需参数: datasource".to_string());
            return rpc_result;
        }
    };
    
    let database_name = match body_map.get("database").and_then(|v| v.as_str()) {
        Some(name) => name,
        None => {
            rpc_result.code = HttpCode::BadRequest;
            rpc_result.msg = Some("缺少必需参数: database".to_string());
            return rpc_result;
        }
    };
    
    info!("查询数据库表详情: {}.{}", datasource_name, database_name);
    
    // 验证数据源是否存在
    let all_datasources = manager.get_datasource_names();
    if !all_datasources.contains(&datasource_name.to_string()) {
        rpc_result.code = HttpCode::NotFound;
        rpc_result.msg = Some(format!("数据源 '{}' 不存在", datasource_name));
        return rpc_result;
    }
    
    // 验证数据库是否存在
    let databases = manager.get_database_names(datasource_name);
    if !databases.contains(&database_name.to_string()) {
        rpc_result.code = HttpCode::NotFound;
        rpc_result.msg = Some(format!("数据库 '{}.{}' 不存在", datasource_name, database_name));
        return rpc_result;
    }
    
    // 构建数据库信息
    let mut database_info = HashMap::new();
    database_info.insert("type".to_string(), json!("database"));
    database_info.insert("datasource".to_string(), json!(datasource_name));
    database_info.insert("name".to_string(), json!(database_name));
    
    // 获取数据库下的所有表信息
    let table_info_map = metadata::get_datasource_table_name_list(datasource_name, database_name);
    let mut tables_info = HashMap::new();
    
    info!("数据库 {}.{} 包含 {} 个表", datasource_name, database_name, table_info_map.len());
    
    // 遍历每个表，获取详细信息
    for (table_name, table_comment) in table_info_map {
        debug!("处理表: {}.{}.{}", datasource_name, database_name, table_name);
        
        let mut table_info = HashMap::new();
        table_info.insert("type".to_string(), json!("table"));
        table_info.insert("name".to_string(), json!(table_name.clone()));
        table_info.insert("comment".to_string(), json!(table_comment));
        
        // 获取表的元数据信息
        if let Some(table_meta) = metadata::get_datasource_table(datasource_name, database_name, &table_name) {
            let mut columns_info = HashMap::new();
            
            debug!("表 {} 包含 {} 个字段", table_name, table_meta.columns.len());
            
            // 遍历表的每个字段
            for (_column_name, column) in &table_meta.columns {
                let mut column_info = HashMap::new();
                column_info.insert("type".to_string(), json!("column"));
                column_info.insert("name".to_string(), json!(column.field.clone()));
                column_info.insert("field".to_string(), json!(column.field.clone()));
                column_info.insert("type_name".to_string(), json!(column.type_name.clone()));
                column_info.insert("null".to_string(), json!(column.null.clone()));
                column_info.insert("default".to_string(), 
                    if let Some(ref default) = column.default {
                        json!(default)
                    } else {
                        json!(null)
                    }
                );
                column_info.insert("comment".to_string(), 
                    if let Some(ref comment) = column.comment {
                        json!(comment)
                    } else {
                        json!("")
                    }
                );
                column_info.insert("key".to_string(), 
                    if let Some(ref key) = column.key {
                        json!(key)
                    } else {
                        json!("")
                    }
                );
                column_info.insert("extra".to_string(), 
                    if let Some(ref extra) = column.extra {
                        json!(extra)
                    } else {
                        json!("")
                    }
                );
                
                columns_info.insert(column.field.clone(), json!(column_info));
            }
            
            table_info.insert("columns".to_string(), json!(columns_info));
        } else {
            warn!("无法获取表 {}.{}.{} 的元数据信息", datasource_name, database_name, table_name);
            table_info.insert("columns".to_string(), json!({}));
        }
        
        tables_info.insert(table_name, json!(table_info));
    }
    
    database_info.insert("tables".to_string(), json!(tables_info));
    
    let mut result_data = HashMap::new();
    result_data.insert("database_info".to_string(), json!(database_info));
    
    info!("数据库表详情查询完成，返回 {} 个表的详细信息", tables_info.len());
    
    rpc_result.data = Some(result_data);
    rpc_result
}