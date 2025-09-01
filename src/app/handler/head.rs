use log::{info, warn, error, debug};
use crate::app::common::rpc::{HttpCode};
use crate::app::datasource::manager::DataSourceManager;
use crate::app::datasource::dialect::SqlBuilder;
use crate::app::handler::util::parser::{DatabaseTargetParser, DatabaseTargetDefaults, new_err_result, new_ok_result, KEY_CODE, KEY_MSG};
use std::sync::Arc;
use serde_json::{Value, Map, json};

/// 处理HEAD请求的异步方法，主要用于检查表是否存在和记录计数
///
/// # 参数
/// * `datasource_manager` - 数据源管理器
/// * `body_map` - 包含请求参数的HashMap，键为表名(String)，值为查询条件(Value)
/// * `defaults` - 默认值提供者
///
/// # 返回值
/// 返回serde_json::Value类型的JSON响应数据，包含操作结果
///
/// # 错误处理
/// - 如果表不存在，返回错误信息
/// - 如果参数格式错误，返回错误信息
/// - 如果查询失败，返回错误信息
pub async fn handle_head(
    datasource_manager: Arc<DataSourceManager>,
    body_map: Map<String, Value>,
    defaults: Option<DatabaseTargetDefaults>,
) -> Map<String, Value> {
    info!("开始处理HEAD请求");
    debug!("请求数据: {:?}", body_map);

    let mut result_payload: Map<String, Value> = Map::new();

    // 解析请求体
    let parse_result = DatabaseTargetParser::parse_request_body(body_map);
    
    // 如果有解析错误，直接返回
    if !parse_result.errors.is_empty() {
        warn!("请求解析失败: {:?}", parse_result.errors);
        result_payload = new_err_result(HttpCode::BadRequest, parse_result.errors.join("; ").as_str());
        return result_payload;
    }
    
    info!("请求解析成功，共解析到 {} 个数据项", parse_result.items.len());

    let defaults = defaults.unwrap_or_else(|| {
        DatabaseTargetDefaults::new(None, None, Some("public".to_string()))
    });

    // 处理每个解析的数据项
    for item in parse_result.items {
        let target = defaults.apply_defaults(item.target);
        
        debug!("处理目标: datasource={}, database={}, schema={}, table={}", 
               target.datasource.as_deref().unwrap_or(""), 
               target.database.as_deref().unwrap_or(""), 
               target.schema.as_deref().unwrap_or(""), 
               target.table);

        // 执行查询
        match count_one(
            datasource_manager.clone(),
            target.datasource.as_deref().unwrap_or("default"),
            target.database.as_deref().unwrap_or("default"),
            target.schema.as_deref().unwrap_or("public"),
            &target.table,
            &item.data,
        ).await {
            Ok(result) => {
                result_payload.insert(target.table.clone(), result);
            },
            Err(err) => {
                error!("查询失败: {}", err);
                result_payload = new_err_result(HttpCode::BadRequest, err.as_str());
                break;
            }
        }
    }

    return new_ok_result(result_payload);
}

async fn count_one(
    datasource_manager: Arc<DataSourceManager>,
    datasource_name: &str,
    database_name: &str,
    schema: &str,
    table: &str,
    conditions: &Map<String, Value>,
) -> Result<Value, String> {
    debug!("开始处理查询: {}.{}, 条件数量: {}", schema, table, conditions.len());
    
    // 获取数据库连接
    let connection = datasource_manager
        .get_connection(datasource_name, database_name)
        .ok_or_else(|| format!("无法获取数据库连接: datasource={}, database={}", datasource_name, database_name))?;
    
    // 获取数据源配置
    let datasource_config = datasource_manager
        .get_datasource_config(datasource_name)
        .ok_or_else(|| format!("无法获取数据源配置: {}", datasource_name))?;
    
    // 创建SQL构建器
    let sql_builder = SqlBuilder::new(&datasource_config.kind);
    
    // 检查是否有@column参数
    let column_spec = conditions.get("@column")
        .and_then(|v| v.as_str())
        .unwrap_or("*");
    
    // 过滤掉@开头的特殊参数，只保留查询条件
    let query_conditions: Map<String, Value> = conditions
        .iter()
        .filter(|(key, _)| !key.starts_with('@'))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    
    // 如果有@column参数且不是"*"，执行SELECT查询
    if column_spec != "*" || !query_conditions.is_empty() {
        // 构建字段列表
        let columns = sql_builder.build_columns(column_spec);
        let columns_str = columns.join(", ");
        
        // 构建WHERE条件
        let where_clause = sql_builder.build_where_conditions(&query_conditions);
        
        // 构建SELECT查询SQL
        let sql = if where_clause.is_empty() {
            format!("SELECT {} FROM {}.{}", columns_str, schema, table)
        } else {
            format!("SELECT {} FROM {}.{}{}", columns_str, schema, table, where_clause)
        };
        
        debug!("执行SELECT查询: {}", sql);
        
        // 执行查询并返回结果
        match connection.query_list(&sql, vec![]).await {
            Ok(rows) => {
                debug!("查询结果行数: {}", rows.len());
                Ok(json!(rows))
            },
            Err(e) => {
                error!("SELECT查询失败: {}", e);
                Err(e.to_string())
            }
        }
    } else {
        // 没有@column参数，执行COUNT查询
        let where_clause = sql_builder.build_where_conditions(&query_conditions);
        
        let sql = if where_clause.is_empty() {
            format!("SELECT COUNT(*) FROM {}.{}", schema, table)
        } else {
            format!("SELECT COUNT(*) FROM {}.{}{}", schema, table, where_clause)
        };
        
        debug!("执行COUNT查询: {}", sql);
        
        // 执行查询
        match connection.count(&sql, vec![]).await {
            Ok(count) => {
                debug!("统计结果: {}", count);
                Ok(json!(count))
            },
            Err(e) => {
                error!("COUNT查询失败: {}", e);
                Err(e.to_string())
            }
        }
    }
}