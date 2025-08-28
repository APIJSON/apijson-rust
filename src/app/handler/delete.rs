use std::collections::HashMap;
use log::{debug, info, warn, error};
use crate::app::common::rpc::{RpcResult, HttpCode};
use crate::app::datasource::manager::{DataSourceManager, DatabaseConnection};
use crate::app::datasource::dialect::SqlBuilder;
use crate::app::handler::util::parser::{DatabaseTargetParser, DatabaseTargetDefaults, DatabaseTarget};
use std::sync::Arc;
use serde_json::Value;

/// 处理删除数据的请求
///
/// # 参数
/// * `datasource_manager` - 数据源管理器
/// * `body_map` - 包含删除请求的数据映射，key为表名，value为删除条件
///
pub async fn handle_delete(
    datasource_manager: Arc<DataSourceManager>,
    body_map: HashMap<String, Value>,
) -> RpcResult<HashMap<String, Value>> {
    info!("开始处理删除请求，包含 {} 个表项", body_map.len());
    debug!("删除请求体: {:?}", body_map);
    
    let mut rpc_result = RpcResult {
        code: HttpCode::Ok,
        msg: None,
        data: None,
    };

    // 解析请求体
    let parse_result = DatabaseTargetParser::parse_request_body(body_map);
    if !parse_result.errors.is_empty() {
        error!("解析请求体失败: {:?}", parse_result.errors);
        rpc_result.code = HttpCode::BadRequest;
        rpc_result.msg = Some(format!("请求解析失败: {}", parse_result.errors.join(", ")));
        return rpc_result;
    }
    
    info!("成功解析 {} 个数据项", parse_result.items.len());

    // 应用默认值
    let defaults = DatabaseTargetDefaults::new(
        Some("default".to_string()),
        Some("default_db".to_string()),
        Some("public".to_string()),
    );

    let mut result_payload = HashMap::<String, Value>::new();

    // 处理每个数据项
    for item in parse_result.items {
        let target = defaults.apply_defaults(item.target.clone());
        
        // 验证目标完整性
        if let Err(err) = defaults.validate_target(&target) {
            warn!("目标验证失败: {}", err);
            rpc_result.code = HttpCode::BadRequest;
            result_payload.insert(target.table.clone(), Value::String(err));
            continue;
        }

        // 执行删除操作
        match delete_one(&datasource_manager, &target, &item.data).await {
            Ok(affected_rows) => {
                info!("删除成功，表: {}, 影响行数: {}", target.table, affected_rows);
                result_payload.insert(target.table, Value::Number(affected_rows.into()));
            }
            Err(err) => {
                error!("删除失败，表: {}, 错误: {}", target.table, err);
                rpc_result.code = HttpCode::InternalServerError;
                result_payload.insert(target.table, Value::String(err));
            }
        }
    }

    if !result_payload.is_empty() {
        rpc_result.data = Some(result_payload);
    }
    
    info!("删除请求处理完成");
    rpc_result
}

/// 执行数据删除操作
///
/// # 参数
/// * `datasource_manager` - 数据源管理器
/// * `target` - 数据库目标信息
/// * `data` - 删除条件，支持两种格式：
///   * `{"id": number}` - 删除单条记录
///   * `{"id{}": [number]}` - 批量删除多条记录
///
/// # 返回值
/// * `Ok(u64)` - 成功时返回受影响的行数
/// * `Err(String)` - 失败时返回错误信息
async fn delete_one(
    datasource_manager: &DataSourceManager,
    target: &DatabaseTarget,
    data: &HashMap<String, Value>,
) -> Result<u64, String> {
    let datasource_name = target.datasource.as_ref().ok_or("数据源名称为空")?;
    let database_name = target.database.as_ref().ok_or("数据库名称为空")?;
    
    debug!("开始删除操作，表: {}.{}.{}", datasource_name, target.schema.as_deref().unwrap_or(""), target.table);
    
    // 获取数据库连接
    let connection = datasource_manager
        .get_connection(datasource_name, database_name)
        .ok_or_else(|| format!("无法获取数据库连接: datasource={}, database={}", datasource_name, database_name))?;
    
    // 获取数据源配置以确定SQL方言
    let datasource_config = datasource_manager
        .get_datasource_config(datasource_name)
        .ok_or_else(|| format!("找不到数据源配置: {}", datasource_name))?;
    
    // 创建SQL构建器
    let sql_builder = SqlBuilder::new(&datasource_config.kind);
    debug!("使用SQL方言: {:?}", datasource_config.kind);
    
    if let Some(id_value) = data.get("id") {
        // 处理单个 ID 删除
        let id = id_value.as_i64().ok_or("ID必须是数字")?;
        debug!("删除单条记录，ID: {}", id);
        
        let schema = target.schema.as_deref().unwrap_or("");
        let sql = sql_builder.build_delete(schema, &target.table, id);
        
        debug!("执行单个删除SQL: {}", sql);
        execute_delete(&connection, &sql).await
    } else if let Some(id_array) = data.get("id{}") {
        // 处理批量 ID 删除
        let ids = id_array.as_array().ok_or("id{}必须是数组")?;
        let id_list = ids.iter()
            .map(|v| v.as_i64().ok_or("数组中的ID必须是数字"))
            .collect::<Result<Vec<_>, _>>()?;
        
        if id_list.is_empty() {
            return Err("删除ID列表不能为空".to_string());
        }
        
        debug!("批量删除记录，IDs: {:?}", id_list);
         let id_list_str = id_list.iter().map(|id| id.to_string()).collect::<Vec<_>>().join(", ");
         
         // 对于批量删除，我们使用IN子句一次性删除
         let schema = target.schema.as_deref().unwrap_or("");
         let where_clause = format!("id IN ({})", id_list_str);
         let sql = sql_builder.build_delete_with_where(schema, &target.table, &where_clause);
         
         debug!("执行批量删除SQL: {}", sql);
         execute_delete(&connection, &sql).await
    } else {
        // 没有提供有效的 ID
        Err(format!("删除操作必须包含 'id' 或 'id{{}}' 字段，表: {}, 数据: {:?}", target.table, data))
    }
}

/// 执行实际的删除 SQL 操作
///
/// # 参数
/// * `connection` - 数据库连接实例
/// * `sql` - 要执行的删除 SQL 语句
///
/// # 返回值
/// * `Ok(u64)` - 成功时返回受影响的行数
/// * `Err(String)` - 失败时返回错误信息
async fn execute_delete(connection: &DatabaseConnection, sql: &str) -> Result<u64, String> {
    debug!("执行删除SQL: {}", sql);
    match connection.update(sql).await {
        Ok(affected_rows) => {
            info!("删除操作成功: 影响了 {} 行", affected_rows);
            Ok(affected_rows)
        }
        Err(e) => {
            let error_msg = format!("删除操作失败: {}", e);
            error!("{}, SQL: {}", error_msg, sql);
            Err(error_msg)
        }
    }
}