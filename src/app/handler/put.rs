use std::collections::HashMap;
use std::sync::Arc;
use log::{info, warn, error, debug};
use crate::app::datasource::manager::DataSourceManager;
use crate::app::datasource::dialect::SqlBuilder;
use crate::app::handler::util::parser::{DatabaseTargetParser, DatabaseTargetDefaults};
use crate::app::common::rpc::{RpcResult, HttpCode};

/// 处理数据更新请求
/// 
/// # 参数
/// * `datasource_manager` - 数据源管理器
/// * `body_map` - 包含更新请求的数据映射
/// * `defaults` - 默认的数据库目标信息
/// 
/// # 返回值
/// 返回 JSON 格式的处理结果：
/// * 成功：返回更新后的记录ID
/// * 失败：`{"code": 400, "msg": "错误信息"}`
/// 
/// # 示例
/// ```json
/// {
///   "datasource": "mysql_main",
///   "database": "test_db", 
///   "schema": "public",
///   "table": "user",
///   "data": {
///     "id": 1,
///     "name": "新名字",
///     "age": 25
///   }
/// }
/// ```
pub async fn handle_put(
    datasource_manager: Arc<DataSourceManager>,
    body_map: HashMap<String, serde_json::Value>,
    defaults: Option<DatabaseTargetDefaults>,
) -> RpcResult<HashMap<String, serde_json::Value>> {
    info!("开始处理PUT请求");
    debug!("请求数据: {:?}", body_map);
    
    let mut rpc_result = RpcResult::<HashMap<String, serde_json::Value>>{ 
        code: HttpCode::Ok, 
        msg: None, 
        data: None 
    };

    // 解析请求体
    let parse_result = DatabaseTargetParser::parse_request_body(body_map);
    
    // 检查解析错误
    if !parse_result.errors.is_empty() {
        warn!("请求解析失败: {:?}", parse_result.errors);
        rpc_result.code = HttpCode::BadRequest;
        rpc_result.msg = Some(format!("请求解析失败: {}", parse_result.errors.join(", ")));
        return rpc_result;
    }
    
    info!("请求解析成功，共解析到 {} 个数据项", parse_result.items.len());

    // 应用默认值
    let items_with_defaults = if let Some(ref defaults) = defaults {
        parse_result.items.into_iter().map(|mut item| {
            item.target = defaults.apply_defaults(item.target);
            item
        }).collect()
    } else {
        parse_result.items
    };

    let mut result_payload = HashMap::new();
    
    for item in items_with_defaults {
        // 验证目标完整性
        if item.target.datasource.is_none() || item.target.database.is_none() || item.target.schema.is_none() {
            error!("缺少必要的数据库目标信息: datasource={:?}, database={:?}, schema={:?}", 
                   item.target.datasource, item.target.database, item.target.schema);
            rpc_result.code = HttpCode::BadRequest;
            rpc_result.msg = Some("缺少必要的数据库目标信息".to_string());
            return rpc_result;
        }
        
        let target = item.target;

        // 执行更新操作
        let datasource_name = target.datasource.as_ref().unwrap();
        let database_name = target.database.as_ref().unwrap();
        let schema_name = target.schema.as_ref().unwrap();
        
        info!("执行更新操作: {}.{}.{}.{}", datasource_name, database_name, schema_name, target.table);
        
        match update_one(
            datasource_manager.clone(),
            datasource_name,
            database_name,
            schema_name,
            &target.table,
            &item.data,
        ).await {
            Ok(id) => {
                info!("更新操作成功: {}.{}.{}.{}, id={}", datasource_name, database_name, schema_name, target.table, id);
                result_payload.insert(target.table.clone(), serde_json::json!(id));
            },
            Err(err) => {
                error!("更新操作失败: {}.{}.{}.{}, 错误: {}", datasource_name, database_name, schema_name, target.table, err);
                rpc_result.code = HttpCode::BadRequest;
                result_payload.insert(target.table.clone(), serde_json::json!(err));
            }
        }
    }
    
    if !result_payload.is_empty() {
        rpc_result.data = Some(result_payload);
    }
    rpc_result
}

/// 执行单条记录的更新操作
/// 
/// # 参数
/// * `datasource_manager` - 数据源管理器
/// * `datasource_name` - 数据源名称
/// * `database_name` - 数据库名称
/// * `schema` - 模式名称
/// * `table` - 表名
/// * `data` - 要更新的数据，必须包含 id 字段
/// 
/// # 返回值
/// * `Ok(i64)` - 成功时返回更新记录的 ID
/// * `Err(String)` - 失败时返回错误信息
async fn update_one(
    datasource_manager: Arc<DataSourceManager>,
    datasource_name: &str,
    database_name: &str,
    schema: &str,
    table: &str,
    data: &HashMap<String, serde_json::Value>,
) -> Result<i64, String> {
    debug!("开始执行单条更新操作: {}.{}.{}.{}", datasource_name, database_name, schema, table);
    debug!("更新数据: {:?}", data);
    
    // 检查是否包含 id 字段
    let id = match data.get("id") {
        Some(id_value) => {
            if !id_value.is_number() {
                let error_msg = format!("'id' 字段类型不是数字, table: {}, data: {:?}", table, data);
                warn!("{}", error_msg);
                return Err(error_msg);
            }
            id_value.as_i64().unwrap()
        },
        None => {
            let error_msg = format!("更新数据必须包含 'id' 字段, table: {}, data: {:?}", table, data);
            warn!("{}", error_msg);
            return Err(error_msg);
        }
    };
    
    debug!("提取到记录ID: {}", id);

    // 获取数据库连接
    debug!("获取数据库连接: {}.{}", datasource_name, database_name);
    let connection = datasource_manager
        .get_connection(datasource_name, database_name)
        .ok_or_else(|| {
            let error_msg = format!(
                "无法获取数据库连接: datasource={}, database={}",
                datasource_name, database_name
            );
            error!("{}", error_msg);
            error_msg
        })?;

    // 获取数据源配置以确定SQL方言
    let datasource_config = datasource_manager
        .get_datasource_config(datasource_name)
        .ok_or_else(|| {
            let error_msg = format!("找不到数据源配置: {}", datasource_name);
            error!("{}", error_msg);
            error_msg
        })?;

    // 创建SQL构建器
    let sql_builder = SqlBuilder::new(&datasource_config.kind);
    debug!("使用SQL方言: {:?}", datasource_config.kind);

    // 构建UPDATE SQL
    let sql = sql_builder.build_update(schema, table, data, id);
    debug!("构建的UPDATE SQL: {}", sql);

    // 执行更新操作
    debug!("执行UPDATE SQL: {}", sql);
    match connection.update(&sql).await {
        Ok(cnt) => {
            if cnt > 0 {
                info!("更新操作成功: 影响了 {} 行，记录ID: {}", cnt, id);
                Ok(id)
            } else {
                let error_msg = "更新操作未影响任何行".to_string();
                warn!("{}, SQL: {}", error_msg, sql);
                Err(error_msg)
            }
        }
        Err(e) => {
            let error_msg = format!("更新操作失败: {}", e);
            error!("{}, SQL: {}", error_msg, sql);
            Err(error_msg)
        }
    }
}