use std::collections::HashMap;
use log::{info, warn, error, debug};
use crate::app::common::rpc::{RpcResult, HttpCode};
use crate::app::common::id::get_next_id;
use crate::app::datasource::manager::DataSourceManager;
use crate::app::datasource::dialect::SqlBuilder;
use crate::app::handler::util::parser::{DatabaseTargetParser, DatabaseTargetDefaults};
use std::sync::Arc;

/// 处理数据插入请求
/// 
/// # 参数
/// * `datasource_manager` - 数据源管理器
/// * `body_map` - 包含插入请求的数据映射，key为表名，value为要插入的数据
/// * `defaults` - 默认值提供者
/// 
/// # 返回值
/// 返回 JSON 格式的处理结果：
/// * 成功：返回插入后的完整记录数据
/// * 失败：`{"code": 400, "msg": "错误信息"}`
pub async fn handle_post(
    datasource_manager: Arc<DataSourceManager>,
    body_map: HashMap<String, serde_json::Value>,
    defaults: Option<DatabaseTargetDefaults>,
) -> RpcResult<HashMap<String, serde_json::Value>> {
    info!("开始处理POST请求");
    debug!("请求数据: {:?}", body_map);
    
    let mut rpc_result = RpcResult {
        code: HttpCode::Ok,
        msg: None,
        data: None,
    };

    // 解析请求体
    let parse_result = DatabaseTargetParser::parse_request_body(body_map);
    
    // 如果有解析错误，直接返回
    if !parse_result.errors.is_empty() {
        warn!("请求解析失败: {:?}", parse_result.errors);
        rpc_result.code = HttpCode::BadRequest;
        rpc_result.msg = Some(format!("解析错误: {}", parse_result.errors.join("; ")));
        return rpc_result;
    }
    
    info!("请求解析成功，共解析到 {} 个数据项", parse_result.items.len());

    let mut result_payload = HashMap::new();
    let defaults = defaults.unwrap_or_else(|| {
        DatabaseTargetDefaults::new(None, None, Some("public".to_string()))
    });

    // 处理每个解析后的数据项
    for item in parse_result.items {
        // 应用默认值
        let target = defaults.apply_defaults(item.target);
        debug!("应用默认值后的目标: datasource={:?}, database={:?}, schema={:?}, table={}", 
               target.datasource, target.database, target.schema, target.table);
        
        // 验证目标完整性
        if let Err(err) = defaults.validate_target(&target) {
            error!("目标验证失败: {}, table: {}", err, target.table);
            rpc_result.code = HttpCode::BadRequest;
            result_payload.insert(
                target.table.clone(),
                serde_json::json!(format!("目标验证失败: {}", err))
            );
            continue;
        }

        // 执行插入操作
        let datasource_name = target.datasource.as_ref().unwrap();
        let database_name = target.database.as_ref().unwrap();
        let schema_name = target.schema.as_ref().unwrap();
        
        info!("执行插入操作: {}.{}.{}.{}", datasource_name, database_name, schema_name, target.table);
        
        match insert_one(
            datasource_manager.clone(),
            datasource_name,
            database_name,
            schema_name,
            &target.table,
            &item.data,
        ).await {
            Ok(id) => {
                info!("插入操作成功: {}.{}.{}.{}, id={}", datasource_name, database_name, schema_name, target.table, id);
                result_payload.insert(target.table.clone(), serde_json::json!(id));
            },
            Err(err) => {
                error!("插入操作失败: {}.{}.{}.{}, 错误: {}", datasource_name, database_name, schema_name, target.table, err);
                rpc_result.code = HttpCode::BadRequest;
                result_payload.insert(target.table.clone(), serde_json::Value::String(err));
            }
        }
    }

    if !result_payload.is_empty() {
        rpc_result.data = Some(result_payload);
    }
    rpc_result
}

/// 执行单条记录的插入操作
/// 
/// # 参数
/// * `datasource_manager` - 数据源管理器
/// * `datasource_name` - 数据源名称
/// * `database_name` - 数据库名称
/// * `schema` - 模式名称
/// * `table` - 表名
/// * `data` - 要插入的数据
/// 
/// # 返回值
/// * `Ok(i64)` - 成功时返回插入记录的 ID
/// * `Err(String)` - 失败时返回错误信息
async fn insert_one(
    datasource_manager: Arc<DataSourceManager>,
    datasource_name: &str,
    database_name: &str,
    schema: &str,
    table: &str,
    data: &HashMap<String, serde_json::Value>,
) -> Result<i64, String> {
    debug!("开始执行单条插入操作: {}.{}.{}.{}", datasource_name, database_name, schema, table);
    debug!("插入数据: {:?}", data);
    
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

    // 准备插入数据（添加自动生成的ID）
    let mut insert_data = data.clone();
    let data_id = get_next_id();
    debug!("生成记录ID: {}", data_id);
    insert_data.insert("id".to_string(), serde_json::Value::Number(serde_json::Number::from(data_id)));

    // 构建INSERT SQL
    let sql = sql_builder.build_insert(schema, table, &insert_data);
    debug!("构建的INSERT SQL: {}", sql);

    // 执行插入操作
    debug!("执行INSERT SQL: {}", sql);
    match connection.update(&sql).await {
        Ok(cnt) => {
            if cnt > 0 {
                info!("插入操作成功: 影响了 {} 行，记录ID: {}", cnt, data_id);
                Ok(data_id)
            } else {
                let error_msg = "插入操作未影响任何行".to_string();
                warn!("{}, SQL: {}", error_msg, sql);
                Err(error_msg)
            }
        }
        Err(e) => {
            let error_msg = format!("插入操作失败: {}", e);
            error!("{}, SQL: {}", error_msg, sql);
            Err(error_msg)
        }
    }
}