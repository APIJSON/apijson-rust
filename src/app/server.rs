//! HTTP服务器模块
//! 
//! 提供RESTful API接口用于测试查询功能

use std::sync::Arc;
use axum::{
    extract::{Path, Query, State},
    response::Json,
    routing::get,
    Router,
};
use serde_json::{json, Value};
use std::collections::HashMap;
use tower_http::cors::CorsLayer;
use crate::app::{
    datasource::manager::DataSourceManager,
    handler::get::handle_get,
};

/// 应用状态
#[derive(Clone)]
pub struct AppState {
    pub datasource_manager: Arc<DataSourceManager>,
}

/// 创建HTTP服务器路由
pub fn create_router(datasource_manager: Arc<DataSourceManager>) -> Router {
    let app_state = AppState {
        datasource_manager,
    };

    Router::new()
        // .route("/api/v1/:datasource/:database/:table", get(query_handler))
        .route("/health", get(health_check))
        .layer(CorsLayer::permissive())
        .with_state(app_state)
}

/// 查询处理器
async fn query_handler(
    Path((datasource, database, table)): Path<(String, String, String)>,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<AppState>,
) -> Json<Value> {
    // 构建查询参数
    let mut query_params = HashMap::new();
    
    // 添加基本参数
    query_params.insert("datasource".to_string(), json!(datasource));
    query_params.insert("database".to_string(), json!(database));
    query_params.insert("table".to_string(), json!(table));
    
    // 添加查询参数
    for (key, value) in params {
        query_params.insert(key, json!(value));
    }
    
    // 调用查询处理器
    let result = handle_get(
        &state.datasource_manager,
        query_params,
    ).await;
    
    // 转换结果
    Json(json!({
        "code": result.code as u16,
        "data": result.data,
        "msg": result.msg
    }))
}

/// 健康检查
async fn health_check() -> Json<Value> {
    Json(json!({
        "status": "ok",
        "message": "Panda Base is running"
    }))
}

/// 启动HTTP服务器
pub async fn start_server(
    datasource_manager: Arc<DataSourceManager>,
    port: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    let app = create_router(datasource_manager);
    
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    
    println!("🚀 服务器启动成功！");
    println!("📍 监听地址: http://localhost:{}", port);
    println!("🔍 API示例: http://localhost:{}/api/v1/ds_mysql/panda_db/user", port);
    println!("💚 健康检查: http://localhost:{}/health", port);
    
    axum::serve(listener, app).await?;
    
    Ok(())
}