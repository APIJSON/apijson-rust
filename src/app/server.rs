//! HTTP服务器模块
//! 
//! 提供RESTful API接口用于测试查询功能

use std::sync::Arc;
use axum::{
    extract::Path,
    Json,
    routing::{get, post},
    Router,
};
use serde_json::{json, Value, Map};
use std::collections::HashMap;
use tower_http::cors::CorsLayer;
use crate::app::{
    datasource::manager::DataSourceManager,
    handler::{
        get::handle_get,
        head::handle_head,
        post::handle_post,
        put::handle_put,
        delete::handle_delete,
    },
};
use crate::app::common::rpc::HttpCode;
use crate::app::handler::util::parser::new_err_result;

/// 创建HTTP服务器路由
pub fn create_router(datasource_manager: Arc<DataSourceManager>) -> Router {
    let mgr = datasource_manager.clone();
    let curd_handler = move |Path(method): Path<String>, Json(request_data): Json<Map<String, Value>>| {
        let mgr = mgr.clone();
        async move {
            let method_norm = method.strip_suffix(".json").unwrap_or(&method);
            let rpc_result = match method_norm {
                "head" => {
                    handle_head(mgr.clone(), request_data, None).await
                }
                "get" => {
                    handle_get(&mgr, request_data).await
                }
                "put" => {
                    handle_put(mgr.clone(), request_data, None).await
                }
                "post" => {
                    handle_post(mgr.clone(), request_data, None).await
                }
                "delete" => {
                    handle_delete(mgr.clone(), request_data).await
                }
                _ => {
                    return Json(json!(new_err_result(HttpCode::MethodNotAllowed, format!("unknown method: {}", method_norm).as_str())));
                }
            };
            Json(json!(rpc_result))
        }
    };

    Router::new()
        // 动态派发CRUD操作，形如：/get.json、/post.json、/put.json、/delete.json、/head.json
        // 这里使用 `/:method` 捕获，包括带有 .json 后缀的情形（例如 get.json），在处理器中去掉后缀
        .route("/:method", post(curd_handler))
        .route("/health", get(health_check))
        .layer(CorsLayer::permissive())
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