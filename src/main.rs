//! Panda Base 主程序入口
//! 
//! 负责应用的启动和初始化

pub mod app;

use log::info;
use crate::app::{
    startup::AppStartup,
    server::start_server,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    env_logger::init();
    
    info!("🚀 启动 Panda Base 应用...");
    
    // 初始化应用
    let datasource_manager = match AppStartup::initialize(None).await {
        Ok(manager) => {
            info!("✅ 应用初始化成功");
            manager
        },
        Err(e) => {
            eprintln!("❌ 应用初始化失败: {}", e);
            return Err(e.into());
        }
    };
    
    // 打印统计信息
    let stats = AppStartup::get_statistics(&datasource_manager);
    info!("📊 应用统计信息:\n{}", stats);
    
    // 启动HTTP服务器
    let port = 8080;
    info!("🌐 启动HTTP服务器，端口: {}", port);
    
    start_server(datasource_manager, port).await?;
    
    Ok(())
}
