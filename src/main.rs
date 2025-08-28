pub mod app;

use log::info;
use app::startup::AppStartup;

#[tokio::main]
async fn main() {
    // 初始化日志
    init_tk_log();
    
    info!("Panda Base 应用启动中...");
    
    // 初始化多数据源系统
    match AppStartup::initialize(None).await {
        Ok(manager) => {
            info!("应用启动成功！");
            
            // 打印统计信息
            let stats = AppStartup::get_statistics(&manager);
            info!("系统统计信息:\n{}", stats);
            
            // 这里可以添加其他应用逻辑
            info!("应用正在运行...");
            
            // 保持应用运行
            tokio::signal::ctrl_c().await.expect("Failed to listen for ctrl+c");
            info!("收到退出信号，应用正在关闭...");
        },
        Err(e) => {
            eprintln!("应用启动失败: {}", e);
            std::process::exit(1);
        }
    }
}


//初始化tk log
fn init_tk_log() {
    tklog::LOG.set_console(true)
        .set_level(tklog::LEVEL::Info)
        .set_formatter("{time} | {level} | {file} | {message}\n")
        .uselog();  // 启用官方log库
}
