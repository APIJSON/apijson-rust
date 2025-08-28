// 定义全局 Snowflake 变量
use lazy_static::lazy_static;
lazy_static! {
    static ref GLOBAL_SNOWFLAKE: std::sync::Mutex<rustflake::Snowflake> = std::sync::Mutex::new(rustflake::Snowflake::new(1420070400000, 1, 1));
}

/// 生成一个全局唯一的ID (基于Snowflake算法)
///
/// # 返回
/// 返回一个u64类型的唯一ID
pub fn get_next_id() -> i64 {
    GLOBAL_SNOWFLAKE.lock().unwrap().generate()
}