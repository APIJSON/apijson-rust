/// 将字节向量编码为Base64字符串
///
/// # 参数
/// * `bytes` - 需要编码的字节向量
///
/// # 返回
/// 返回Base64编码后的字符串
use base64::{Engine as _, engine::general_purpose};
pub fn base64_encode(bytes: Vec<u8>) -> String {
    general_purpose::STANDARD.encode(bytes)
}