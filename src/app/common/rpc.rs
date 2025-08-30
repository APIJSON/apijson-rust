/// 标准RPC响应结构体
#[derive(Debug, Clone, serde::Serialize)]
pub struct RpcResult<T: serde::Serialize> {
    /// HTTP状态码，与http::StatusCode一致
    pub code: HttpCode,
    /// 人类可读的消息
    pub msg: Option<String>,
    /// 响应数据负载
    pub data: Option<T>,
}

/// HTTP状态码枚举
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpCode {
    /// 200 OK
    Ok = 200,
    /// 201 Created
    Created = 201,
    /// 204 No Content
    NoContent = 204,
    /// 400 Bad Request
    BadRequest = 400,
    /// 401 Unauthorized
    Unauthorized = 401,
    /// 403 Forbidden
    Forbidden = 403,
    /// 404 Not Found
    NotFound = 404,
    /// 405 Method Not Allowed
    MethodNotAllowed = 405,
    /// 409 Conflict
    Conflict = 409,
    /// 422 Unprocessable Entity
    UnprocessableEntity = 422,
    /// 500 Internal Server Error
    InternalServerError = 500,
    /// 502 Bad Gateway
    BadGateway = 502,
    /// 503 Service Unavailable
    ServiceUnavailable = 503,
    /// 504 Gateway Timeout
    GatewayTimeout = 504,
}

impl serde::Serialize for HttpCode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_u16(*self as u16)
    }
}

impl<'de> serde::Deserialize<'de> for HttpCode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let code = u16::deserialize(deserializer)?;
        match code {
            200 => Ok(HttpCode::Ok),
            201 => Ok(HttpCode::Created),
            204 => Ok(HttpCode::NoContent),
            400 => Ok(HttpCode::BadRequest),
            401 => Ok(HttpCode::Unauthorized),
            403 => Ok(HttpCode::Forbidden),
            404 => Ok(HttpCode::NotFound),
            405 => Ok(HttpCode::MethodNotAllowed),
            409 => Ok(HttpCode::Conflict),
            422 => Ok(HttpCode::UnprocessableEntity),
            500 => Ok(HttpCode::InternalServerError),
            502 => Ok(HttpCode::BadGateway),
            503 => Ok(HttpCode::ServiceUnavailable),
            504 => Ok(HttpCode::GatewayTimeout),
            _ => Err(serde::de::Error::custom(format!("Unknown HTTP status code: {}", code))),
        }
    }
}



