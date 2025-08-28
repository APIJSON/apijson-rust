use std::collections::HashMap;
use serde_json::Value;
use log::{debug, info, warn, error};

/// 数据库四元素信息
#[derive(Debug, Clone)]
pub struct DatabaseTarget {
    /// 数据源名称
    pub datasource: Option<String>,
    /// 数据库名称
    pub database: Option<String>,
    /// 模式名称
    pub schema: Option<String>,
    /// 表名
    pub table: String,
}

/// 解析后的数据项
#[derive(Debug, Clone)]
pub struct ParsedDataItem {
    /// 数据库目标信息
    pub target: DatabaseTarget,
    /// 清理后的数据（移除了元数据字段）
    pub data: HashMap<String, Value>,
}

/// 解析结果
#[derive(Debug, Clone)]
pub struct ParseResult {
    /// 解析后的数据项列表
    pub items: Vec<ParsedDataItem>,
    /// 解析错误信息
    pub errors: Vec<String>,
}

/// 四元素解析器
pub struct DatabaseTargetParser;

impl DatabaseTargetParser {
    /// 解析请求体中的数据库目标信息
    /// 
    /// # 参数
    /// * `body_map` - 请求体数据映射
    /// 
    /// # 返回值
    /// 返回解析结果，包含所有解析后的数据项和错误信息
    pub fn parse_request_body(body_map: HashMap<String, Value>) -> ParseResult {
        info!("开始解析请求体，包含 {} 个表项", body_map.len());
        debug!("请求体内容: {:?}", body_map);
        
        let mut items = Vec::new();
        let mut errors = Vec::new();

        for (table_key, param) in body_map {
            debug!("解析表项: {}", table_key);
            match Self::parse_table_data(&table_key, param) {
                Ok(mut parsed_items) => {
                    info!("成功解析表 '{}', 获得 {} 个数据项", table_key, parsed_items.len());
                    items.append(&mut parsed_items);
                }
                Err(err) => {
                    error!("解析表 '{}' 失败: {}", table_key, err);
                    errors.push(format!("解析表 '{}' 时出错: {}", table_key, err));
                }
            }
        }

        info!("解析完成，共获得 {} 个数据项，{} 个错误", items.len(), errors.len());
        if !errors.is_empty() {
            warn!("解析过程中发现错误: {:?}", errors);
        }
        
        ParseResult { items, errors }
    }

    /// 解析单个表的数据
    /// 
    /// # 参数
    /// * `table_key` - 表键名（可能包含[]后缀表示数组）
    /// * `param` - 参数值
    /// 
    /// # 返回值
    /// 返回解析后的数据项列表
    fn parse_table_data(table_key: &str, param: Value) -> Result<Vec<ParsedDataItem>, String> {
        let mut items = Vec::new();
        
        // 检查是否为数组表（以[]结尾）
        let (table_name, is_array) = if table_key.ends_with("[]") {
            (table_key.trim_end_matches("[]"), true)
        } else {
            (table_key, false)
        };
        
        debug!("解析表数据: table_name={}, is_array={}", table_name, is_array);

        if is_array {
            // 处理数组数据
            debug!("处理数组数据");
            match param.as_array() {
                Some(array) => {
                    debug!("数组包含 {} 个元素", array.len());
                    for (index, item) in array.iter().enumerate() {
                        match item.as_object() {
                            Some(obj) => {
                                debug!("解析数组第 {} 项", index);
                                let parsed_item = Self::parse_single_item(table_name, obj.clone(), true)?;
                                items.push(parsed_item);
                            }
                            None => {
                                error!("数组第{}项不是有效的对象: {:?}", index, item);
                                return Err(format!("数组第{}项不是有效的对象", index));
                            }
                        }
                    }
                }
                None => {
                    error!("期望数组类型，但收到: {:?}", param);
                    return Err("期望数组类型，但收到其他类型".to_string());
                }
            }
        } else {
            // 处理单个对象数据
            debug!("处理单个对象数据");
            match param.as_object() {
                Some(obj) => {
                    let parsed_item = Self::parse_single_item(table_name, obj.clone(), false)?;
                    items.push(parsed_item);
                }
                None => {
                    error!("期望对象类型，但收到: {:?}", param);
                    return Err("期望对象类型，但收到其他类型".to_string());
                }
            }
        }

        Ok(items)
    }

    /// 解析单个数据项
    /// 
    /// # 参数
    /// * `table_name` - 表名
    /// * `obj` - 数据对象
    /// * `is_array` - 是否为数组项
    /// 
    /// # 返回值
    /// 返回解析后的数据项
    fn parse_single_item(
        table_name: &str,
        obj: serde_json::Map<String, Value>,
        is_array: bool,
    ) -> Result<ParsedDataItem, String> {
        debug!("解析单个数据项: table={}, is_array={}", table_name, is_array);
        
        let mut data = HashMap::new();
        let mut datasource = None;
        let mut database = None;
        let mut schema = None;

        // 解析数据，提取元数据字段
        for (key, value) in obj {
            match key.as_str() {
                "@datasource" => {
                    datasource = value.as_str().map(|s| s.to_string());
                    debug!("提取元数据字段 @datasource: {:?}", datasource);
                }
                "@database" => {
                    database = value.as_str().map(|s| s.to_string());
                    debug!("提取元数据字段 @database: {:?}", database);
                }
                "@schema" => {
                    schema = value.as_str().map(|s| s.to_string());
                    debug!("提取元数据字段 @schema: {:?}", schema);
                }
                _ => {
                    // 普通数据字段
                    data.insert(key, value);
                }
            }
        }

        let target = DatabaseTarget {
            datasource,
            database,
            schema,
            table: table_name.to_string(),
        };

        debug!("解析完成，数据字段数量: {}, 目标信息: {:?}", data.len(), target);

        Ok(ParsedDataItem {
            target,
            data,
        })
    }

}

/// 数据库目标解析器的默认值提供者
pub struct DatabaseTargetDefaults {
    /// 默认数据源名称
    pub default_datasource: Option<String>,
    /// 默认数据库名称
    pub default_database: Option<String>,
    /// 默认模式名称
    pub default_schema: Option<String>,
}

impl DatabaseTargetDefaults {
    /// 创建新的默认值提供者
    pub fn new(
        default_datasource: Option<String>,
        default_database: Option<String>,
        default_schema: Option<String>,
    ) -> Self {
        Self {
            default_datasource,
            default_database,
            default_schema,
        }
    }

    /// 应用默认值到数据库目标
    /// 
    /// # 参数
    /// * `target` - 数据库目标
    /// 
    /// # 返回值
    /// 返回应用默认值后的数据库目标
    pub fn apply_defaults(&self, mut target: DatabaseTarget) -> DatabaseTarget {
        if target.datasource.is_none() {
            target.datasource = self.default_datasource.clone();
        }
        if target.database.is_none() {
            target.database = self.default_database.clone();
        }
        if target.schema.is_none() {
            target.schema = self.default_schema.clone();
        }
        target
    }

    /// 验证数据库目标是否完整
    /// 
    /// # 参数
    /// * `target` - 数据库目标
    /// 
    /// # 返回值
    /// 如果目标完整返回Ok(())，否则返回错误信息
    pub fn validate_target(&self, target: &DatabaseTarget) -> Result<(), String> {
        debug!("验证数据库目标完整性: {:?}", target);
        
        let mut missing = Vec::new();

        if target.datasource.is_none() {
            missing.push("datasource");
        }
        if target.database.is_none() {
            missing.push("database");
        }
        if target.schema.is_none() {
            missing.push("schema");
        }

        if missing.is_empty() {
            debug!("数据库目标验证通过");
            Ok(())
        } else {
            warn!("数据库目标验证失败，缺少: {:?}", missing);
            Err(format!("缺少必要的数据库目标信息: {}", missing.join(", ")))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_single_object() {
        let mut body_map = HashMap::new();
        body_map.insert(
            "users".to_string(),
            json!({
                "@datasource": "main",
                "@database": "app_db",
                "@schema": "public",
                "name": "John",
                "age": 25
            }),
        );

        let result = DatabaseTargetParser::parse_request_body(body_map);
        assert_eq!(result.errors.len(), 0);
        assert_eq!(result.items.len(), 1);

        let item = &result.items[0];
        assert_eq!(item.target.datasource, Some("main".to_string()));
        assert_eq!(item.target.database, Some("app_db".to_string()));
        assert_eq!(item.target.schema, Some("public".to_string()));
        assert_eq!(item.target.table, "users");
        assert_eq!(item.data.len(), 2);
    }

    #[test]
    fn test_parse_array_data() {
        let mut body_map = HashMap::new();
        body_map.insert(
            "moment[]".to_string(),
            json!([
                {
                    "@datasource": "main",
                    "content": "moment1",
                    "userId": 1
                },
                {
                    "content": "moment2",
                    "userId": 2
                }
            ]),
        );

        let result = DatabaseTargetParser::parse_request_body(body_map);
        assert_eq!(result.errors.len(), 0);
        assert_eq!(result.items.len(), 2);

        let item1 = &result.items[0];
        assert_eq!(item1.target.datasource, Some("main".to_string()));
        assert_eq!(item1.target.table, "moment");

        let item2 = &result.items[1];
        assert_eq!(item2.target.datasource, None);
        assert_eq!(item2.target.table, "moment");
    }

    #[test]
    fn test_apply_defaults() {
        let defaults = DatabaseTargetDefaults::new(
            Some("default_ds".to_string()),
            Some("default_db".to_string()),
            Some("default_schema".to_string()),
        );

        let target = DatabaseTarget {
            datasource: None,
            database: Some("custom_db".to_string()),
            schema: None,
            table: "users".to_string(),
        };

        let result = defaults.apply_defaults(target);
        assert_eq!(result.datasource, Some("default_ds".to_string()));
        assert_eq!(result.database, Some("custom_db".to_string())); // 保持自定义值
        assert_eq!(result.schema, Some("default_schema".to_string()));
    }
}