# Panda Base - 多数据源管理系统

一个优雅、高性能的 Rust 多数据源管理系统，支持 MySQL 和 PostgreSQL 数据库的统一管理和操作。

## 🚀 特性

- **多数据源支持**: 同时管理多个 MySQL 和 PostgreSQL 数据源
- **统一接口**: 提供统一的数据库操作接口，屏蔽底层数据库差异
- **智能缓存**: 内置高效的元数据缓存系统，支持数据源、数据库、表的关联查询
- **配置驱动**: 基于 YAML 配置文件的声明式配置管理
- **异步支持**: 基于 Tokio 的异步运行时，支持高并发操作
- **类型安全**: 利用 Rust 的类型系统确保运行时安全
- **完整测试**: 提供全面的单元测试和集成测试

## 📋 系统要求

- Rust 1.70+
- MySQL 5.7+ 或 PostgreSQL 12+
- Tokio 异步运行时

## 🛠️ 安装和配置

### 1. 克隆项目

```bash
git clone <repository-url>
cd panda-base
```

### 2. 安装依赖

```bash
cargo build
```

### 3. 配置数据源

编辑 `application.yaml` 文件：

```yaml
datasources:
  # MySQL 数据源
  - name: ds_mysql
    kind: mysql
    username: root
    password: "123456"
    url: "mysql://root:123456@localhost:3306"
    database: ["db1", "db2", "db3"]
    default: true
    
  # PostgreSQL 数据源
  - name: ds_pg
    kind: postgres
    username: postgres
    password: "123456"
    url: "postgres://postgres:123456@localhost:5432"
    database: ["db4", "db5", "db6"]
    default: false
```

### 4. 运行应用

```bash
cargo run
```

## 🏗️ 架构设计

### 核心模块

```
src/app/datasource/
├── mod.rs          # 模块定义和数据结构
├── config.rs       # 配置结构体和验证
├── loader.rs       # YAML 配置加载器
├── manager.rs      # 数据源管理器
├── cache.rs        # 元数据缓存系统
├── core.rs         # MySQL 数据库连接和操作
├── postgres.rs     # PostgreSQL 数据库连接和操作
├── codec.rs        # 编码工具
└── tests.rs        # 集成测试
```

### 设计原则

1. **高内聚低耦合**: 每个模块职责单一，模块间依赖最小化
2. **配置驱动**: 通过配置文件管理数据源，无需修改代码
3. **统一抽象**: 为不同数据库提供统一的操作接口
4. **缓存优化**: 智能缓存元数据，减少数据库查询
5. **错误处理**: 完善的错误处理和恢复机制

## 📚 使用指南

### 基本用法

```rust
use panda_base::app::{
    startup::AppStartup,
    datasource::manager::DataSourceManager,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化应用
    let manager = AppStartup::initialize(None).await?;
    
    // 获取默认数据源连接
    let connection = manager.get_default_connection("database_name")?;
    
    // 执行查询
    let result = connection.query_one("SELECT * FROM users WHERE id = ?", &[1]).await?;
    
    Ok(())
}
```

### 配置管理

```rust
use panda_base::app::datasource::{
    loader::ConfigLoader,
    config::DataSourcesConfig,
};

// 从文件加载配置
let config = ConfigLoader::load_from_file("application.yaml")?;

// 从字符串加载配置
let yaml_content = r#"
datasources:
  - name: test_db
    kind: mysql
    username: root
    password: "123456"
    url: "mysql://localhost:3306"
    database: ["testdb"]
    default: true
"#;
let config = ConfigLoader::load_from_string(yaml_content)?;
```

### 数据源操作

```rust
// 获取所有数据源名称
let datasource_names = manager.get_datasource_names();

// 获取指定数据源的数据库列表
let databases = manager.get_database_names("ds_mysql")?;

// 获取特定数据库连接
let connection = manager.get_connection("ds_mysql", "database_name")?;
```

### 缓存查询

```rust
use panda_base::app::datasource::cache::*;

// 获取数据源的数据库列表
let databases = get_datasource_database_names("ds_mysql");

// 获取数据库大小
let size = get_datasource_database_size("ds_mysql", "database_name");

// 获取表列表
let tables = get_datasource_table_name_list("ds_mysql", "database_name");

// 检查表是否存在
let exists = is_datasource_table_exists("ds_mysql", "database_name", "table_name");
```

## 🧪 测试

### 运行所有测试

```bash
cargo test
```

### 运行特定测试模块

```bash
# 配置测试
cargo test config

# 缓存测试
cargo test cache

# 集成测试
cargo test integration_tests

# 性能测试
cargo test performance_tests
```

### 测试覆盖率

```bash
cargo tarpaulin --out Html
```

## 📊 性能特性

- **连接池**: 自动管理数据库连接池，支持高并发访问
- **智能缓存**: 元数据缓存命中率 > 95%
- **异步操作**: 基于 Tokio 的异步 I/O，支持数千并发连接
- **内存优化**: 使用 `Arc` 和 `RwLock` 实现高效的内存共享

## 🔧 配置选项

### 数据源配置

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `name` | String | ✅ | 数据源唯一名称 |
| `kind` | Enum | ✅ | 数据库类型 (mysql/postgres) |
| `username` | String | ✅ | 数据库用户名 |
| `password` | String | ✅ | 数据库密码 |
| `url` | String | ✅ | 数据库连接 URL |
| `database` | Array | ✅ | 数据库名称列表 |
| `default` | Boolean | ✅ | 是否为默认数据源 |

### URL 格式

- **MySQL**: `mysql://[username:password@]host:port[/database]`
- **PostgreSQL**: `postgres://[username:password@]host:port[/database]`

## 🚨 注意事项

1. **唯一性约束**:
   - 数据源名称必须唯一
   - 必须有且仅有一个默认数据源

2. **安全性**:
   - 配置文件中的密码建议使用环境变量
   - 生产环境中应使用 SSL/TLS 连接

3. **性能优化**:
   - 合理配置连接池大小
   - 定期清理缓存数据
   - 监控数据库连接状态

## 🤝 贡献指南

1. Fork 项目
2. 创建特性分支 (`git checkout -b feature/amazing-feature`)
3. 提交更改 (`git commit -m 'Add some amazing feature'`)
4. 推送到分支 (`git push origin feature/amazing-feature`)
5. 开启 Pull Request

### 代码规范

- 遵循 Rust 官方代码风格
- 添加适当的文档注释
- 确保所有测试通过
- 新功能需要添加相应测试

## 📄 许可证

本项目采用 MIT 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

## 🆘 支持

如果您遇到问题或有建议，请：

1. 查看 [Issues](../../issues) 页面
2. 创建新的 Issue
3. 联系维护者

## 🗺️ 路线图

- [ ] 支持更多数据库类型 (SQLite, Oracle)
- [ ] 添加数据库迁移工具
- [ ] 实现读写分离
- [ ] 添加监控和指标收集
- [ ] 支持分布式缓存
- [ ] 添加 Web 管理界面

---

**Panda Base** - 让多数据源管理变得简单而优雅 🐼