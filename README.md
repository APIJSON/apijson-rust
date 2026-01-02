# APIJSON-Rust - 腾讯 APIJSON Rust 版 & 多数据源管理系统

一个优雅、高性能的 Rust 多数据源管理系统，支持 MySQL 和 PostgreSQL 数据库的统一管理和操作。

## 🚀 特性

- **多数据源支持**: 同时管理多个 MySQL 和 PostgreSQL 数据源
- **统一接口**: 提供统一的数据库操作接口，屏蔽底层数据库差异
- **智能缓存**: 内置高效的元数据缓存系统，支持数据源、数据库、表的关联查询
- **配置驱动**: 基于 YAML 配置文件的声明式配置管理
- **异步支持**: 基于 Tokio 的异步运行时，支持高并发操作
- **类型安全**: 利用 Rust 的类型系统确保运行时安全

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

2. **性能优化**:
   - 合理配置连接池大小
   - 定期清理缓存数据
   - 监控数据库连接状态

## 🗺️ 路线图

- [ ] 支持更多数据库类型 (SQLite, Oracle)
- [ ] 添加数据库迁移工具
- [ ] 实现读写分离
- [ ] 添加监控和指标收集
- [ ] 支持分布式缓存
- [ ] 添加 Web 管理界面

---

**Panda Base** - 让多数据源管理变得简单而优雅 🐼

## 测试环境运行
初始化数据库
```bash
docker compose up -d 
```
