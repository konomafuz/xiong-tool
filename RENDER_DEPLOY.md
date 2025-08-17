# Render PostgreSQL 部署指南

## 🚀 **部署步骤**

### 1. **Render环境设置**

在Render控制台设置以下环境变量：

```bash
# 数据库连接（已设置）
DATABASE_URL=postgresql://xiong_tool_sql_user:jwokxZ31cxSckuksdwzrYzwN349r8ie2mDf@dpg-d2fh98be5dus73aobvd0-a/xiong_tool_sql

# 应用配置
FLASK_ENV=production
FLASK_DEBUG=False

# 数据库配置（针对免费版优化）
DB_POOL_SIZE=3
DB_MAX_OVERFLOW=5
DB_POOL_TIMEOUT=30
DB_POOL_RECYCLE=3600

# 功能开关
ENABLE_DATABASE_CACHE=True
CACHE_EXPIRE_HOURS=1

# 编码设置
PYTHONIOENCODING=utf-8
LANG=C.UTF-8
```

### 2. **Render服务配置**

```yaml
# 构建命令（可选）
Build Command: python deploy.py

# 启动命令
Start Command: python app.py

# 健康检查
Health Check Path: /system_status

# 端口
Port: 5000
```

### 3. **部署流程**

1. **代码推送到GitHub**
2. **Render自动构建**：
   - 安装依赖：`pip install -r requirements.txt`
   - 运行部署脚本：`python deploy.py`
3. **启动应用**：`python app.py`
4. **健康检查**：访问 `/system_status`

## 🔧 **本地测试**

### 快速测试配置：
```bash
cd okx_pnl_tool
python test_config_only.py    # 测试配置加载
python deploy.py              # 测试部署脚本
python app.py                 # 启动应用
```

### 数据库连接测试：
```bash
python test_db_connection.py  # 完整数据库测试
```

## 📊 **监控端点**

部署后可用的监控端点：

- **系统状态**: `GET /system_status`
  ```json
  {
    "database_status": "connected",
    "connection_pool": {
      "pool_size": 3,
      "checked_out": 1,
      "overflow": 0
    },
    "memory_usage_mb": 45.2,
    "status": "healthy"
  }
  ```

- **数据库测试**: `GET /db_test`
- **连接池清理**: `POST /cleanup_connections`

## 🛠️ **故障排除**

### 1. **数据库连接失败**
```bash
# 检查连接池状态
curl https://your-app.onrender.com/system_status

# 手动清理连接池
curl -X POST https://your-app.onrender.com/cleanup_connections
```

### 2. **编码问题**
- 确保设置了 `PYTHONIOENCODING=utf-8`
- 检查 `LANG=C.UTF-8` 环境变量

### 3. **连接池耗尽**
- 检查 `pool_size + max_overflow ≤ 15`
- 监控 `checked_out` 连接数
- 必要时调用连接池清理

## 📋 **优化配置**

### 免费版PostgreSQL限制：
- **最大连接数**: 20
- **推荐配置**: pool_size=3, max_overflow=5
- **实际使用**: ≤8个连接，安全余量充足

### 性能优化：
- ✅ **pool_pre_ping**: 避免idle连接断开
- ✅ **scoped_session**: 长任务独立会话
- ✅ **自动清理**: 请求结束立即释放连接
- ✅ **缓存机制**: 1小时数据缓存，减少API调用

## 🎯 **功能状态**

| 功能 | 本地状态 | 生产状态 |
|------|----------|----------|
| 应用启动 | ✅ 正常 | ✅ 预期正常 |
| 配置加载 | ✅ 通过 | ✅ 预期正常 |
| 数据库连接 | ⚠️ 网络限制 | ✅ 预期正常 |
| 数据缓存 | ⚠️ 跳过 | ✅ 预期工作 |
| API功能 | ✅ 正常 | ✅ 预期正常 |

## 💡 **注意事项**

1. **本地测试**：数据库连接失败是正常的，应用会自动降级
2. **生产环境**：Render网络内部可正常连接PostgreSQL
3. **连接监控**：定期检查 `/system_status` 确保连接池健康
4. **缓存策略**：首次访问会调用API，后续1小时内使用缓存

## 🚀 **部署检查清单**

- [ ] 环境变量已设置
- [ ] 代码已推送到GitHub
- [ ] Render服务配置正确
- [ ] 构建命令设置为 `python deploy.py`
- [ ] 启动命令设置为 `python app.py`
- [ ] 健康检查路径为 `/system_status`
- [ ] 部署后访问 `/db_test` 验证数据库连接

部署完成后，你的应用将具备：
- 🔄 **智能缓存**：优先使用数据库，API作后备
- 🛡️ **连接保护**：自动管理连接池，防止耗尽
- 📊 **实时监控**：连接状态、内存使用一目了然
- ⚡ **高性能**：针对Render免费版优化的配置
