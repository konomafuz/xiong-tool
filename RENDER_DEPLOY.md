# Render PostgreSQL 部署指南（免费版）

## 🆓 **免费版限制说明**

Render免费版有以下限制：
- ❌ **不支持Pre-Deploy Command**
- ❌ **不支持Build Command**
- ✅ **支持Start Command**
- ✅ **支持环境变量设置**

## 🚀 **免费版部署步骤**

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

### 2. **免费版服务配置**

**重要：免费版只需要设置以下两项**

```bash
# 启动命令（唯一必需的配置）
Start Command: python app.py

# 健康检查（可选）
Health Check Path: /system_status
```

**不要设置：**
- ❌ Build Command（免费版不支持）
- ❌ Pre-Deploy Command（免费版不支持）

### 3. **免费版部署流程**

1. **代码推送到GitHub**
2. **Render自动部署**：
   - 自动安装requirements.txt中的依赖
   - 直接运行：`python app.py`
3. **应用自动初始化**：
   - 应用启动时自动检测并初始化数据库
   - 如果数据库连接失败，自动降级为API模式
4. **健康检查**：访问 `/system_status`

## 🎯 **Render免费版配置界面**

### Web Service配置：

```
Name: your-app-name
Environment: Python
Region: Oregon (US West)
Branch: main
Root Directory: okx_pnl_tool
Runtime: Python 3

Build Command: (留空 - 免费版不支持)
Start Command: python app.py
```

**可选的启动方式：**
- `python app.py` - 直接启动（推荐）
- `python start.py` - 使用启动脚本（更多日志）

### 🔧 **本地测试**

免费版部署前测试：
```bash
cd okx_pnl_tool

# 测试配置
python test_config_only.py

# 测试应用启动
python app.py

# 测试启动脚本（可选）
python start.py
```

### 环境变量配置：

在Environment Variables部分添加：

```
DATABASE_URL = postgresql://xiong_tool_sql_user:jwokxZ31cxSckuksdwzrYzwN349r8ie2mDf@dpg-d2fh98be5dus73aobvd0-a/xiong_tool_sql
FLASK_ENV = production
FLASK_DEBUG = False
DB_POOL_SIZE = 3
DB_MAX_OVERFLOW = 5
ENABLE_DATABASE_CACHE = True
PYTHONIOENCODING = utf-8
LANG = C.UTF-8
```

## 🚀 **免费版部署检查清单**

部署前检查：
- [ ] 代码已推送到GitHub
- [ ] Render连接到正确的GitHub仓库
- [ ] Root Directory设置为 `okx_pnl_tool`
- [ ] Start Command设置为 `python app.py`
- [ ] **Build Command留空**（重要！）
- [ ] 环境变量已全部设置
- [ ] requirements.txt在正确位置

部署后检查：
- [ ] 服务状态显示为 "Live"
- [ ] 访问应用URL无错误
- [ ] 访问 `/system_status` 返回JSON
- [ ] 访问 `/db_test` 验证数据库连接

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

## 🛠️ **免费版故障排除**

### 1. **部署失败：Build Command不支持**
```
错误: Build commands are not available on the free plan
解决: 删除Build Command配置，留空即可
```

### 2. **应用启动失败：端口问题**
```
错误: Port 5000 is not available
解决: 确保app.py使用PORT环境变量
代码: port = int(os.getenv('PORT', 5000))
```

### 3. **数据库连接超时**
```
错误: Connection timeout
解决: 这是正常的，应用会自动降级为API模式
检查: 访问/system_status查看状态
```

### 4. **依赖安装失败**
```
错误: Package installation failed
解决: 检查requirements.txt格式
确保: 所有包都支持Python 3.x
```

### 5. **应用无响应（Cold Start）**
```
现象: 免费版应用会休眠，首次访问较慢
解决: 这是正常的，等待30-60秒
建议: 可以设置定时ping保持活跃
```

## 🎯 **免费版限制说明**

| 功能 | 免费版 | 说明 |
|------|--------|------|
| Build Command | ❌ | 不支持预构建命令 |
| Pre-Deploy | ❌ | 不支持部署前脚本 |
| 持续运行 | ❌ | 15分钟无访问会休眠 |
| 自定义域名 | ❌ | 只能使用.onrender.com域名 |
| Start Command | ✅ | 支持启动命令 |
| 环境变量 | ✅ | 支持设置环境变量 |
| PostgreSQL | ✅ | 免费PostgreSQL数据库 |
| HTTPS | ✅ | 自动HTTPS证书 |

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

## ✅ **免费版部署成功验证**

部署完成后，按顺序检查以下项目：

### 1. 基础检查
- [ ] Render控制台显示服务状态为 "Live"
- [ ] 访问应用URL无500错误
- [ ] 页面能正常加载（可能需要等待冷启动）

### 2. 功能检查
- [ ] 访问 `https://your-app.onrender.com/` - 首页加载
- [ ] 访问 `https://your-app.onrender.com/system_status` - 返回JSON状态
- [ ] 访问 `https://your-app.onrender.com/db_test` - 数据库连接测试

### 3. 数据库检查
```bash
# 检查数据库连接状态
curl https://your-app.onrender.com/db_test

# 期望返回：
{
  "status": "success",
  "message": "数据库连接正常",
  "database_name": "xiong_tool_sql",
  "user_name": "xiong_tool_sql_user"
}
```

### 4. API功能检查
- [ ] 访问 `/top_earners` 页面
- [ ] 尝试查询一个代币地址
- [ ] 检查是否返回数据或使用缓存

🎉 **如果以上检查都通过，恭喜你成功部署到Render免费版！**

---

### 💡 **免费版使用建议**

1. **保持活跃**：每15分钟访问一次应用避免休眠
2. **监控状态**：定期检查 `/system_status`
3. **数据缓存**：充分利用1小时数据缓存减少API调用
4. **升级时机**：如需24/7运行或自定义域名，考虑付费版
