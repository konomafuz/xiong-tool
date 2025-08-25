# 链上资金流分析工具使用指南

## 快速开始

### 1. 启动应用
```bash
cd C:\Users\xiong\AIcode\build-your-onchain-agent-main\okx_pnl_tool
streamlit run streamlit_app.py
```

应用将在浏览器中打开：http://localhost:8502

### 2. 准备数据

上传包含以下字段的CSV文件：
- `from_address`: 发送地址
- `to_address`: 接收地址  
- `value`: 转账金额
- `tx_hash`: 交易哈希
- `timestamp`: 时间戳（可选）
- `type`: 交易类型，如 'TRANSFER'（可选）

### 3. 功能模块

#### 3.1 资金来源分析
- 对已知地址进行标注（交易所、OTC、巨鲸等）
- 统计各来源的资金流入量
- 生成来源分布图表

**使用步骤：**
1. 在"已知地址标注"文本框中输入格式：
   ```
   0x1234...,Binance交易所
   0x5678...,OTC商户
   0xabcd...,巨鲸地址
   ```
2. 查看标注后的数据
3. 分析来源聚合统计

#### 3.2 资金流向Sankey图
- 可视化资金在地址间的流动路径
- 支持交互式HTML导出
- 自动筛选主要流向

**使用步骤：**
1. 点击"生成Sankey图"按钮
2. 在图中查看资金流向关系
3. 鼠标悬停查看详细数值

#### 3.3 地址集群识别
- **转账关系聚类**：基于直接转账关系识别地址集群
- **Co-spend分析**：识别在同一交易中共同作为输入的地址

**使用步骤：**
1. 点击"执行转账关系聚类"查看基于转账的集群
2. 点击"执行co-spend分析"查看共花费模式
3. 查看聚类分布统计

## 示例数据格式

```csv
from_address,to_address,value,tx_hash,type
0x1234567890abcdef,0xabcdef1234567890,1000,0xhash1,TRANSFER
0xabcdef1234567890,0x9876543210fedcba,800,0xhash2,TRANSFER
0x1234567890abcdef,0x9876543210fedcba,500,0xhash3,TRANSFER
```

## 应用场景

1. **反洗钱调查**：追踪可疑资金来源和去向
2. **巨鲸分析**：识别大户的资金操作模式
3. **交易所分析**：了解用户资金流入流出
4. **项目方分析**：监控代币分发和持仓变化
5. **套利检测**：发现地址间的套利行为

## 高级功能

### 在Flask应用中集成
可以将这些分析功能集成到现有的Flask应用中：

```python
from modules.source_analysis import SourceAnalyzer
from modules.sankey_viz import plot_sankey
from modules.cluster_addresses import co_spend_cluster_analysis

# 在Flask路由中使用
@app.route('/analyze')
def analyze():
    # 资金来源分析
    analyzer = SourceAnalyzer(tx_df)
    labeled_data = analyzer.label_sources(known_sources)
    
    # 生成Sankey图
    fig = plot_sankey(tx_df, output_html='static/sankey.html')
    
    # Co-spend分析
    cluster_df, stats = co_spend_cluster_analysis(events_df, holders_df)
    
    return render_template('analysis_results.html', ...)
```

### 批量处理
对于大量数据，可以直接调用Python模块：

```python
import pandas as pd
from modules.source_analysis import SourceAnalyzer

# 读取数据
df = pd.read_csv('transactions.csv')

# 分析
analyzer = SourceAnalyzer(df)
results = analyzer.get_top_sources(10)
print(results)
```

## 注意事项

1. **数据隐私**：确保敏感地址信息的安全性
2. **性能考虑**：大数据集可能需要较长处理时间
3. **网络图复杂度**：过多节点时可视化可能较慢
4. **数据质量**：确保输入数据的完整性和准确性

## 故障排除

### 常见问题
1. **缺少字段错误**：检查CSV文件是否包含必需字段
2. **内存不足**：对于大数据集，考虑分批处理
3. **可视化异常**：检查plotly是否正确安装

### 依赖安装
如遇到依赖问题，运行：
```bash
pip install streamlit plotly networkx matplotlib pandas numpy
```
