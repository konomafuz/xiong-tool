import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
import matplotlib
matplotlib.use('Agg')  # 使用非交互式后端
import matplotlib.pyplot as plt

# 设置matplotlib支持中文
plt.rcParams['font.sans-serif'] = ['Microsoft YaHei', 'SimHei', 'Arial Unicode MS', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False


def build_sankey_data(tx_df, from_col='from_address', to_col='to_address', value_col='value', 
                     address_labels=None, top_n=20):
    """
    构建Sankey图所需数据
    :param tx_df: 包含转账记录的DataFrame
    :param from_col: 源地址列名
    :param to_col: 目标地址列名
    :param value_col: 金额列名
    :param address_labels: {address: label}，可选
    :param top_n: 只展示前N大流量的地址
    :return: nodes, links
    """
    # 检查列是否存在
    if from_col not in tx_df.columns:
        available_cols = [col for col in tx_df.columns if 'from' in col.lower()]
        if available_cols:
            from_col = available_cols[0]
        else:
            raise ValueError(f"未找到源地址列")
    
    if to_col not in tx_df.columns:
        available_cols = [col for col in tx_df.columns if 'to' in col.lower()]
        if available_cols:
            to_col = available_cols[0]
        else:
            raise ValueError(f"未找到目标地址列")
    
    if value_col not in tx_df.columns:
        # 寻找可能的数值列
        available_cols = [col for col in tx_df.columns if any(keyword in col.lower() 
                         for keyword in ['value', 'amount', 'balance', 'usd'])]
        if available_cols:
            value_col = available_cols[0]
        else:
            # 创建交易计数列
            tx_df = tx_df.copy()
            tx_df['transaction_count'] = 1
            value_col = 'transaction_count'
    
    # 确保数值列是数值类型
    tx_df = tx_df.copy()
    try:
        tx_df[value_col] = pd.to_numeric(tx_df[value_col], errors='coerce')
        tx_df = tx_df.dropna(subset=[value_col])
    except:
        tx_df['transaction_count'] = 1
        value_col = 'transaction_count'
    
    # 统计流量最大的地址
    top_from = tx_df.groupby(from_col)[value_col].sum().nlargest(top_n).index
    top_to = tx_df.groupby(to_col)[value_col].sum().nlargest(top_n).index
    top_addrs = set(top_from) | set(top_to)
    
    # 只保留主要流向
    df = tx_df[tx_df[from_col].isin(top_addrs) & tx_df[to_col].isin(top_addrs)].copy()
    
    # 聚合相同地址对的交易
    df = df.groupby([from_col, to_col])[value_col].sum().reset_index()
    
    # 为了更好的布局，按地址类型分组
    source_addrs = set(df[from_col].unique())
    target_addrs = set(df[to_col].unique())
    
    # 区分三类地址：仅源地址、仅目标地址、既是源又是目标
    only_source = source_addrs - target_addrs
    only_target = target_addrs - source_addrs
    both = source_addrs & target_addrs
    
    # 按类别排序地址，便于布局
    all_addrs = list(only_source) + list(both) + list(only_target)
    
    # 生成标签
    if address_labels:
        labels = []
        for addr in all_addrs:
            if addr in address_labels:
                label = address_labels[addr]
            else:
                label = addr[:8] + '...'
            labels.append(label)
    else:
        labels = [addr[:8] + '...' for addr in all_addrs]
    
    addr2idx = {addr: i for i, addr in enumerate(all_addrs)}
    
    # 构建links
    links = {
        'source': df[from_col].map(addr2idx).tolist(),
        'target': df[to_col].map(addr2idx).tolist(),
        'value': df[value_col].tolist(),
    }
    
    return labels, links


def plot_sankey_standard(df, from_col='from_address', to_col='to_address', value_col='value', 
                        title='资金流向图', output_path='sankey.html', interactive=True,
                        address_labels=None, top_n=20):
    """
    绘制标准样式的Sankey图（较粗的线条）
    """
    try:
        labels, links = build_sankey_data(df, from_col, to_col, value_col, address_labels, top_n)
        
        # 为节点生成不同颜色
        import random
        random.seed(42)  # 固定随机种子，确保颜色一致
        node_colors = []
        color_palette = [
            'rgba(31, 119, 180, 0.8)',   # 蓝色
            'rgba(255, 127, 14, 0.8)',   # 橙色
            'rgba(44, 160, 44, 0.8)',    # 绿色
            'rgba(214, 39, 40, 0.8)',    # 红色
            'rgba(148, 103, 189, 0.8)',  # 紫色
            'rgba(140, 86, 75, 0.8)',    # 棕色
            'rgba(227, 119, 194, 0.8)',  # 粉色
            'rgba(127, 127, 127, 0.8)',  # 灰色
            'rgba(188, 189, 34, 0.8)',   # 橄榄色
            'rgba(23, 190, 207, 0.8)',   # 青色
        ]
        
        # 为每个节点分配颜色
        for i in range(len(labels)):
            node_colors.append(color_palette[i % len(color_palette)])
        
        # 为链接生成颜色（基于源节点）- 标准粗度
        link_colors = []
        for source_idx in links['source']:
            base_color = color_palette[source_idx % len(color_palette)]
            link_color = base_color.replace('0.8', '0.5')  # 标准透明度
            link_colors.append(link_color)
        
        fig = go.Figure(go.Sankey(
            node=dict(
                pad=15,           # 标准节点间距，从20降到15
                thickness=18,     # 标准节点厚度，从20降到18
                line=dict(color="rgba(0,0,0,0.5)", width=1),
                label=labels,
                color=node_colors,
                hovertemplate='节点: %{label}<br>总流量: %{value}<extra></extra>'
            ),
            link=dict(
                source=links['source'],
                target=links['target'],
                value=links['value'],
                color=link_colors,
                hovertemplate='从: %{source.label}<br>到: %{target.label}<br>金额: %{value}<extra></extra>'
            ),
            orientation="h",
            arrangement="snap"
        ))
        
        fig.update_layout(
            title={
                'text': title,
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 18, 'family': "Microsoft YaHei, SimHei, Arial"}
            },
            font=dict(
                family="Microsoft YaHei, SimHei, Arial",
                size=12
            ),
            width=1200,
            height=600,
            plot_bgcolor='rgba(240,240,240,0.1)',
            paper_bgcolor='white',
            margin=dict(l=20, r=20, t=60, b=20)
        )
        
        if interactive and output_path:
            pio.write_html(fig, output_path, auto_open=False)
            print(f"标准Sankey图已导出为: {output_path}")
        
        return fig
        
    except Exception as e:
        print(f"生成标准Sankey图时出错: {e}")
        return None


def plot_sankey(df, from_col='from_address', to_col='to_address', value_col='value', 
                title='资金流向图', output_path='sankey.html', interactive=True,
                address_labels=None, top_n=20):
    """
    绘制Sankey图显示资金流向
    
    参数:
    - df: 数据框，包含转账记录
    - from_col: 源地址列名
    - to_col: 目标地址列名  
    - value_col: 金额列名
    - title: 图表标题
    - output_path: 输出路径
    - interactive: 是否生成交互式HTML
    - address_labels: 地址标签映射
    - top_n: 显示前N个地址
    """
    try:
        labels, links = build_sankey_data(df, from_col, to_col, value_col, address_labels, top_n)
        
        # 为节点生成不同颜色
        import random
        random.seed(42)  # 固定随机种子，确保颜色一致
        node_colors = []
        color_palette = [
            'rgba(31, 119, 180, 0.8)',   # 蓝色
            'rgba(255, 127, 14, 0.8)',   # 橙色
            'rgba(44, 160, 44, 0.8)',    # 绿色
            'rgba(214, 39, 40, 0.8)',    # 红色
            'rgba(148, 103, 189, 0.8)',  # 紫色
            'rgba(140, 86, 75, 0.8)',    # 棕色
            'rgba(227, 119, 194, 0.8)',  # 粉色
            'rgba(127, 127, 127, 0.8)',  # 灰色
            'rgba(188, 189, 34, 0.8)',   # 橄榄色
            'rgba(23, 190, 207, 0.8)',   # 青色
        ]
        
        # 为每个节点分配颜色
        for i in range(len(labels)):
            node_colors.append(color_palette[i % len(color_palette)])
        
        # 为链接生成颜色（基于源节点）
        link_colors = []
        for source_idx in links['source']:
            # 使用源节点的颜色，但透明度更低
            base_color = color_palette[source_idx % len(color_palette)]
            # 将颜色透明度降低，让线条更细腻
            link_color = base_color.replace('0.8', '0.25')  # 从0.4降到0.25
            link_colors.append(link_color)
        
        fig = go.Figure(go.Sankey(
            node=dict(
                pad=10,           # 减少节点间距，从25降到10，让节点更短
                thickness=12,     # 稍微增加厚度，从15到12
                line=dict(color="rgba(0,0,0,0.3)", width=0.5),  # 节点边框更细
                label=labels,
                color=node_colors,
                # 添加字体设置
                hovertemplate='节点: %{label}<br>总流量: %{value}<extra></extra>'
            ),
            link=dict(
                source=links['source'],
                target=links['target'],
                value=links['value'],
                color=link_colors,
                # 添加悬停信息
                hovertemplate='从: %{source.label}<br>到: %{target.label}<br>金额: %{value}<extra></extra>'
            ),
            # 设置布局方向和对齐
            orientation="h",  # 水平布局
            arrangement="snap"  # 自动对齐
        ))
        
        fig.update_layout(
            title={
                'text': title,
                'x': 0.5,
                'xanchor': 'center',
                'font': {'size': 18, 'family': "Microsoft YaHei, SimHei, Arial"}
            },
            font=dict(
                family="Microsoft YaHei, SimHei, Arial",
                size=12
            ),
            # 增加图表尺寸
            width=1200,
            height=600,
            # 设置背景
            plot_bgcolor='rgba(240,240,240,0.1)',
            paper_bgcolor='white',
            # 添加边距
            margin=dict(l=20, r=20, t=60, b=20)
        )
        
        if interactive and output_path:
            pio.write_html(fig, output_path, auto_open=False)
            print(f"Sankey图已导出为: {output_path}")
        
        return fig
        
    except Exception as e:
        print(f"生成Sankey图时出错: {e}")
        return None


def plot_network_flow(df, from_col='from_address', to_col='to_address', value_col='value',
                     title='资金流向网络图', output_path='network_flow.html',
                     address_labels=None, top_n=20):
    """
    绘制网络图形式的资金流向（更清晰的布局）
    """
    try:
        import plotly.graph_objects as go
        import networkx as nx
        import numpy as np
        
        # 准备数据
        labels, links = build_sankey_data(df, from_col, to_col, value_col, address_labels, top_n)
        
        # 创建NetworkX图
        G = nx.DiGraph()
        
        # 添加节点
        for i, label in enumerate(labels):
            G.add_node(i, label=label)
        
        # 添加边
        for source, target, value in zip(links['source'], links['target'], links['value']):
            G.add_edge(source, target, weight=value)
        
        # 使用分层布局
        try:
            pos = nx.spring_layout(G, k=3, iterations=50, seed=42)
        except:
            pos = nx.random_layout(G, seed=42)
        
        # 准备节点坐标
        node_x = []
        node_y = []
        node_text = []
        node_size = []
        
        for node in G.nodes():
            x, y = pos[node]
            node_x.append(x)
            node_y.append(y)
            node_text.append(labels[node])
            # 根据流入流出量设置节点大小
            in_flow = sum([G[u][node]['weight'] for u in G.predecessors(node)])
            out_flow = sum([G[node][v]['weight'] for v in G.successors(node)])
            total_flow = in_flow + out_flow
            node_size.append(max(20, min(50, total_flow / 1000000000000000000 * 30)))  # 归一化节点大小
        
        # 准备边
        edge_x = []
        edge_y = []
        edge_info = []
        
        for edge in G.edges():
            x0, y0 = pos[edge[0]]
            x1, y1 = pos[edge[1]]
            edge_x.extend([x0, x1, None])
            edge_y.extend([y0, y1, None])
            weight = G[edge[0]][edge[1]]['weight']
            edge_info.append(f"{labels[edge[0]]} -> {labels[edge[1]]}: {weight}")
        
        # 创建边的轨迹
        edge_trace = go.Scatter(
            x=edge_x, y=edge_y,
            line=dict(width=1, color='rgba(120,120,120,0.4)'),  # 线条更细，从width=2降到1，颜色更淡
            hoverinfo='none',
            mode='lines'
        )
        
        # 创建节点的轨迹
        node_trace = go.Scatter(
            x=node_x, y=node_y,
            mode='markers+text',
            hoverinfo='text',
            text=node_text,
            textposition="middle center",
            hovertext=node_text,
            marker=dict(
                size=node_size,
                color=list(range(len(node_x))),
                colorscale='Viridis',
                line=dict(width=1, color='rgba(255,255,255,0.8)')  # 节点边框更细，从width=2降到1
            )
        )
        
        # 创建图形
        fig = go.Figure(data=[edge_trace, node_trace],
                       layout=go.Layout(
                           title=title,
                           titlefont_size=16,
                           showlegend=False,
                           hovermode='closest',
                           margin=dict(b=20,l=5,r=5,t=40),
                           annotations=[ dict(
                               text="资金流向网络图 - 节点大小代表流量规模",
                               showarrow=False,
                               xref="paper", yref="paper",
                               x=0.005, y=-0.002,
                               xanchor='left', yanchor='bottom',
                               font=dict(color="grey", size=12)
                           )],
                           xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                           yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)
                       ))
        
        if output_path:
            pio.write_html(fig, output_path, auto_open=False)
            print(f"网络流向图已导出为: {output_path}")
        
        return fig
        
    except Exception as e:
        print(f"生成网络流向图时出错: {e}")
        return None


# 保持原有函数的兼容性
def build_sankey_data_legacy(tx_df, address_labels=None, top_n=20):
    """兼容旧版本的函数"""
    return build_sankey_data(tx_df, 'from_address', 'to_address', 'value', address_labels, top_n)


def plot_sankey_legacy(tx_df, address_labels=None, top_n=20, title="资金流向Sankey图", output_html=None):
    """兼容旧版本的函数"""
    labels, links = build_sankey_data_legacy(tx_df, address_labels, top_n)
    fig = go.Figure(go.Sankey(
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="black", width=0.5),
            label=labels,
            color="blue"
        ),
        link=dict(
            source=links['source'],
            target=links['target'],
            value=links['value'],
        )
    ))
    fig.update_layout(title_text=title, font_size=12)
    if output_html:
        pio.write_html(fig, output_html, auto_open=False)
        print(f"Sankey图已导出为: {output_html}")
    return fig


# 示例用法
if __name__ == '__main__':
    data = [
        {'from_address': 'A', 'to_address': 'B', 'value': 100},
        {'from_address': 'B', 'to_address': 'C', 'value': 80},
        {'from_address': 'A', 'to_address': 'C', 'value': 50},
        {'from_address': 'C', 'to_address': 'D', 'value': 30},
    ]
    df = pd.DataFrame(data)
    plot_sankey(df, top_n=4, output_path='test_sankey.html')
