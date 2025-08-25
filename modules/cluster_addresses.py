# cluster_addresses.py
import networkx as nx
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict
import os

def build_transfer_graph(events_df, min_amount=0):
    """根据转账事件构建地址关系图"""
    G = nx.Graph()
    
    if events_df.empty:
        return G
    
    # 筛选转账事件 - 如果没有type字段，假设所有都是转账
    if 'type' in events_df.columns:
        transfers = events_df[events_df['type'] == 'TRANSFER'].copy()
    else:
        transfers = events_df.copy()
    
    for _, transfer in transfers.iterrows():
        from_addr = transfer.get('from_address', '')
        to_addr = transfer.get('to_address', '')
        # 优先使用'value'字段，如果没有则使用'amount'
        amount = transfer.get('value', transfer.get('amount', 0))
        
        if from_addr and to_addr and amount >= min_amount:
            # 添加边，权重为转账金额
            if G.has_edge(from_addr, to_addr):
                G[from_addr][to_addr]['weight'] += amount
                G[from_addr][to_addr]['count'] += 1
            else:
                G.add_edge(from_addr, to_addr, weight=amount, count=1)
    
    return G

def build_interaction_graph(events_df):
    """构建地址交互图（基于共同参与的交易）"""
    G = nx.Graph()
    
    if events_df.empty:
        return G
    
    # 按交易哈希分组
    for tx_hash, group in events_df.groupby('tx_hash'):
        addresses = set()
        
        for _, event in group.iterrows():
            if event['type'] == 'TRANSFER':
                addresses.add(event.get('from_address', ''))
                addresses.add(event.get('to_address', ''))
            elif event['type'] == 'SWAP':
                addresses.add(event.get('address', ''))
        
        # 去除空地址
        addresses = {addr for addr in addresses if addr}
        
        # 为同一交易中的地址添加连接
        for addr1 in addresses:
            for addr2 in addresses:
                if addr1 != addr2:
                    if G.has_edge(addr1, addr2):
                        G[addr1][addr2]['weight'] += 1
                    else:
                        G.add_edge(addr1, addr2, weight=1)
    
    return G

def cluster_addresses(G, algorithm='louvain'):
    """对地址进行聚类"""
    if G.number_of_nodes() == 0:
        return {}
    
    if algorithm == 'louvain':
        try:
            from networkx.algorithms.community import greedy_modularity_communities
            communities = list(greedy_modularity_communities(G))
        except ImportError:
            # 如果没有louvain，使用连通分量
            communities = list(nx.connected_components(G))
    
    elif algorithm == 'connected_components':
        communities = list(nx.connected_components(G))
    
    else:
        # 默认使用连通分量
        communities = list(nx.connected_components(G))
    
    # 构建地址到聚类ID的映射
    cluster_map = {}
    for idx, community in enumerate(communities):
        for addr in community:
            cluster_map[addr] = idx
    
    return cluster_map

def analyze_clusters(cluster_map, holders_df):
    """分析聚类结果"""
    if not cluster_map:
        return pd.DataFrame(), pd.DataFrame()
    
    cluster_df = pd.DataFrame(list(cluster_map.items()), columns=['address', 'cluster_id'])
    
    # 合并持仓数据
    if not holders_df.empty and 'address' in holders_df.columns:
        cluster_df = cluster_df.merge(holders_df, on='address', how='left')
    
    # 计算聚类统计 - 根据可用字段动态调整
    agg_dict = {'address': 'count'}
    
    # 只有当holders_df不为空且包含相应字段时才进行聚合
    if not holders_df.empty:
        if 'balance' in holders_df.columns:
            agg_dict['balance'] = 'sum'
        if 'percentage' in holders_df.columns:
            agg_dict['percentage'] = 'sum'
    
    cluster_stats = cluster_df.groupby('cluster_id').agg(agg_dict).rename(columns={'address': 'address_count'})
    cluster_stats = cluster_stats.reset_index()
    
    # 根据可用字段进行排序
    if 'balance' in cluster_stats.columns:
        cluster_stats = cluster_stats.sort_values('balance', ascending=False)
    else:
        cluster_stats = cluster_stats.sort_values('address_count', ascending=False)
    
    return cluster_df, cluster_stats

def visualize_clusters(G, cluster_map, output_path='cluster_graph.png'):
    """可视化聚类结果"""
    if G.number_of_nodes() == 0:
        print("图为空，无法可视化")
        return
    
    # 设置matplotlib支持中文
    import matplotlib
    matplotlib.use('Agg')  # 使用非交互式后端
    import matplotlib.pyplot as plt
    plt.rcParams['font.sans-serif'] = ['Microsoft YaHei', 'SimHei', 'Arial Unicode MS', 'DejaVu Sans']
    plt.rcParams['axes.unicode_minus'] = False
    
    plt.figure(figsize=(15, 10))
    
    # 设置布局
    if G.number_of_nodes() < 100:
        pos = nx.spring_layout(G, k=1, iterations=50)
    else:
        pos = nx.spring_layout(G, k=0.5, iterations=30)
    
    # 为每个聚类分配颜色
    unique_clusters = set(cluster_map.values())
    colors = plt.cm.Set3(np.linspace(0, 1, len(unique_clusters)))
    cluster_colors = {cluster: colors[i] for i, cluster in enumerate(unique_clusters)}
    
    # 绘制节点
    for node in G.nodes():
        cluster_id = cluster_map.get(node, -1)
        color = cluster_colors.get(cluster_id, 'gray')
        nx.draw_networkx_nodes(G, pos, nodelist=[node], node_color=[color], 
                              node_size=100, alpha=0.8)
    
    # 绘制边
    nx.draw_networkx_edges(G, pos, alpha=0.3, width=0.5, edge_color='gray')
    
    # 添加标签（仅对度数高的节点）
    high_degree_nodes = [node for node, degree in G.degree() if degree > 2]
    labels = {node: node[:8] + '...' for node in high_degree_nodes}
    nx.draw_networkx_labels(G, pos, labels, font_size=8)
    
    plt.title("地址聚类可视化")
    plt.axis('off')
    plt.tight_layout()
    
    # 保存图片
    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else '.', exist_ok=True)
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    
    print(f"聚类图已保存至: {output_path}")

def full_cluster_analysis(events_df, holders_df, output_dir='./output'):
    """完整的聚类分析流程"""
    print("正在构建转账关系图...")
    transfer_graph = build_transfer_graph(events_df)
    
    print("正在构建交互关系图...")
    interaction_graph = build_interaction_graph(events_df)
    
    print("正在进行聚类分析...")
    transfer_clusters = cluster_addresses(transfer_graph, 'connected_components')
    interaction_clusters = cluster_addresses(interaction_graph, 'louvain')
    
    results = {}
    
    if transfer_clusters:
        transfer_df, transfer_stats = analyze_clusters(transfer_clusters, holders_df)
        results['transfer_clusters'] = transfer_df
        results['transfer_stats'] = transfer_stats
        
        # 可视化
        os.makedirs(output_dir, exist_ok=True)
        visualize_clusters(transfer_graph, transfer_clusters, 
                          os.path.join(output_dir, 'transfer_clusters.png'))
    
    if interaction_clusters:
        interaction_df, interaction_stats = analyze_clusters(interaction_clusters, holders_df)
        results['interaction_clusters'] = interaction_df
        results['interaction_stats'] = interaction_stats
        
        # 可视化
        os.makedirs(output_dir, exist_ok=True)
        visualize_clusters(interaction_graph, interaction_clusters,
                          os.path.join(output_dir, 'interaction_clusters.png'))
    
    return results
def build_co_spend_graph(events_df):
    """构建co-spend（共花费）关系图：同一交易中多个from_address共同作为输入"""
    G = nx.Graph()
    if events_df.empty:
        return G
    
    # 检查是否有tx_hash字段
    if 'tx_hash' not in events_df.columns:
        print("⚠️ 缺少tx_hash字段，无法进行co-spend分析")
        return G
    
    # 只考虑TRANSFER类型，如果没有type字段则假设所有都是转账
    if 'type' in events_df.columns:
        tx_groups = events_df[events_df['type'] == 'TRANSFER'].groupby('tx_hash')
    else:
        tx_groups = events_df.groupby('tx_hash')
    
    for tx_hash, group in tx_groups:
        from_addrs = set(group['from_address'].dropna())
        # 只考虑多输入的情况
        if len(from_addrs) > 1:
            for addr1 in from_addrs:
                for addr2 in from_addrs:
                    if addr1 != addr2:
                        if G.has_edge(addr1, addr2):
                            G[addr1][addr2]['weight'] += 1
                        else:
                            G.add_edge(addr1, addr2, weight=1)
    return G

def co_spend_cluster_analysis(events_df, holders_df, output_path='co_spend_clusters.png'):
    """co-spend聚类分析及可视化"""
    print("正在构建co-spend关系图...")
    G = build_co_spend_graph(events_df)
    if G.number_of_nodes() == 0:
        print("co-spend图为空")
        return None, None
    print("正在进行co-spend聚类...")
    clusters = cluster_addresses(G, 'louvain')
    cluster_df, cluster_stats = analyze_clusters(clusters, holders_df)
    visualize_clusters(G, clusters, output_path)
    return cluster_df, cluster_stats