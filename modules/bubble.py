import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
from trans import get_okx_transaction_df
from holder import get_all_holders
from datetime import datetime, timezone
import requests
import time
from tqdm import tqdm
from collections import defaultdict
import random

# 修复字段命名问题并优化交易处理
def get_batch_transactions(addresses, chain_id, start_time, end_time):
    """批量获取地址交易记录（优化版）"""
    all_transactions = []
    
    # 随机打乱地址顺序
    random.shuffle(addresses)
    
    # 添加进度条
    with tqdm(total=len(addresses), desc="获取交易记录") as pbar:
        for idx, address in enumerate(addresses):
            # 指数退避重试机制
            max_retries = 5
            for attempt in range(max_retries):
                try:
                    # 随机延迟
                    delay = random.uniform(0.8, 1.5)
                    time.sleep(delay)
                    
                    # 获取地址交易记录
                    df_tx = get_okx_transaction_df(
                        address, 
                        chain=chain_id,
                        begin=start_time,
                        end=end_time
                    )
                    
                    # 如果DataFrame为空，跳过
                    if df_tx.empty:
                        break
                        
                    # 确保列存在 - 使用英文字段名
                    if 'sender_address' in df_tx.columns and 'receiver_address' in df_tx.columns:
                        # 筛选与持仓地址相关的交易
                        mask = (
                            (df_tx['sender_address'].isin(addresses)) | 
                            (df_tx['receiver_address'].isin(addresses))
                        )
                        relevant_tx = df_tx[mask]
                    else:
                        # 如果列不存在，保留所有交易
                        relevant_tx = df_tx
                    
                    # 过滤交易数量
                    if 'coin_amount' in relevant_tx.columns:
                        # 转换为数值类型
                        relevant_tx.loc[:, 'coin_amount'] = pd.to_numeric(relevant_tx['coin_amount'], errors='coerce')
                        # 过滤掉数量小于0.1的交易
                        relevant_tx = relevant_tx.dropna(subset=['coin_amount'])
                        relevant_tx = relevant_tx[relevant_tx['coin_amount'] >= 0.1]
                    
                    # 添加到结果集
                    if not relevant_tx.empty:
                        all_transactions.extend(relevant_tx.to_dict("records"))
                    
                    # 成功处理，跳出重试循环
                    break
                    
                except requests.exceptions.HTTPError as e:
                    # 处理429限流错误
                    if e.response.status_code == 429:
                        if attempt < max_retries - 1:
                            # 指数退避
                            backoff = attempt
                            pbar.write(f"地址 {address[:8]}... 请求过多，等待 {backoff} 秒后重试 ({attempt+1}/{max_retries})")
                            time.sleep(backoff)
                        else:
                            pbar.write(f"地址 {address[:8]}... 请求失败（已尝试 {max_retries} 次），跳过")
                            break
                    else:
                        # 其他HTTP错误
                        pbar.write(f"地址 {address[:8]}... HTTP错误: {e.response.status_code}, 跳过")
                        break
                except Exception as e:
                    # 其他异常
                    if attempt < max_retries - 1:
                        pbar.write(f"地址 {address[:8]}... 错误: {str(e)}，等待后重试 ({attempt+1}/{max_retries})")
                        time.sleep(2)
                    else:
                        pbar.write(f"地址 {address[:8]}... 请求失败（已尝试 {max_retries} 次），跳过")
                        break
            
            pbar.update(1)
            
            # 每处理10个地址增加一次长延迟
            if idx > 0 and idx % 10 == 0:
                long_delay = random.uniform(5, 10)
                pbar.write(f"已完成 {idx+1} 个地址，暂停 {long_delay:.1f} 秒释放压力")
                time.sleep(long_delay)
    
    # 将交易记录转换为DataFrame
    if not all_transactions:
        return pd.DataFrame()
    
    return pd.DataFrame(all_transactions)

# 使用英文字段名
def find_related_groups(transactions_df, holders_df):
    """识别关联地址群组"""
    G = nx.Graph()
    
    # 添加所有持仓地址作为节点
    for _, row in holders_df.iterrows():
        G.add_node(row["address"], balance=row["balance"])
    
    # 添加交易关系作为边
    for _, tx in transactions_df.iterrows():
        from_addr = tx["sender_address"]
        to_addr = tx["receiver_address"]
        
        # 只添加持仓地址间的关系
        if from_addr in G and to_addr in G and from_addr != to_addr:
            # 添加边（如果已存在则增加权重）
            if G.has_edge(from_addr, to_addr):
                G[from_addr][to_addr]["weight"] += 1
            else:
                G.add_edge(from_addr, to_addr, weight=1)
    
    # 识别连通组件（关联群组）
    groups = {}
    for i, comp in enumerate(nx.connected_components(G)):
        group_name = f"Group_{i+1}"
        for addr in comp:
            groups[addr] = group_name
    
    return groups, G


def calculate_group_holdings(holders_df, groups):
    """计算各集群持仓总量并优化分组排序"""
    # 添加群组信息
    holders_df["group"] = holders_df["address"].map(groups).fillna("非集团地址")
    
    # 计算各群组总持仓
    group_totals = holders_df.groupby("group")["balance"].sum().reset_index()
    group_totals.columns = ["group", "total_balance"]
    group_totals = group_totals.sort_values("total_balance", ascending=False)
    
    # 创建分组排序映射
    group_ranking = {}
    for i, (_, row) in enumerate(group_totals.iterrows()):
        group_ranking[row["group"]] = i + 1
    
    # 添加分组排序标记
    def get_group_sort_key(row):
        group = row["group"]
        # 非集团地址排最后
        if group == "非集团地址":
            return (9999, -row["balance"])
        # 集团地址按总持仓排序
        return (group_ranking[group], -row["balance"])
    
    # 添加全局排名
    holders_df = holders_df.sort_values("balance", ascending=False)
    holders_df["global_rank"] = range(1, len(holders_df) + 1)
    
    # 按分组排序
    holders_df["sort_key"] = holders_df.apply(get_group_sort_key, axis=1)
    holders_df = holders_df.sort_values("sort_key").drop(columns=["sort_key"])
    
    # 重新计算百分比（基于总持仓量）
    total_balance = holders_df["balance"].sum()
    holders_df["percentage"] = holders_df["balance"] / total_balance * 100
    
    return holders_df, group_totals

def format_output(holders_df, group_totals):
    """格式化输出结果"""
    # 准备输出数据
    output_lines = []
    current_group = None
    
    # 计算总持仓量
    total_balance = holders_df["balance"].sum()
    
    # 按分组输出
    for _, row in holders_df.iterrows():
        # 分组标题
        if row["group"] != current_group:
            current_group = row["group"]
            
            # 计算当前分组的总持仓和百分比
            group_total = group_totals[group_totals["group"] == current_group]["total_balance"].values[0]
            group_percentage = group_total / total_balance * 100
            
            # 计算当前分组地址数量
            group_count = holders_df[holders_df["group"] == current_group].shape[0]
            
            # 添加分组标题
            if current_group == "非集团地址":
                output_lines.append("\n其他地址 ({})    {:.2f}%".format(group_count, group_percentage))
            else:
                output_lines.append("\n{} ({})    {:.2f}%".format(current_group, group_count, group_percentage))
        
        # 格式化地址
        address = row["address"]
        short_address = address[:4] + "..." + address[-4:] if len(address) > 8 else address
        
        # 添加地址行
        output_lines.append("#{:<3} {:<15} ----   {:.2f}%".format(
            row["global_rank"],
            short_address,
            row["percentage"]
        ))
    
    # 输出结果
    print("\n".join(output_lines))
    
    # 保存到文件
    with open("grouped_holders.txt", "w") as f:
        f.write("\n".join(output_lines))

# 主函数
def main():
    # 1. 参数配置
    CHAIN_ID = "501"  # Solana
    TOKEN_ADDRESS = "75jh5PfbJn78LMuy7txwmXTb3kYaTi2T4xRtk4a85dQ3"
    START_TIME = datetime(2025, 6, 15)
    END_TIME = datetime.now()
    search_time=datetime.now()
    # 2. 获取持仓数据
    print("Fetching holder data...")
    holders_df = get_all_holders(CHAIN_ID, TOKEN_ADDRESS, search_time, top_n=100)
    holders_df.to_csv("holders.csv", index=False)
    
    # 3. 获取交易数据
    print("Fetching transaction data...")
    addresses = holders_df["address"].tolist()
    transactions_df = get_batch_transactions(
        addresses, CHAIN_ID, START_TIME, END_TIME
    )
    
    # 保存交易数据
    if not transactions_df.empty:
        transactions_df.to_csv("transactions.csv", index=False)
    else:
        print("No transactions found")
    
    transactions_df = pd.read_csv("transactions.csv")
    holders_df= pd.read_csv("holders.csv")
    
    # 4. 分析关联群组
    print("Analyzing address relationships...")
    groups, graph = find_related_groups(transactions_df, holders_df)
    
    # 5. 计算集群持仓
    holders_df, group_totals = calculate_group_holdings(holders_df, groups)
    
    # 6. 保存结果
    holders_df.to_csv("holders_with_groups.csv", index=False)
    group_totals.to_csv("group_holdings.csv", index=False)

    # 格式化输出结果
    print("\n格式化持仓报告:")
    format_output(holders_df, group_totals)
    
    print("Analysis complete!")

if __name__ == "__main__":
    main()