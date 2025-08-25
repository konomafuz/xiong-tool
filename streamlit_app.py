import streamlit as st
import pandas as pd
from modules.source_analysis import SourceAnalyzer
from modules.cluster_addresses import build_transfer_graph, cluster_addresses, analyze_clusters, build_co_spend_graph
from modules.sankey_viz import plot_sankey

st.set_page_config(page_title="链上资金流分析", layout="wide")
st.title("链上资金流分析与地址聚类工具")

st.sidebar.header("数据上传与参数设置")
uploaded = st.sidebar.file_uploader("上传交易数据CSV", type=["csv"])

if uploaded:
    df = pd.read_csv(uploaded)
    st.write("## 交易数据预览", df.head())
    
    # 资金来源分析
    st.subheader("1. 资金来源分析")
    if 'from_address' in df.columns:
        known_sources = st.text_area("已知地址标注 (格式: 地址,标签,每行一对)")
        known_map = {}
        if known_sources:
            for line in known_sources.strip().splitlines():
                parts = line.split(',')
                if len(parts) == 2:
                    known_map[parts[0].strip()] = parts[1].strip()
        analyzer = SourceAnalyzer(df)
        labeled = analyzer.label_sources(known_map)
        st.write("标注后数据：", labeled.head())
        agg = analyzer.aggregate_sources()
        st.write("来源聚合统计：", agg)
        st.bar_chart(agg.set_index('source_label'))
    else:
        st.warning("数据缺少from_address字段")
    
    # Sankey可视化
    st.subheader("2. 资金流向Sankey图")
    if {'from_address', 'to_address', 'value'}.issubset(df.columns):
        if st.button("生成Sankey图"):
            fig = plot_sankey(df, top_n=15)
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("数据需包含from_address, to_address, value字段")
    
    # 地址聚类
    st.subheader("3. 地址集群识别与co-spend分析")
    if 'tx_hash' in df.columns:
        if st.button("执行转账关系聚类"):
            G = build_transfer_graph(df)
            clusters = cluster_addresses(G)
            cluster_df, stats = analyze_clusters(clusters, pd.DataFrame())
            st.write("聚类分布：", stats)
        if st.button("执行co-spend分析"):
            G = build_co_spend_graph(df)
            clusters = cluster_addresses(G)
            cluster_df, stats = analyze_clusters(clusters, pd.DataFrame())
            st.write("co-spend聚类分布：", stats)
    else:
        st.info("如需聚类分析，请确保数据包含tx_hash字段")
else:
    st.info("请在左侧上传包含from_address, to_address, value, tx_hash等字段的CSV文件")
