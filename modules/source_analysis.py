import pandas as pd
from collections import defaultdict
import matplotlib
matplotlib.use('Agg')  # 使用非交互式后端
import matplotlib.pyplot as plt
import numpy as np

# 设置matplotlib支持中文
plt.rcParams['font.sans-serif'] = ['Microsoft YaHei', 'SimHei', 'Arial Unicode MS', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

class SourceAnalyzer:
    """
    资金来源分析工具：支持来源标注、聚合统计
    """
    def __init__(self, tx_df: pd.DataFrame):
        """
        :param tx_df: 包含交易数据的DataFrame，需包含['from_address', 'to_address', 'value', 'tx_hash', 'timestamp']等字段
        """
        self.tx_df = tx_df.copy()
        self.source_map = defaultdict(str)

    def label_sources(self, known_sources: dict):
        """
        对已知地址进行来源标注
        :param known_sources: {address: label}
        """
        self.tx_df['source_label'] = self.tx_df['from_address'].map(known_sources).fillna('未知')
        return self.tx_df

    def aggregate_sources(self, group_by='source_label'):
        """
        按来源标签聚合统计资金流入
        :param group_by: 分组字段，默认'source_label'
        :return: DataFrame
        """
        agg = self.tx_df.groupby(group_by)['value'].sum().reset_index().sort_values('value', ascending=False)
        return agg

    def get_top_sources(self, n=10):
        """
        获取资金流入最多的前n个来源
        """
        agg = self.aggregate_sources()
        return agg.head(n)

# 示例用法
if __name__ == '__main__':
    # 假设有一份交易数据
    data = [
        {'from_address': 'A', 'to_address': 'X', 'value': 100},
        {'from_address': 'B', 'to_address': 'X', 'value': 200},
        {'from_address': 'A', 'to_address': 'X', 'value': 50},
        {'from_address': 'C', 'to_address': 'X', 'value': 300},
    ]
    df = pd.DataFrame(data)
    known = {'A': '交易所', 'B': 'OTC', 'C': '未知'}
    analyzer = SourceAnalyzer(df)
    labeled = analyzer.label_sources(known)
    print(labeled)
    print(analyzer.aggregate_sources())
    print(analyzer.get_top_sources(2))
