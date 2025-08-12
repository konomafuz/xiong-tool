"""
Smart Money Tracker 模块包

此包包含应用程序的核心功能模块：
- top_earners: Top盈利地址分析功能
- smart_accounts: 聪明钱小号检测功能
"""

# 显式导入模块内容，方便外部访问
from .top_earners import (
    fetch_top_traders,
    fetch_address_token_list,
    prepare_tokens_data,
    prepare_traders_data
)

from .smart_accounts import (
    get_token_list,
    get_first_buy,
    get_early_traders,
    find_smart_accounts
)

# 定义包的公开接口
__all__ = [
    'fetch_top_traders',
    'fetch_address_token_list',
    'prepare_tokens_data',
    'prepare_traders_data',
    'get_token_list',
    'get_first_buy',
    'get_early_traders',
    'find_smart_accounts'
]