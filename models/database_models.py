"""
数据库模型定义
定义了所有的数据表结构
"""

from sqlalchemy import Column, Integer, String, Float, DateTime, Text, Boolean, Index
from sqlalchemy.sql import func
from config.database import Base

class TopTrader(Base):
    """TOP交易者数据表"""
    __tablename__ = 'top_traders'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    wallet_address = Column(String(100), nullable=False, index=True)
    token_address = Column(String(100), nullable=False, index=True)
    chain_id = Column(String(10), nullable=False, index=True)
    
    # 盈亏数据
    total_pnl = Column(Float, default=0.0)
    total_pnl_percentage = Column(Float, default=0.0)
    realized_profit = Column(Float, default=0.0)
    realized_profit_percentage = Column(Float, default=0.0)
    roi = Column(Float, default=0.0)
    
    # 交易统计
    buy_count = Column(Integer, default=0)
    sell_count = Column(Integer, default=0)
    total_count = Column(Integer, default=0)
    win_rate = Column(Float, default=0.0)
    
    # 交易金额
    buy_value = Column(Float, default=0.0)
    sell_value = Column(Float, default=0.0)
    hold_amount = Column(Float, default=0.0)
    
    # 价格信息
    bought_avg_price = Column(Float, default=0.0)
    sold_avg_price = Column(Float, default=0.0)
    
    # 标签和备注
    tags = Column(Text)
    remark = Column(Text)
    
    # 时间戳
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    # 复合索引
    __table_args__ = (
        Index('idx_token_chain', 'token_address', 'chain_id'),
        Index('idx_wallet_token', 'wallet_address', 'token_address'),
        Index('idx_pnl_desc', 'total_pnl'),
    )

class TokenHolder(Base):
    """代币持有者数据表"""
    __tablename__ = 'token_holders'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    wallet_address = Column(String(100), nullable=False, index=True)
    token_address = Column(String(100), nullable=False, index=True)
    chain_id = Column(String(10), nullable=False, index=True)
    
    # 持仓信息
    balance = Column(Float, default=0.0)
    balance_usd = Column(Float, default=0.0)
    percentage = Column(Float, default=0.0)
    
    # 代币信息
    token_symbol = Column(String(20))
    token_name = Column(String(100))
    
    # 时间戳
    snapshot_time = Column(DateTime(timezone=True), server_default=func.now())
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # 复合索引
    __table_args__ = (
        Index('idx_token_chain_holder', 'token_address', 'chain_id'),
        Index('idx_wallet_token_holder', 'wallet_address', 'token_address'),
        Index('idx_balance_desc', 'balance_usd'),
    )

class WalletTag(Base):
    """钱包标签数据表"""
    __tablename__ = 'wallet_tags'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    wallet_address = Column(String(100), nullable=False, unique=True, index=True)
    
    # 标签信息
    tags = Column(Text)  # JSON格式存储多个标签
    remark = Column(Text)
    category = Column(String(50))  # 分类：whale, smart_money, institution等
    
    # 分析结果
    is_smart_money = Column(Boolean, default=False)
    is_whale = Column(Boolean, default=False)
    is_institution = Column(Boolean, default=False)
    risk_level = Column(String(20))  # low, medium, high
    
    # 统计信息
    total_tokens = Column(Integer, default=0)
    total_volume_usd = Column(Float, default=0.0)
    avg_profit_rate = Column(Float, default=0.0)
    
    # 时间戳
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

class TransactionHistory(Base):
    """交易历史记录表"""
    __tablename__ = 'transaction_history'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    tx_hash = Column(String(100), nullable=False, unique=True, index=True)
    wallet_address = Column(String(100), nullable=False, index=True)
    token_address = Column(String(100), nullable=False, index=True)
    chain_id = Column(String(10), nullable=False, index=True)
    
    # 交易信息
    transaction_type = Column(String(20))  # buy, sell, transfer
    amount = Column(Float, default=0.0)
    price = Column(Float, default=0.0)
    value_usd = Column(Float, default=0.0)
    
    # 区块信息
    block_number = Column(Integer)
    block_timestamp = Column(DateTime(timezone=True))
    
    # 时间戳
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # 复合索引
    __table_args__ = (
        Index('idx_wallet_token_tx', 'wallet_address', 'token_address'),
        Index('idx_token_time', 'token_address', 'block_timestamp'),
        Index('idx_block_number', 'block_number'),
    )

class AnalysisJob(Base):
    """分析任务记录表"""
    __tablename__ = 'analysis_jobs'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    job_type = Column(String(50), nullable=False)  # top_traders, holders, wallet_analysis
    job_params = Column(Text)  # JSON格式存储参数
    
    # 状态信息
    status = Column(String(20), default='pending')  # pending, running, completed, failed
    progress = Column(Integer, default=0)  # 0-100
    
    # 结果信息
    result_count = Column(Integer, default=0)
    error_message = Column(Text)
    
    # 时间戳
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    completed_at = Column(DateTime(timezone=True))
    
    # 索引
    __table_args__ = (
        Index('idx_job_type_status', 'job_type', 'status'),
        Index('idx_created_at', 'created_at'),
    )
