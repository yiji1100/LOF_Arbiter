"""
LOF Arbiter - 数据查询模块
"""

import sqlite3
import pandas as pd
from datetime import date, timedelta
from typing import Optional, List, Dict

# 默认数据库路径
DEFAULT_DB_PATH = '/Users/jackyang/.openclaw/workspace/DataHub/datahub.db'


def get_connection(db_path: str = DEFAULT_DB_PATH):
    """获取数据库连接"""
    return sqlite3.connect(db_path)


def get_latest_trade_date(db_path: str = DEFAULT_DB_PATH) -> str:
    """获取最近交易日期"""
    conn = get_connection(db_path)
    try:
        c = conn.execute(
            "SELECT MAX(交易日期) FROM dwd_fund_lof WHERE 交易日期 IS NOT NULL"
        )
        row = c.fetchone()
        return row[0] if row and row[0] else date.today().strftime('%Y-%m-%d')
    finally:
        conn.close()


def get_lof_data(
    trade_date: Optional[str] = None,
    db_path: str = DEFAULT_DB_PATH
) -> pd.DataFrame:
    """获取 LOF 基金数据"""
    if trade_date is None:
        trade_date = get_latest_trade_date(db_path)
    
    conn = get_connection(db_path)
    try:
        df = pd.read_sql_query(
            "SELECT * FROM dwd_fund_lof WHERE 交易日期 = ? ORDER BY 成交额 DESC",
            conn,
            params=(trade_date,)
        )
        
        # 计算溢价率（处理数据类型，过滤无效数据）
        if '现价' in df.columns and '净值' in df.columns:
            df['现价'] = pd.to_numeric(df['现价'], errors='coerce')
            df['净值'] = pd.to_numeric(df['净值'], errors='coerce')
            # 只计算有效的溢价率
            valid_mask = df['现价'].notna() & df['净值'].notna() & (df['净值'] != 0)
            df.loc[valid_mask, '溢价率'] = (df.loc[valid_mask, '现价'] - df.loc[valid_mask, '净值']) / df.loc[valid_mask, '净值'] * 100
            df['溢价率'] = df['溢价率'].fillna(0)
        
        return df
    finally:
        conn.close()


def get_premium_top(
    n: int = 10,
    min_turnover: float = 1000000,
    min_premium: float = 0.5,
    db_path: str = DEFAULT_DB_PATH
) -> pd.DataFrame:
    """获取高溢价 TOP N（卖出赎回套利机会）"""
    df = get_lof_data(db_path=db_path)
    
    # 筛选条件
    df = df[df['溢价率'] > min_premium]  # 溢价 > 门槛
    df = df[df['成交额'] >= min_turnover]  # 成交额 > 门槛
    df = df[df['申购状态'].str.contains('开放', na=False)]  # 可申购（开放申购/开放式等）
    
    # 按溢价率降序
    df = df.sort_values('溢价率', ascending=False)
    
    return df.head(n)


def get_discount_top(
    n: int = 10,
    min_turnover: float = 1000000,
    min_discount: float = 0.5,
    db_path: str = DEFAULT_DB_PATH
) -> pd.DataFrame:
    """获取高折价 TOP N（买入套利机会）"""
    df = get_lof_data(db_path=db_path)
    
    # 筛选条件（折价为负数）
    df = df[df['溢价率'] < -min_discount]  # 折价 > 门槛
    df = df[df['成交额'] >= min_turnover]  # 成交额 > 门槛
    df = df[df['申购状态'].str.contains('开放', na=False)]  # 可申购
    
    # 按折价率升序（折价越多越靠前）
    df = df.sort_values('溢价率', ascending=True)
    
    return df.head(n)


def get_fund_by_code(
    code: str,
    db_path: str = DEFAULT_DB_PATH
) -> Optional[Dict]:
    """根据代码查询基金"""
    # 清理代码
    code = str(code).strip().upper()
    code_clean = code.replace('.SZ', '').replace('.SH', '').replace('SZ', '').replace('SH', '')
    
    conn = get_connection(db_path)
    try:
        # 模糊匹配
        df = pd.read_sql_query(
            """SELECT * FROM dwd_fund_lof 
               WHERE 基金代码_full LIKE ? OR 基金代码_full LIKE ?
               ORDER BY 交易日期 DESC LIMIT 1""",
            conn,
            params=(f'%{code_clean}.SZ', f'%{code_clean}.SH')
        )
        
        if df.empty:
            # 尝试名称匹配
            df = pd.read_sql_query(
                """SELECT * FROM dwd_fund_lof 
                   WHERE 基金名称 LIKE ?
                   ORDER BY 交易日期 DESC LIMIT 1""",
                conn,
                params=(f'%{code}%',)
            )
        
        if df.empty:
            return None
        
        row = df.iloc[0].to_dict()
        
        # 计算溢价率（处理数据类型）
        try:
            price = float(row.get('现价', 0)) if row.get('现价') not in [None, ''] else 0
            nav = float(row.get('净值', 0)) if row.get('净值') not in [None, ''] else 0
            if nav and price:
                row['溢价率'] = (price - nav) / nav * 100
            else:
                row['溢价率'] = None
        except (ValueError, TypeError):
            row['溢价率'] = None
        
        return row
    finally:
        conn.close()


def calculate_arb_profit(
    fund_code: str,
    amount: float,
    hold_days: int = 7,
    db_path: str = DEFAULT_DB_PATH
) -> Optional[Dict]:
    """计算套利收益"""
    fund = get_fund_by_code(fund_code, db_path)
    
    if not fund:
        return None
    
    # 费率参数
    purchase_fee_rate = fund.get('手续费', 0.012)  # 申购费默认 1.2%
    redeem_fee_rate = 0.005 if hold_days >= 7 else 0.015  # 赎回费：持有>=7天 0.5%，否则 1.5%
    commission_rate = 0.0003  # 佣金默认万三
    
    # 计算（处理数据类型）
    try:
        nav = float(fund.get('净值')) if fund.get('净值') not in [None, ''] else 0
        price = float(fund.get('现价')) if fund.get('现价') not in [None, ''] else nav
    except (ValueError, TypeError):
        return None
    
    if not nav:
        return None
    
    # 份额 = 金额 / 净值
    shares = amount / nav
    
    # 买入费用
    purchase_fee = amount * purchase_fee_rate
    
    # 卖出金额（按现价）
    sell_amount = shares * price
    
    # 卖出费用
    redeem_fee = sell_amount * redeem_fee_rate
    commission = sell_amount * commission_rate
    
    # 净收益
    net_profit = sell_amount - amount - purchase_fee - redeem_fee - commission
    net_profit_rate = net_profit / amount * 100
    
    return {
        'fund_name': fund.get('基金名称'),
        'fund_code': fund.get('基金代码_full'),
        'buy_amount': amount,
        'shares': shares,
        'nav': nav,
        'price': price,
        'premium_rate': fund.get('溢价率'),
        'purchase_fee': purchase_fee,
        'redeem_fee': redeem_fee,
        'commission': commission,
        'total_fee': purchase_fee + redeem_fee + commission,
        'net_profit': net_profit,
        'net_profit_rate': net_profit_rate,
        'hold_days': hold_days
    }


def format_fund_row(row: Dict) -> str:
    """格式化基金信息为文本"""
    name = row.get('基金名称', '未知')
    code = row.get('基金代码_full', '')
    premium = row.get('溢价率') or 0
    price = row.get('现价') or 0
    nav = row.get('净值') or 0
    turnover = row.get('成交额') or 0
    status = row.get('申购状态', '未知')
    
    # 溢价率颜色
    if premium is not None and premium > 1:
        premium_str = f"🔥 +{premium:.2f}%"
    elif premium is not None and premium < -1:
        premium_str = f"💎 {premium:.2f}%"
    else:
        premium_str = f"{premium:.2f}%" if premium is not None else "N/A"
    
    # 成交额格式化
    if turnover >= 100000000:
        turnover_str = f"{turnover/100000000:.2f}亿"
    elif turnover >= 10000:
        turnover_str = f"{turnover/10000:.2f}万"
    else:
        turnover_str = f"{turnover:.0f}"
    
    return (
        f"{name}（{code}）\n"
        f"  溢价率: {premium_str} | 现价: {price:.3f} | 净值: {nav:.3f}\n"
        f"  成交额: {turnover_str} | 状态: {status}"
    )


if __name__ == '__main__':
    # 测试
    print("=== LOF Arbiter 测试 ===\n")
    
    # 获取高溢价 TOP5
    print("【高溢价 TOP5】")
    df = get_premium_top(n=5)
    for _, row in df.iterrows():
        print(format_fund_row(row.to_dict()))
        print()
    
    # 查询单只基金
    print("\n【查询 160140】")
    fund = get_fund_by_code('160140')
    if fund:
        print(format_fund_row(fund))
    
    # 收益测算
    print("\n【套利收益测算】")
    result = calculate_arb_profit('160140', 100000, hold_days=7)
    if result:
        print(f"基金: {result['fund_name']}")
        print(f"买入金额: {result['buy_amount']:.2f}")
        print(f"份额: {result['shares']:.2f}")
        print(f"净值: {result['nav']:.4f}")
        print(f"现价: {result['price']:.4f}")
        print(f"申购费: {result['purchase_fee']:.2f}")
        print(f"赎回费: {result['redeem_fee']:.2f}")
        print(f"佣金: {result['commission']:.2f}")
        print(f"净收益: {result['net_profit']:.2f} ({result['net_profit_rate']:.2f}%)")
