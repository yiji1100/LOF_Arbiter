"""
LOF Arbiter - 数据查询模块

LOF 基金溢价套利机会监测
核心逻辑：
1. 限购限大额 + 高溢价 = 优质套利机会
2. 流动性（成交额）门槛筛选
3. 净值优先取最新日期，其次取上一交易日
"""

import sqlite3
import pandas as pd
from datetime import date, timedelta
from typing import Optional, List, Dict
import os

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
    """
    获取 LOF 基金数据
    
    净值取值逻辑：
    - 优先使用 最新净值日期 的净值
    - 如果为空，使用上一交易日净值
    """
    if trade_date is None:
        trade_date = get_latest_trade_date(db_path)
    
    conn = get_connection(db_path)
    try:
        df = pd.read_sql_query(
            "SELECT * FROM dwd_fund_lof WHERE 交易日期 = ? ORDER BY 成交额 DESC",
            conn,
            params=(trade_date,)
        )
        
        # 计算溢价率（处理数据类型）
        if '现价' in df.columns and '净值' in df.columns:
            df['现价'] = pd.to_numeric(df['现价'], errors='coerce')
            df['净值'] = pd.to_numeric(df['净值'], errors='coerce')
            # 只计算有效的溢价率
            valid_mask = df['现价'].notna() & df['净值'].notna() & (df['净值'] != 0)
            df.loc[valid_mask, '溢价率'] = (df.loc[valid_mask, '现价'] - df.loc[valid_mask, '净值']) / df.loc[valid_mask, '净值'] * 100
            df['溢价率'] = df['溢价率'].fillna(0)
        
        # 格式化成交额（万元）
        if '成交额' in df.columns:
            df['成交额_万元'] = df['成交额'] / 10000
        
        # 申购状态分类（用于筛选）
        def classify_status(status):
            if pd.isna(status):
                return 'unknown'
            if '暂停' in status:
                return 'suspended'
            if '限大额' in status or '限额' in status:
                return 'limited'  # 限购 = 好信号
            if '开放' in status:
                return 'open'
            return 'other'
        
        df['状态分类'] = df['申购状态'].apply(classify_status)
        
        return df
    finally:
        conn.close()


def get_premium_top(
    n: int = 10,
    min_premium: float = 0.5,
    min_turnover: float = 1000000,
    db_path: str = DEFAULT_DB_PATH
) -> pd.DataFrame:
    """
    获取高溢价 TOP N（卖出赎回套利机会）
    
    筛选逻辑：
    1. 溢价率 > 门槛
    2. 成交额 >= 门槛（流动性）
    3. 申购状态不是"暂停申购"
    4. 优先展示：限购 > 开放申购（限购的产品溢价更稳定）
    """
    df = get_lof_data(db_path=db_path)
    
    # 基础筛选
    df = df[df['溢价率'] > min_premium]  # 溢价 > 门槛
    df = df[df['成交额'] >= min_turnover]  # 成交额 > 门槛
    df = df[df['状态分类'] != 'suspended']  # 排除暂停申购
    
    # 按溢价率降序，限购的排前面
    df = df.sort_values(['溢价率', '状态分类'], ascending=[False, True])
    
    return df.head(n)


def get_discount_top(
    n: int = 10,
    min_discount: float = 0.5,
    min_turnover: float = 1000000,
    db_path: str = DEFAULT_DB_PATH
) -> pd.DataFrame:
    """
    获取高折价 TOP N（买入套利机会）
    
    筛选逻辑：
    1. 折价率 > 门槛（折价为负数，绝对值 > 门槛）
    2. 成交额 >= 门槛
    3. 申购状态不是"暂停申购"
    """
    df = get_lof_data(db_path=db_path)
    
    # 基础筛选（折价为负）
    df = df[df['溢价率'] < -min_discount]  # 折价 > 门槛
    df = df[df['成交额'] >= min_turnover]  # 成交额 > 门槛
    df = df[df['状态分类'] != 'suspended']  # 排除暂停申购
    
    # 按折价率升序（折价越多越靠前）
    df = df.sort_values('溢价率', ascending=True)
    
    return df.head(n)


def get_limited_premium_top(
    n: int = 10,
    min_premium: float = 0.5,
    min_turnover: float = 1000000,
    db_path: str = DEFAULT_DB_PATH
) -> pd.DataFrame:
    """
    获取限购高溢价 TOP N（核心套利机会）
    
    限购产品溢价更稳定，套利空间更大
    """
    df = get_lof_data(db_path=db_path)
    
    # 筛选限购产品
    df = df[df['状态分类'] == 'limited']  # 限大额/限额
    df = df[df['溢价率'] > min_premium]
    df = df[df['成交额'] >= min_turnover]
    
    df = df.sort_values('溢价率', ascending=False)
    
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
        
        # 计算溢价率
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


def export_lof_csv(
    filepath: str,
    min_premium: float = 0.5,
    min_turnover: float = 1000000,
    db_path: str = DEFAULT_DB_PATH
) -> str:
    """
    导出 LOF 基金行情 CSV 文件
    
    字段：基金代码, 名称, 溢价率, 当日交易额(万元), 现价, 涨跌幅, 净值, 时间, 申购状态, 购买起点, 日累计限定金额, 手续费
    """
    df = get_lof_data(db_path=db_path)
    
    # 筛选有溢价率或成交额较大的
    df = df[(df['溢价率'].abs() > 0) | (df['成交额'] >= min_turnover)]
    
    # 选择并重命名字段
    export_df = pd.DataFrame()
    export_df['基金代码'] = df['基金代码_full']
    export_df['名称'] = df['基金名称']
    export_df['溢价率'] = df['溢价率'].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else '')
    export_df['当日交易额(万元)'] = df['成交额_万元'].apply(lambda x: f"{x:.2f}" if pd.notna(x) else '')
    export_df['现价'] = df['现价'].apply(lambda x: f"{x:.4f}" if pd.notna(x) else '')
    export_df['涨跌幅'] = df['涨跌幅'].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else '')
    export_df['净值'] = df['净值'].apply(lambda x: f"{x:.4f}" if pd.notna(x) else '')
    export_df['时间'] = df['净值使用日期'].fillna(df['交易日期'])
    export_df['申购状态'] = df['申购状态'].fillna('')
    export_df['购买起点'] = df['购买起点'].apply(lambda x: f"{x:.2f}" if pd.notna(x) else '')
    export_df['日累计限定金额'] = df['日累计限定金额'].apply(lambda x: f"{x:.2f}" if pd.notna(x) else '')
    export_df['手续费'] = df['手续费'].apply(lambda x: f"{x:.4f}" if pd.notna(x) else '')
    
    # 保存 CSV
    export_df.to_csv(filepath, index=False, encoding='utf-8-sig')
    
    return filepath


def format_fund_row(row: Dict, include_status: bool = True) -> str:
    """格式化基金信息为文本"""
    name = row.get('基金名称', '未知')
    code = row.get('基金代码_full', '')
    premium = row.get('溢价率') or 0
    price = row.get('现价') or 0
    nav = row.get('净值') or 0
    nav_date = row.get('净值使用日期') or row.get('交易日期') or ''
    turnover = row.get('成交额') or 0
    turnover_wan = turnover / 10000 if turnover else 0
    status = row.get('申购状态', '未知')
    
    # 溢价率颜色和标签
    if premium is not None and premium > 1:
        premium_str = f"🔥 +{premium:.2f}%"
    elif premium is not None and premium < -1:
        premium_str = f"💎 {premium:.2f}%"
    else:
        premium_str = f"{premium:.2f}%"
    
    # 成交额格式化
    if turnover_wan >= 10000:
        turnover_str = f"{turnover_wan/10000:.2f}亿"
    elif turnover_wan >= 1:
        turnover_str = f"{turnover_wan:.2f}万"
    else:
        turnover_str = f"{turnover_wan*10000:.0f}元"
    
    # 净值日期
    nav_date_str = f"（净值日期: {nav_date}）" if nav_date else ''
    
    # 状态标签
    status_tag = ''
    if include_status:
        if '限大额' in str(status) or '限额' in str(status):
            status_tag = ' [限购]'
        elif '暂停' in str(status):
            status_tag = ' [暂停]'
    
    return (
        f"{name}（{code}）{status_tag}\n"
        f"  溢价率: {premium_str} | 现价: {price:.3f} | 净值: {nav:.4f} {nav_date_str}\n"
        f"  成交额: {turnover_str} | 状态: {status}"
    )


def format_arbitrage_report(db_path: str = DEFAULT_DB_PATH) -> str:
    """生成套利机会报告"""
    lines = []
    
    # 限购高溢价 TOP
    df_limited = get_limited_premium_top(n=5, min_premium=0.5)
    if not df_limited.empty:
        lines.append("🎯 【限购高溢价 TOP5】（优质套利机会）")
        for _, row in df_limited.iterrows():
            lines.append(format_fund_row(row.to_dict()))
            lines.append("")
    else:
        lines.append("🎯 【限购高溢价】今日暂无满足条件的限购高溢价品种")
        lines.append("")
    
    # 高溢价 TOP
    df_premium = get_premium_top(n=5, min_premium=0.5)
    if not df_premium.empty:
        lines.append("🔥 【高溢价 TOP5】（卖出赎回套利）")
        for _, row in df_premium.iterrows():
            lines.append(format_fund_row(row.to_dict()))
            lines.append("")
    
    # 高折价 TOP
    df_discount = get_discount_top(n=5, min_discount=0.5)
    if not df_discount.empty:
        lines.append("💎 【高折价 TOP5】（买入套利）")
        for _, row in df_discount.iterrows():
            lines.append(format_fund_row(row.to_dict()))
            lines.append("")
    
    # 风险提示
    lines.append("⚠️ 风险提示：")
    lines.append("- 套利需 T+2 交割，资金占用两天")
    lines.append("- 赎回费通常 0.5%，持有 <7天 为 1.5%")
    lines.append("- 高溢价需关注流动性，避免无法成交")
    lines.append("- 限购产品溢价更稳定，优先关注")
    
    return "\n".join(lines)


if __name__ == '__main__':
    # 测试
    print("=== LOF Arbiter 测试 ===\n")
    
    # 生成报告
    print(format_arbitrage_report())
    print()
    
    # 导出 CSV 测试
    export_path = '/tmp/lof_export_test.csv'
    export_lof_csv(export_path)
    print(f"✅ CSV 已导出: {export_path}")
