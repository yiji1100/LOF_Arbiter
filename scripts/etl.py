"""
LOF Arbiter - ETL 模块
从 akshare 获取 LOF 基金数据
"""

import akshare as ak
import pandas as pd
from datetime import date
from typing import Dict, List, Optional

from scripts.db import save_lof_data, init_database


def make_full_code(code: str) -> str:
    """构造标准格式代码 xxxxxx.SZ / xxxxxx.SH"""
    c = str(code).strip().lower().replace('sz', '').replace('sh', '').replace('.', '')
    suffix = 'SZ' if (len(c) >= 1 and c[0] in '0123456789') else 'SH'
    return f"{c}.{suffix}"


def clean_code(code: str) -> str:
    """清理基金代码"""
    return str(code).strip().lower().replace('sz', '').replace('sh', '').replace('.', '')


def fetch_lof_realtime() -> pd.DataFrame:
    """
    获取 LOF 基金实时行情（来自新浪财经）
    """
    try:
        df = ak.fund_etf_category_sina(symbol='LOF基金')
        df.rename(columns={
            '代码': 'fund_code',
            '名称': 'fund_name',
            '最新价': 'price',
            '涨跌幅': 'change_pct',
            '涨跌额': 'change_amt',
            '成交量': 'volume',
            '成交额': 'turnover',
            '今开': 'open',
            '最高': 'high',
            '最低': 'low',
            '昨收': 'prev_close',
            '基金类型': 'fund_type'
        }, inplace=True)
        
        df['fund_code_full'] = df['fund_code'].apply(make_full_code)
        df['fund_code_clean'] = df['fund_code'].apply(clean_code)
        
        return df
    except Exception as e:
        print(f"获取 LOF 实时行情失败: {e}")
        return pd.DataFrame()


def fetch_lof_nav() -> pd.DataFrame:
    """
    获取 LOF 基金净值数据（来自东方财富）
    净值字段优先级：第3列 > 第5列
    """
    try:
        df = ak.fund_open_fund_daily_em()
        
        # 获取净值列
        nav_cols = sorted([c for c in df.columns if '单位净值' in c], reverse=True)
        
        if len(nav_cols) >= 1:
            latest_col = nav_cols[0]  # 第3列
            latest_date = latest_col.replace('-单位净值', '')
            df['nav'] = pd.to_numeric(df[latest_col], errors='coerce')
            df['nav_date'] = latest_date
            
            if len(nav_cols) >= 2:
                prev_col = nav_cols[1]  # 第5列
                prev_date = prev_col.replace('-单位净值', '')
                df['prev_nav'] = pd.to_numeric(df[prev_col], errors='coerce')
                df['prev_nav_date'] = prev_date
        else:
            df['nav'] = None
            df['nav_date'] = None
            df['prev_nav'] = None
            df['prev_nav_date'] = None
        
        df['fund_code_clean'] = df['基金代码'].apply(clean_code)
        
        # 选择需要的列
        nav_df = df[['fund_code_clean', 'nav', 'nav_date', 'prev_nav', 'prev_nav_date']].copy()
        nav_df.columns = ['fund_code_clean', 'nav', 'nav_date', 'prev_nav', 'prev_nav_date']
        
        return nav_df
    except Exception as e:
        print(f"获取 LOF 净值数据失败: {e}")
        return pd.DataFrame()


def fetch_lof_purchase() -> pd.DataFrame:
    """
    获取 LOF 基金申购状态（来自东方财富）
    """
    try:
        df = ak.fund_purchase_em()
        df['fund_code_clean'] = df['基金代码'].apply(clean_code)
        
        df.rename(columns={
            '基金代码': 'fund_code',
            '基金简称': 'fund_name',
            '申购状态': 'purchase_status',
            '购买起点': 'purchase_limit',
            '日累计限定金额': 'daily_limit',
            '手续费': 'fee_rate'
        }, inplace=True)
        
        purchase_df = df[['fund_code_clean', 'purchase_status', 'purchase_limit', 'daily_limit', 'fee_rate']].copy()
        
        return purchase_df
    except Exception as e:
        print(f"获取 LOF 申购状态失败: {e}")
        return pd.DataFrame()


def run_etl() -> Dict:
    """
    运行 ETL：抓取并融合 LOF 数据
    """
    trade_date = date.today().strftime('%Y-%m-%d')
    
    print(f"开始 LOF ETL，日期: {trade_date}")
    
    # 1. 获取实时行情
    print("获取 LOF 实时行情...")
    df_price = fetch_lof_realtime()
    if df_price.empty:
        return {'success': False, 'error': '获取实时行情失败'}
    print(f"  行情数据: {len(df_price)} 条")
    
    # 2. 获取净值数据
    print("获取净值数据...")
    df_nav = fetch_lof_nav()
    if df_nav.empty:
        print("  净值数据获取失败，使用默认值")
    else:
        print(f"  净值数据: {len(df_nav)} 条")
    
    # 3. 获取申购状态
    print("获取申购状态...")
    df_purchase = fetch_lof_purchase()
    if df_purchase.empty:
        print("  申购状态获取失败，使用默认值")
    else:
        print(f"  申购状态: {len(df_purchase)} 条")
    
    # 4. 数据融合
    print("融合数据...")
    
    # 选择需要的列（处理可能不存在的列）
    price_cols = ['fund_code', 'fund_code_full', 'fund_name', 'price', 'change_pct', 'turnover', 'fund_code_clean']
    df_price_sel = df_price[[c for c in price_cols if c in df_price.columns]].copy()
    if 'fund_type' in df_price.columns:
        df_price_sel['fund_type'] = df_price['fund_type']
    
    df_result = df_price_sel.copy()
    
    # 关联净值
    if not df_nav.empty:
        df_result = df_result.merge(
            df_nav, left_on='fund_code_clean', right_on='fund_code_clean', how='left'
        )
    else:
        df_result['nav'] = None
        df_result['nav_date'] = None
        df_result['prev_nav'] = None
        df_result['prev_nav_date'] = None
    
    # 关联申购状态
    if not df_purchase.empty:
        df_result = df_result.merge(
            df_purchase, left_on='fund_code_clean', right_on='fund_code_clean', how='left'
        )
    else:
        df_result['purchase_status'] = None
        df_result['purchase_limit'] = None
        df_result['daily_limit'] = None
        df_result['fee_rate'] = None
    
    # 5. 计算溢价率（优先使用 nav，为空则用 prev_nav）
    def calc_premium(row):
        price = row.get('price')
        nav = row.get('nav') if pd.notna(row.get('nav')) else row.get('prev_nav')
        if price and nav and nav > 0:
            return (price - nav) / nav * 100
        return None
    
    df_result['premium_rate'] = df_result.apply(calc_premium, axis=1)
    
    # 6. 转换数据为字典列表
    data = []
    for _, row in df_result.iterrows():
        # 确定净值和净值日期（优先 nav）
        nav = row.get('nav') if pd.notna(row.get('nav')) else row.get('prev_nav')
        nav_date = row.get('nav_date') if pd.notna(row.get('nav_date')) else row.get('prev_nav_date')
        
        item = {
            'fund_code': str(row.get('fund_code', '')).strip(),
            'fund_code_full': row.get('fund_code_full'),
            'fund_name': row.get('fund_name'),
            'price': row.get('price'),
            'nav': nav,
            'nav_date': nav_date,
            'prev_nav': row.get('prev_nav') if pd.notna(row.get('prev_nav')) else None,
            'prev_nav_date': row.get('prev_nav_date') if pd.notna(row.get('prev_nav_date')) else None,
            'premium_rate': row.get('premium_rate'),
            'turnover': row.get('turnover'),
            'change_pct': row.get('change_pct'),
            'purchase_status': row.get('purchase_status'),
            'purchase_limit': row.get('purchase_limit'),
            'daily_limit': row.get('daily_limit'),
            'fee_rate': row.get('fee_rate'),
        }
        data.append(item)
    
    # 7. 保存数据库
    init_database()
    count = save_lof_data(data, trade_date)
    
    print(f"LOF ETL 完成: {count} 条")
    
    return {
        'success': True,
        'records_count': count,
        'trade_date': trade_date
    }


if __name__ == '__main__':
    result = run_etl()
    print(result)
