"""
LOF Arbiter - 数据库模块
独立管理 LOF 基金数据，不依赖 DataHub
"""

import sqlite3
import os
from datetime import datetime
from typing import Optional

# Skill 数据目录
SKILL_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(SKILL_DIR, 'data', 'lof_arbiter.db')


def get_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    """获取数据库连接"""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    return sqlite3.connect(db_path)


def init_database(db_path: str = DB_PATH) -> None:
    """初始化数据库表结构"""
    conn = get_connection(db_path)
    try:
        cursor = conn.cursor()
        
        # LOF 基金行情表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS lof_daily (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fund_code TEXT NOT NULL,
                fund_code_full TEXT,
                fund_name TEXT,
                price REAL,
                nav REAL,
                nav_date TEXT,
                prev_nav REAL,
                prev_nav_date TEXT,
                premium_rate REAL,
                turnover REAL,
                change_pct REAL,
                purchase_status TEXT,
                purchase_limit REAL,
                daily_limit REAL,
                fee_rate REAL,
                trade_date TEXT,
                etl_time TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(fund_code, trade_date)
            )
        """)
        
        # 创建索引
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_lof_code ON lof_daily(fund_code)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_lof_date ON lof_daily(trade_date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_lof_premium ON lof_daily(premium_rate)")
        
        conn.commit()
        print(f"数据库初始化完成: {db_path}")
    finally:
        conn.close()


def save_lof_data(data: list, trade_date: str, db_path: str = DB_PATH) -> int:
    """保存 LOF 数据，返回插入条数"""
    conn = get_connection(db_path)
    try:
        cursor = conn.cursor()
        count = 0
        for item in data:
            cursor.execute("""
                INSERT OR REPLACE INTO lof_daily 
                (fund_code, fund_code_full, fund_name, price, nav, nav_date, 
                 prev_nav, prev_nav_date, premium_rate, turnover, change_pct,
                 purchase_status, purchase_limit, daily_limit, fee_rate, trade_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                item.get('fund_code'),
                item.get('fund_code_full'),
                item.get('fund_name'),
                item.get('price'),
                item.get('nav'),
                item.get('nav_date'),
                item.get('prev_nav'),
                item.get('prev_nav_date'),
                item.get('premium_rate'),
                item.get('turnover'),
                item.get('change_pct'),
                item.get('purchase_status'),
                item.get('purchase_limit'),
                item.get('daily_limit'),
                item.get('fee_rate'),
                trade_date
            ))
            count += 1
        conn.commit()
        return count
    finally:
        conn.close()


def get_latest_trade_date(db_path: str = DB_PATH) -> Optional[str]:
    """获取最新交易日期"""
    conn = get_connection(db_path)
    try:
        cursor = conn.execute("SELECT MAX(trade_date) FROM lof_daily")
        row = cursor.fetchone()
        return row[0] if row and row[0] else None
    finally:
        conn.close()


def table_exists(table_name: str, db_path: str = DB_PATH) -> bool:
    """检查表是否存在"""
    conn = get_connection(db_path)
    try:
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table_name,)
        )
        return cursor.fetchone() is not None
    finally:
        conn.close()


if __name__ == '__main__':
    init_database()
    print(f"数据库路径: {DB_PATH}")
