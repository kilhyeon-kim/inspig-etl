#!/usr/bin/env python3
"""TS_INS_WEEK 테이블 컬럼 확인"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from src.common import Config, Database

def main():
    config = Config()
    db = Database(config)

    with db.get_connection() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            SELECT COLUMN_NAME FROM USER_TAB_COLUMNS
            WHERE TABLE_NAME = 'TS_INS_WEEK'
            ORDER BY COLUMN_ID
        """)
        print("TS_INS_WEEK 컬럼:")
        for row in cursor.fetchall():
            print(f"  {row[0]}")

        cursor.close()

if __name__ == '__main__':
    main()
