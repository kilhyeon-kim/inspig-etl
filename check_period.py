#!/usr/bin/env python3
"""ETL vs BAK 기간 비교"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from src.common import Config, Database

def main():
    config = Config()
    db = Database(config)

    with db.get_connection() as conn:
        cursor = conn.cursor()

        print("=" * 60)
        print("TS_INS_WEEK vs TS_INS_WEEK_BAK 기간(DT_FROM~DT_TO) 비교")
        print("=" * 60)

        # ETL 결과
        cursor.execute("""
            SELECT REPORT_YEAR, REPORT_WEEK_NO, FARM_NO, DT_FROM, DT_TO
            FROM TS_INS_WEEK
            WHERE REPORT_YEAR = 2025
            ORDER BY REPORT_WEEK_NO, FARM_NO
            FETCH FIRST 10 ROWS ONLY
        """)
        print("\nETL (TS_INS_WEEK):")
        for row in cursor.fetchall():
            print(f"  {row[0]}년 {row[1]}주 농장{row[2]}: {row[3]} ~ {row[4]}")

        # 백업 데이터
        cursor.execute("""
            SELECT REPORT_YEAR, REPORT_WEEK_NO, FARM_NO, DT_FROM, DT_TO
            FROM TS_INS_WEEK_BAK
            WHERE REPORT_YEAR = 2025
            ORDER BY REPORT_WEEK_NO, FARM_NO
            FETCH FIRST 10 ROWS ONLY
        """)
        print("\nBAK (TS_INS_WEEK_BAK):")
        for row in cursor.fetchall():
            print(f"  {row[0]}년 {row[1]}주 농장{row[2]}: {row[3]} ~ {row[4]}")

        cursor.close()

if __name__ == '__main__':
    main()
