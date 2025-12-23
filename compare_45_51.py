#!/usr/bin/env python3
"""ETL 결과와 백업 데이터 비교 스크립트 (45~51주)"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from src.common import Config, Database

def main():
    config = Config()
    db = Database(config)

    with db.get_connection() as conn:
        cursor = conn.cursor()

        print("=" * 80)
        print("TS_INS_WEEK vs TS_INS_WEEK_BAK 비교 (45~51주)")
        print("=" * 80)

        # 1. 비교 대상 확인
        cursor.execute("""
            SELECT DISTINCT REPORT_WEEK_NO, COUNT(*) AS CNT
            FROM TS_INS_WEEK_BAK
            WHERE REPORT_YEAR = 2024 AND REPORT_WEEK_NO BETWEEN 45 AND 51
            GROUP BY REPORT_WEEK_NO
            ORDER BY REPORT_WEEK_NO
        """)
        bak_weeks = cursor.fetchall()
        if not bak_weeks:
            # 2025년 시도
            cursor.execute("""
                SELECT DISTINCT REPORT_WEEK_NO, COUNT(*) AS CNT
                FROM TS_INS_WEEK_BAK
                WHERE REPORT_YEAR = 2025 AND REPORT_WEEK_NO BETWEEN 45 AND 51
                GROUP BY REPORT_WEEK_NO
                ORDER BY REPORT_WEEK_NO
            """)
            bak_weeks = cursor.fetchall()
            year = 2025
        else:
            year = 2024

        print(f"\n비교 연도: {year}")
        print(f"백업 주차: {[w[0] for w in bak_weeks]}")

        # 2. 컬럼 그룹 정의 (실제 테이블 컬럼명)
        col_groups = {
            'GB (교배)': ['LAST_GB_CNT', 'LAST_GB_SUM'],
            'BM (분만)': ['LAST_BM_CNT', 'LAST_BM_TOTAL', 'LAST_BM_LIVE', 'LAST_BM_DEAD', 'LAST_BM_MUMMY',
                       'LAST_BM_AVG_TOTAL', 'LAST_BM_AVG_LIVE', 'LAST_BM_SUM_CNT', 'LAST_BM_SUM_TOTAL',
                       'LAST_BM_SUM_LIVE', 'LAST_BM_SUM_AVG_TOTAL', 'LAST_BM_SUM_AVG_LIVE'],
            'EU (이유)': ['LAST_EU_CNT', 'LAST_EU_JD_CNT', 'LAST_EU_AVG_JD', 'LAST_EU_AVG_KG',
                       'LAST_EU_SUM_CNT', 'LAST_EU_SUM_JD', 'LAST_EU_SUM_AVG_JD', 'LAST_EU_CHG_JD'],
            'SG (사고)': ['LAST_SG_CNT', 'LAST_SG_AVG_GYUNGIL', 'LAST_SG_SUM', 'LAST_SG_SUM_AVG_GYUNGIL'],
            'CL (도폐)': ['LAST_CL_CNT', 'LAST_CL_SUM'],
            'SH (출하)': ['LAST_SH_CNT', 'LAST_SH_AVG_KG', 'LAST_SH_SUM', 'LAST_SH_AVG_SUM'],
            'THIS (예정)': ['THIS_GB_SUM', 'THIS_IMSIN_SUM', 'THIS_BM_SUM', 'THIS_EU_SUM',
                          'THIS_VACCINE_SUM', 'THIS_SHIP_SUM'],
            'ALERT (관리대상)': ['ALERT_TOTAL', 'ALERT_HUBO', 'ALERT_EU_MI', 'ALERT_SG_MI', 'ALERT_BM_DELAY', 'ALERT_EU_DELAY'],
            'MODON (두수)': ['MODON_REG_CNT', 'MODON_SANGSI_CNT'],
        }

        all_cols = [col for cols in col_groups.values() for col in cols]
        col_str = ', '.join([f'A.{c} AS BAK_{c}, B.{c} AS ETL_{c}' for c in all_cols])

        # 3. 전체 비교 쿼리
        sql = f"""
            SELECT A.REPORT_WEEK_NO, A.FARM_NO, {col_str}
            FROM TS_INS_WEEK_BAK A
            JOIN TS_INS_WEEK B ON A.FARM_NO = B.FARM_NO
                AND A.REPORT_YEAR = B.REPORT_YEAR
                AND A.REPORT_WEEK_NO = B.REPORT_WEEK_NO
            WHERE A.REPORT_YEAR = :year
              AND A.REPORT_WEEK_NO BETWEEN 45 AND 51
            ORDER BY A.REPORT_WEEK_NO, A.FARM_NO
        """
        cursor.execute(sql, {'year': year})
        rows = cursor.fetchall()

        print(f"\n비교 대상 건수: {len(rows)}건")

        # 4. 그룹별 차이 집계
        group_diffs = {g: {} for g in col_groups}  # {group: {col: [(ww, farm, bak, etl), ...]}}
        total_match = 0
        total_diff = 0

        for row in rows:
            ww = row[0]
            farm_no = row[1]
            col_idx = 2

            for group, cols in col_groups.items():
                for col in cols:
                    bak_val = row[col_idx]
                    etl_val = row[col_idx + 1]
                    col_idx += 2

                    # 비교 (소수점 1자리까지)
                    bak_v = float(bak_val or 0)
                    etl_v = float(etl_val or 0)

                    if abs(bak_v - etl_v) > 0.05:  # 0.1 미만 차이는 무시
                        if col not in group_diffs[group]:
                            group_diffs[group][col] = []
                        group_diffs[group][col].append((ww, farm_no, bak_v, etl_v))
                        total_diff += 1
                    else:
                        total_match += 1

        # 5. 결과 출력
        print("\n" + "=" * 80)
        print("그룹별 차이 분석")
        print("=" * 80)

        for group, cols_diff in group_diffs.items():
            if cols_diff:
                print(f"\n### {group} ###")
                for col, diffs in cols_diff.items():
                    print(f"\n  [{col}] - {len(diffs)}건 차이")
                    # 주차별 집계
                    by_week = {}
                    for ww, farm, bak, etl in diffs:
                        if ww not in by_week:
                            by_week[ww] = []
                        by_week[ww].append((farm, bak, etl))

                    for ww in sorted(by_week.keys()):
                        farms = by_week[ww]
                        print(f"    {ww}주차: {len(farms)}건")
                        for farm, bak, etl in farms[:3]:
                            diff_pct = ((etl - bak) / bak * 100) if bak != 0 else 0
                            print(f"      농장 {farm}: BAK={bak:.1f} vs ETL={etl:.1f} ({diff_pct:+.1f}%)")
                        if len(farms) > 3:
                            print(f"      ... 외 {len(farms) - 3}건")
            else:
                print(f"\n### {group} ### - 모두 일치!")

        print("\n" + "=" * 80)
        print(f"총 비교: {total_match + total_diff}건")
        print(f"  일치: {total_match}건 ({total_match/(total_match+total_diff)*100:.1f}%)")
        print(f"  차이: {total_diff}건 ({total_diff/(total_match+total_diff)*100:.1f}%)")
        print("=" * 80)

        # 6. TS_INS_WEEK_SUB 비교
        print("\n\n" + "=" * 80)
        print("TS_INS_WEEK_SUB vs TS_INS_WEEK_SUB_BAK 비교 (45~51주)")
        print("=" * 80)

        # GUBUN별 비교
        cursor.execute("""
            SELECT A.GUBUN, A.SUB_GUBUN, COUNT(*) AS BAK_CNT,
                   (SELECT COUNT(*) FROM TS_INS_WEEK_SUB S
                    JOIN TS_INS_WEEK W ON S.MASTER_SEQ = W.MASTER_SEQ AND S.FARM_NO = W.FARM_NO
                    WHERE W.REPORT_YEAR = :year AND W.REPORT_WEEK_NO BETWEEN 45 AND 51
                      AND S.GUBUN = A.GUBUN AND NVL(S.SUB_GUBUN, 'NULL') = NVL(A.SUB_GUBUN, 'NULL')) AS ETL_CNT
            FROM TS_INS_WEEK_SUB_BAK A
            JOIN TS_INS_WEEK_BAK W ON A.MASTER_SEQ = W.MASTER_SEQ AND A.FARM_NO = W.FARM_NO
            WHERE W.REPORT_YEAR = :year AND W.REPORT_WEEK_NO BETWEEN 45 AND 51
            GROUP BY A.GUBUN, A.SUB_GUBUN
            ORDER BY A.GUBUN, A.SUB_GUBUN
        """, {'year': year})
        gubun_counts = cursor.fetchall()

        print("\nGUBUN/SUB_GUBUN별 건수 비교:")
        print("-" * 60)
        print(f"{'GUBUN':<10} {'SUB_GUBUN':<15} {'BAK':>8} {'ETL':>8} {'차이':>8}")
        print("-" * 60)

        for gubun, sub_gubun, bak_cnt, etl_cnt in gubun_counts:
            sub = sub_gubun or '-'
            diff = etl_cnt - bak_cnt
            status = 'OK' if diff == 0 else f'{diff:+d}'
            print(f"{gubun:<10} {sub:<15} {bak_cnt:>8} {etl_cnt:>8} {status:>8}")

        # 7. SUB STAT 상세 비교 (주요 컬럼)
        print("\n\n" + "=" * 80)
        print("SUB STAT 상세 비교 (CNT_*, VAL_* 컬럼)")
        print("=" * 80)

        stat_gubuns = ['GB', 'BM', 'EU', 'SG', 'DOPE', 'SHIP', 'CONFIG', 'ALERT', 'SCHEDULE']

        for gubun in stat_gubuns:
            # 각 GUBUN별 CNT/VAL 컬럼 비교
            cursor.execute(f"""
                SELECT A.REPORT_WEEK_NO, A.FARM_NO,
                       B.CNT_1 AS BAK_CNT_1, E.CNT_1 AS ETL_CNT_1,
                       B.CNT_2 AS BAK_CNT_2, E.CNT_2 AS ETL_CNT_2,
                       B.CNT_3 AS BAK_CNT_3, E.CNT_3 AS ETL_CNT_3,
                       B.CNT_4 AS BAK_CNT_4, E.CNT_4 AS ETL_CNT_4,
                       B.CNT_5 AS BAK_CNT_5, E.CNT_5 AS ETL_CNT_5,
                       B.VAL_1 AS BAK_VAL_1, E.VAL_1 AS ETL_VAL_1,
                       B.VAL_2 AS BAK_VAL_2, E.VAL_2 AS ETL_VAL_2,
                       B.VAL_3 AS BAK_VAL_3, E.VAL_3 AS ETL_VAL_3,
                       B.VAL_4 AS BAK_VAL_4, E.VAL_4 AS ETL_VAL_4,
                       B.VAL_5 AS BAK_VAL_5, E.VAL_5 AS ETL_VAL_5
                FROM TS_INS_WEEK_BAK A
                JOIN TS_INS_WEEK_SUB_BAK B ON B.MASTER_SEQ = A.MASTER_SEQ AND B.FARM_NO = A.FARM_NO
                JOIN TS_INS_WEEK C ON C.FARM_NO = A.FARM_NO AND C.REPORT_YEAR = A.REPORT_YEAR
                                  AND C.REPORT_WEEK_NO = A.REPORT_WEEK_NO
                LEFT JOIN TS_INS_WEEK_SUB E ON E.MASTER_SEQ = C.MASTER_SEQ AND E.FARM_NO = C.FARM_NO
                                           AND E.GUBUN = B.GUBUN AND NVL(E.SUB_GUBUN, 'X') = NVL(B.SUB_GUBUN, 'X')
                                           AND NVL(E.SORT_NO, 0) = NVL(B.SORT_NO, 0)
                WHERE A.REPORT_YEAR = :year AND A.REPORT_WEEK_NO BETWEEN 45 AND 51
                  AND B.GUBUN = :gubun AND NVL(B.SUB_GUBUN, 'STAT') = 'STAT'
                ORDER BY A.REPORT_WEEK_NO, A.FARM_NO
            """, {'year': year, 'gubun': gubun})
            rows = cursor.fetchall()

            if not rows:
                continue

            diffs = []
            cols = ['CNT_1', 'CNT_2', 'CNT_3', 'CNT_4', 'CNT_5', 'VAL_1', 'VAL_2', 'VAL_3', 'VAL_4', 'VAL_5']

            for row in rows:
                ww, farm = row[0], row[1]
                col_idx = 2
                for col in cols:
                    bak_v = float(row[col_idx] or 0)
                    etl_v = float(row[col_idx + 1] or 0)
                    col_idx += 2
                    if abs(bak_v - etl_v) > 0.05:
                        diffs.append((ww, farm, col, bak_v, etl_v))

            if diffs:
                print(f"\n[{gubun}/STAT] - {len(diffs)}건 차이")
                # 컬럼별 집계
                by_col = {}
                for ww, farm, col, bak, etl in diffs:
                    if col not in by_col:
                        by_col[col] = []
                    by_col[col].append((ww, farm, bak, etl))

                for col in sorted(by_col.keys()):
                    items = by_col[col]
                    print(f"  {col}: {len(items)}건")
                    for ww, farm, bak, etl in items[:2]:
                        print(f"    {ww}주차 농장 {farm}: BAK={bak:.1f} vs ETL={etl:.1f}")
                    if len(items) > 2:
                        print(f"    ... 외 {len(items) - 2}건")
            else:
                print(f"\n[{gubun}/STAT] - 모두 일치! ({len(rows)}건)")

        cursor.close()

if __name__ == '__main__':
    main()
