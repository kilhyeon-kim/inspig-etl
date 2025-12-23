#!/usr/bin/env python3
"""ETL 결과와 백업 데이터 비교 스크립트"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from src.common import Config, Database

def main():
    config = Config()
    db = Database(config)

    with db.get_connection() as conn:
        cursor = conn.cursor()

        # 1. 에러 로그 확인
        print("=" * 60)
        print("1. TS_INS_JOB_LOG 에러 확인")
        print("=" * 60)
        cursor.execute("""
            SELECT MASTER_SEQ, FARM_NO, JOB_NM, STATUS_CD, ERROR_MSG, LOG_INS_DT
            FROM TS_INS_JOB_LOG
            WHERE STATUS_CD = 'ERROR'
            ORDER BY LOG_INS_DT DESC
            FETCH FIRST 10 ROWS ONLY
        """)
        rows = cursor.fetchall()
        if rows:
            for row in rows:
                print(f"  SEQ={row[0]}, FARM={row[1]}, JOB={row[2]}, STATUS={row[3]}")
                print(f"    ERROR: {row[4][:100] if row[4] else 'N/A'}...")
        else:
            print("  에러 로그 없음")

        # 2. 생성된 마스터 확인
        print("\n" + "=" * 60)
        print("2. TS_INS_MASTER 생성 현황")
        print("=" * 60)
        cursor.execute("""
            SELECT SEQ, REPORT_YEAR, REPORT_WEEK_NO, STATUS_CD,
                   TARGET_CNT, COMPLETE_CNT, ERROR_CNT,
                   TO_CHAR(LOG_INS_DT, 'YYYY-MM-DD HH24:MI:SS') AS INS_DT
            FROM TS_INS_MASTER
            ORDER BY SEQ DESC
            FETCH FIRST 10 ROWS ONLY
        """)
        rows = cursor.fetchall()
        for row in rows:
            print(f"  SEQ={row[0]}, {row[1]}년 {row[2]}주, STATUS={row[3]}, "
                  f"대상={row[4]}, 완료={row[5]}, 오류={row[6]}, {row[7]}")

        # 3. 주차별 TS_INS_WEEK 건수 확인
        print("\n" + "=" * 60)
        print("3. TS_INS_WEEK 주차별 건수")
        print("=" * 60)
        cursor.execute("""
            SELECT M.REPORT_YEAR, M.REPORT_WEEK_NO, COUNT(*) AS CNT
            FROM TS_INS_WEEK W
            JOIN TS_INS_MASTER M ON W.MASTER_SEQ = M.SEQ
            GROUP BY M.REPORT_YEAR, M.REPORT_WEEK_NO
            ORDER BY M.REPORT_YEAR, M.REPORT_WEEK_NO
        """)
        rows = cursor.fetchall()
        for row in rows:
            print(f"  {row[0]}년 {row[1]}주: {row[2]}건")

        # 4. 백업 테이블 존재 확인 및 건수
        print("\n" + "=" * 60)
        print("4. 백업 테이블 확인")
        print("=" * 60)

        # TS_INS_WEEK_BAK 존재 확인
        cursor.execute("""
            SELECT COUNT(*) FROM USER_TABLES WHERE TABLE_NAME = 'TS_INS_WEEK_BAK'
        """)
        bak_exists = cursor.fetchone()[0] > 0

        if bak_exists:
            cursor.execute("SELECT COUNT(*) FROM TS_INS_WEEK_BAK")
            week_bak_cnt = cursor.fetchone()[0]
            print(f"  TS_INS_WEEK_BAK: {week_bak_cnt}건")

            cursor.execute("""
                SELECT COUNT(*) FROM USER_TABLES WHERE TABLE_NAME = 'TS_INS_WEEK_SUB_BAK'
            """)
            sub_bak_exists = cursor.fetchone()[0] > 0

            if sub_bak_exists:
                cursor.execute("SELECT COUNT(*) FROM TS_INS_WEEK_SUB_BAK")
                sub_bak_cnt = cursor.fetchone()[0]
                print(f"  TS_INS_WEEK_SUB_BAK: {sub_bak_cnt}건")
        else:
            print("  백업 테이블이 없습니다.")

        # 5. TS_INS_WEEK vs TS_INS_WEEK_BAK 비교 (농장별, 주차별)
        # 백업 테이블은 별도 MASTER_SEQ 사용 -> REPORT_YEAR, REPORT_WEEK_NO로 직접 비교
        if bak_exists:
            print("\n" + "=" * 60)
            print("5. TS_INS_WEEK vs TS_INS_WEEK_BAK 주요 컬럼 비교")
            print("=" * 60)

            # 비교할 컬럼들
            compare_cols = [
                'MODON_REG_CNT', 'MODON_SANGSI_CNT',
                'LAST_GB_CNT', 'LAST_BM_CNT', 'LAST_BM_TOTAL', 'LAST_BM_LIVE',
                'LAST_EU_CNT', 'LAST_EU_JD_CNT',
                'LAST_SG_CNT', 'LAST_CL_CNT',
                'KPI_PSY', 'KPI_DELAY_DAY'
            ]

            # 주차별, 농장별 비교
            cursor.execute("""
                SELECT W.REPORT_YEAR, W.REPORT_WEEK_NO, W.FARM_NO
                FROM TS_INS_WEEK W
                JOIN TS_INS_MASTER M ON W.MASTER_SEQ = M.SEQ
                WHERE M.REPORT_YEAR = 2025
                ORDER BY W.REPORT_WEEK_NO, W.FARM_NO
            """)
            etl_farms = cursor.fetchall()

            total_diff = 0
            for year, week_no, farm_no in etl_farms:
                # ETL 결과 (현재 테이블)
                sql = f"""
                    SELECT {', '.join(compare_cols)}
                    FROM TS_INS_WEEK W
                    WHERE W.REPORT_YEAR = :year AND W.REPORT_WEEK_NO = :week_no AND W.FARM_NO = :farm_no
                """
                cursor.execute(sql, {'year': year, 'week_no': week_no, 'farm_no': farm_no})
                etl_row = cursor.fetchone()

                # 백업 데이터 (REPORT_YEAR, REPORT_WEEK_NO, FARM_NO로 직접 매칭)
                sql = f"""
                    SELECT {', '.join(compare_cols)}
                    FROM TS_INS_WEEK_BAK W
                    WHERE W.REPORT_YEAR = :year AND W.REPORT_WEEK_NO = :week_no AND W.FARM_NO = :farm_no
                """
                cursor.execute(sql, {'year': year, 'week_no': week_no, 'farm_no': farm_no})
                bak_row = cursor.fetchone()

                if etl_row and bak_row:
                    diffs = []
                    for i, col in enumerate(compare_cols):
                        etl_val = etl_row[i] or 0
                        bak_val = bak_row[i] or 0
                        # float 비교를 위해 소수점 2자리까지 비교
                        if isinstance(etl_val, float) or isinstance(bak_val, float):
                            if abs(float(etl_val) - float(bak_val)) > 0.01:
                                diffs.append(f"{col}: ETL={etl_val:.2f}, BAK={bak_val:.2f}")
                        elif etl_val != bak_val:
                            diffs.append(f"{col}: ETL={etl_val}, BAK={bak_val}")

                    if diffs:
                        print(f"\n  [{week_no}주차 농장 {farm_no}] 차이 발견!")
                        for d in diffs:
                            print(f"    {d}")
                        total_diff += len(diffs)
                    else:
                        print(f"  [{week_no}주차 농장 {farm_no}] 일치")
                elif not bak_row:
                    print(f"  [{week_no}주차 농장 {farm_no}] 백업 데이터 없음")

            print(f"\n총 차이 건수: {total_diff}")

        # 6. TS_INS_WEEK_SUB vs TS_INS_WEEK_SUB_BAK 전체 비교
        # TS_INS_WEEK_BAK의 MASTER_SEQ/FARM_NO로 추적하여 비교
        if bak_exists and sub_bak_exists:
            print("\n" + "=" * 60)
            print("6. TS_INS_WEEK_SUB vs TS_INS_WEEK_SUB_BAK 전체 비교")
            print("=" * 60)

            # 백업 테이블의 주차별, 농장별 MASTER_SEQ 매핑 조회
            cursor.execute("""
                SELECT MASTER_SEQ, FARM_NO, REPORT_YEAR, REPORT_WEEK_NO
                FROM TS_INS_WEEK_BAK
                WHERE REPORT_YEAR = 2025 AND REPORT_WEEK_NO BETWEEN 45 AND 47
                ORDER BY REPORT_WEEK_NO, FARM_NO
            """)
            bak_week_records = cursor.fetchall()
            print(f"  비교 대상: {len(bak_week_records)}건 (TS_INS_WEEK_BAK 45~47주)")

            # 현재 ETL 테이블의 MASTER_SEQ 매핑 조회
            cursor.execute("""
                SELECT W.MASTER_SEQ, W.FARM_NO, W.REPORT_YEAR, W.REPORT_WEEK_NO
                FROM TS_INS_WEEK W
                WHERE W.REPORT_YEAR = 2025 AND W.REPORT_WEEK_NO BETWEEN 45 AND 47
                ORDER BY W.REPORT_WEEK_NO, W.FARM_NO
            """)
            etl_week_records = cursor.fetchall()

            # ETL MASTER_SEQ 매핑 (FARM_NO, WEEK_NO) -> MASTER_SEQ
            etl_master_map = {(r[1], r[3]): r[0] for r in etl_week_records}

            total_sub_diff = 0
            total_sub_match = 0

            for bak_master_seq, farm_no, year, week_no in bak_week_records:
                # 현재 ETL의 MASTER_SEQ 찾기
                etl_master_seq = etl_master_map.get((farm_no, week_no))

                if not etl_master_seq:
                    print(f"\n  [{week_no}주차 농장 {farm_no}] ETL 데이터 없음 (MASTER_SEQ 매핑 실패)")
                    continue

                # ETL SUB 데이터 (현재 테이블) - GUBUN별 건수
                cursor.execute("""
                    SELECT GUBUN, COUNT(*) FROM TS_INS_WEEK_SUB
                    WHERE MASTER_SEQ = :master_seq AND FARM_NO = :farm_no
                    GROUP BY GUBUN ORDER BY GUBUN
                """, {'master_seq': etl_master_seq, 'farm_no': farm_no})
                etl_gubun = {r[0]: r[1] for r in cursor.fetchall()}

                # 백업 SUB 데이터 (백업 MASTER_SEQ 사용)
                cursor.execute("""
                    SELECT GUBUN, COUNT(*) FROM TS_INS_WEEK_SUB_BAK
                    WHERE MASTER_SEQ = :master_seq AND FARM_NO = :farm_no
                    GROUP BY GUBUN ORDER BY GUBUN
                """, {'master_seq': bak_master_seq, 'farm_no': farm_no})
                bak_gubun = {r[0]: r[1] for r in cursor.fetchall()}

                # GUBUN별 비교
                all_gubuns = set(etl_gubun.keys()) | set(bak_gubun.keys())
                diffs = []
                for gubun in sorted(all_gubuns):
                    e = etl_gubun.get(gubun, 0)
                    b = bak_gubun.get(gubun, 0)
                    if e != b:
                        diffs.append(f"{gubun}: ETL={e}, BAK={b}")

                if diffs:
                    print(f"\n  [{week_no}주차 농장 {farm_no}] GUBUN 차이 발견!")
                    print(f"    ETL MASTER_SEQ={etl_master_seq}, BAK MASTER_SEQ={bak_master_seq}")
                    for d in diffs:
                        print(f"    {d}")
                    total_sub_diff += len(diffs)
                else:
                    etl_total = sum(etl_gubun.values())
                    bak_total = sum(bak_gubun.values())
                    print(f"  [{week_no}주차 농장 {farm_no}] 일치 (ETL={etl_total}건, BAK={bak_total}건)")
                    total_sub_match += 1

            print(f"\n  SUB 테이블 비교 결과 (GUBUN별 건수):")
            print(f"    일치: {total_sub_match}건")
            print(f"    차이: {total_sub_diff}건")
            print("\n  ※ ETL은 핵심 STAT 데이터만 저장, Oracle은 상세 ROW/SCATTER 포함")

        # 7. SUB STAT 데이터 상세 비교 (핵심 통계 데이터) - 모든 컬럼 비교
        if bak_exists and sub_bak_exists:
            print("\n" + "=" * 60)
            print("7. TS_INS_WEEK_SUB STAT 데이터 상세 비교 (전체 컬럼)")
            print("=" * 60)

            stat_match = 0
            stat_diff = 0

            # 비교 컬럼 정의 (SHIP/STAT 기준)
            compare_columns = [
                'SORT_NO', 'CNT_1', 'CNT_2', 'CNT_3', 'CNT_4', 'CNT_5', 'CNT_6',
                'VAL_1', 'VAL_2', 'VAL_3', 'VAL_4', 'VAL_5',
                'STR_1', 'STR_2'
            ]
            col_str = ', '.join(compare_columns)

            for bak_master_seq, farm_no, year, week_no in bak_week_records:
                etl_master_seq = etl_master_map.get((farm_no, week_no))
                if not etl_master_seq:
                    continue

                # STAT 데이터만 비교
                stat_gubuns = ['SHIP', 'SG', 'DOPE', 'GB', 'BM', 'EU']
                diffs = []

                for gubun in stat_gubuns:
                    cursor.execute(f"""
                        SELECT {col_str}
                        FROM TS_INS_WEEK_SUB
                        WHERE MASTER_SEQ = :master_seq AND FARM_NO = :farm_no
                          AND GUBUN = :gubun AND SUB_GUBUN = 'STAT'
                        ORDER BY SORT_NO
                    """, {'master_seq': etl_master_seq, 'farm_no': farm_no, 'gubun': gubun})
                    etl_rows = cursor.fetchall()

                    cursor.execute(f"""
                        SELECT {col_str}
                        FROM TS_INS_WEEK_SUB_BAK
                        WHERE MASTER_SEQ = :master_seq AND FARM_NO = :farm_no
                          AND GUBUN = :gubun AND SUB_GUBUN = 'STAT'
                        ORDER BY SORT_NO
                    """, {'master_seq': bak_master_seq, 'farm_no': farm_no, 'gubun': gubun})
                    bak_rows = cursor.fetchall()

                    if len(etl_rows) != len(bak_rows):
                        diffs.append(f"{gubun}/STAT: 건수 다름 (ETL={len(etl_rows)}, BAK={len(bak_rows)})")
                    else:
                        for i, (etl_r, bak_r) in enumerate(zip(etl_rows, bak_rows)):
                            # 모든 컬럼 비교
                            for j, col_name in enumerate(compare_columns):
                                e_val = etl_r[j] if etl_r[j] is not None else 0
                                b_val = bak_r[j] if bak_r[j] is not None else 0

                                # 문자열 비교
                                if col_name.startswith('STR_'):
                                    e_val = str(e_val or '').strip()
                                    b_val = str(b_val or '').strip()
                                    if e_val != b_val:
                                        diffs.append(f"{gubun}/STAT.{col_name}: ETL='{e_val}', BAK='{b_val}'")
                                # 숫자 비교
                                else:
                                    try:
                                        if abs(float(e_val) - float(b_val)) > 0.01:
                                            if isinstance(e_val, float) or isinstance(b_val, float):
                                                diffs.append(f"{gubun}/STAT.{col_name}: ETL={e_val:.2f}, BAK={b_val:.2f}")
                                            else:
                                                diffs.append(f"{gubun}/STAT.{col_name}: ETL={e_val}, BAK={b_val}")
                                    except (ValueError, TypeError):
                                        if str(e_val) != str(b_val):
                                            diffs.append(f"{gubun}/STAT.{col_name}: ETL={e_val}, BAK={b_val}")

                if diffs:
                    print(f"\n  [{week_no}주차 농장 {farm_no}] STAT 차이:")
                    for d in diffs[:10]:  # 최대 10개만 출력
                        print(f"    {d}")
                    if len(diffs) > 10:
                        print(f"    ... 외 {len(diffs)-10}건")
                    stat_diff += 1
                else:
                    stat_match += 1

            print(f"\n  STAT 데이터 비교 결과:")
            print(f"    일치: {stat_match}건")
            print(f"    차이: {stat_diff}건")

        # 8. SHIP/STAT 상세 비교 (VAL_4 내농장단가 확인)
        if bak_exists and sub_bak_exists:
            print("\n" + "=" * 60)
            print("8. SHIP/STAT 상세 비교 (내농장단가 VAL_4 확인)")
            print("=" * 60)

            for bak_master_seq, farm_no, year, week_no in bak_week_records:
                etl_master_seq = etl_master_map.get((farm_no, week_no))
                if not etl_master_seq:
                    continue

                # ETL SHIP/STAT
                cursor.execute("""
                    SELECT CNT_1, CNT_2, CNT_3, CNT_4, CNT_5, CNT_6,
                           VAL_1, VAL_2, VAL_3, VAL_4, VAL_5,
                           STR_1, STR_2
                    FROM TS_INS_WEEK_SUB
                    WHERE MASTER_SEQ = :master_seq AND FARM_NO = :farm_no
                      AND GUBUN = 'SHIP' AND SUB_GUBUN = 'STAT'
                """, {'master_seq': etl_master_seq, 'farm_no': farm_no})
                etl_ship = cursor.fetchone()

                # BAK SHIP/STAT
                cursor.execute("""
                    SELECT CNT_1, CNT_2, CNT_3, CNT_4, CNT_5, CNT_6,
                           VAL_1, VAL_2, VAL_3, VAL_4, VAL_5,
                           STR_1, STR_2
                    FROM TS_INS_WEEK_SUB_BAK
                    WHERE MASTER_SEQ = :master_seq AND FARM_NO = :farm_no
                      AND GUBUN = 'SHIP' AND SUB_GUBUN = 'STAT'
                """, {'master_seq': bak_master_seq, 'farm_no': farm_no})
                bak_ship = cursor.fetchone()

                if etl_ship and bak_ship:
                    col_names = ['CNT_1(출하두수)', 'CNT_2(누계)', 'CNT_3(1등급+)',
                                 'CNT_4(출하일령)', 'CNT_5(포유기간)', 'CNT_6(역산일)',
                                 'VAL_1(1등급율)', 'VAL_2(평균체중)', 'VAL_3(등지방)',
                                 'VAL_4(내농장단가)', 'VAL_5(전국단가)',
                                 'STR_1(이유FROM)', 'STR_2(이유TO)']

                    print(f"\n  [{week_no}주차 농장 {farm_no}] SHIP/STAT:")
                    for i, col in enumerate(col_names):
                        e_val = etl_ship[i] if etl_ship[i] is not None else '-'
                        b_val = bak_ship[i] if bak_ship[i] is not None else '-'
                        match = "[O]" if str(e_val) == str(b_val) or (
                            isinstance(e_val, (int, float)) and isinstance(b_val, (int, float)) and
                            abs(float(e_val or 0) - float(b_val or 0)) < 0.01
                        ) else "[X]"
                        print(f"    {match} {col}: ETL={e_val}, BAK={b_val}")
                else:
                    if not etl_ship:
                        print(f"  [{week_no}주차 농장 {farm_no}] ETL SHIP/STAT 없음")
                    if not bak_ship:
                        print(f"  [{week_no}주차 농장 {farm_no}] BAK SHIP/STAT 없음")

        cursor.close()

if __name__ == '__main__':
    main()
