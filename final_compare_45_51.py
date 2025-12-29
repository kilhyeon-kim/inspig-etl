#!/usr/bin/env python3
"""ETL 결과와 Oracle 프로시저 결과(BAK) 상세 비교 리포트 (45~51주)"""

import sys
import os
from pathlib import Path

# 프로젝트 루트를 path에 추가
sys.path.insert(0, str(Path(__file__).parent))

from src.common import Config, Database

def main():
    config = Config()
    db = Database(config)
    
    output_file = "final_diff_report_45_51.md"
    
    with db.get_connection() as conn:
        cursor = conn.cursor()
        
        # 연도 확인 (2024년 또는 2025년)
        cursor.execute("SELECT DISTINCT REPORT_YEAR FROM TS_INS_WEEK_BAK WHERE REPORT_WEEK_NO BETWEEN 45 AND 51")
        years = [row[0] for row in cursor.fetchall()]
        year = 2024 if 2024 in years else 2025
            
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(f"# ETL vs Oracle 프로시저 결과 비교 리포트 ({year}년 45~51주)\n\n")
            f.write(f"- 생성일시: {os.popen('date /t').read().strip()} {os.popen('time /t').read().strip()}\n")
            f.write(f"- 기준(Ground Truth): Oracle 프로시저 결과 (`_BAK` 테이블)\n")
            f.write(f"- 비교대상: Python ETL 결과 (현재 테이블)\n\n")

            # 1. TS_INS_MASTER 비교
            f.write("## 1. 마스터 정보 (TS_INS_MASTER) 비교\n\n")
            f.write("| 주차 | 구분 | 항목 | BAK (프로시저) | ETL (Python) | 차이 |\n")
            f.write("| :--- | :--- | :--- | ---: | ---: | ---: |\n")
            
            master_sql = f"""
                SELECT A.REPORT_WEEK_NO, A.DAY_GB, 
                       A.TARGET_CNT AS BAK_TARGET, B.TARGET_CNT AS ETL_TARGET,
                       A.COMPLETE_CNT AS BAK_COMP, B.COMPLETE_CNT AS ETL_COMP,
                       A.ERROR_CNT AS BAK_ERR, B.ERROR_CNT AS ETL_ERR
                FROM TS_INS_MASTER_BAK A
                JOIN TS_INS_MASTER B ON A.REPORT_YEAR = B.REPORT_YEAR 
                    AND A.REPORT_WEEK_NO = B.REPORT_WEEK_NO 
                    AND A.DAY_GB = B.DAY_GB
                WHERE A.REPORT_YEAR = :year AND A.REPORT_WEEK_NO BETWEEN 45 AND 51
                ORDER BY A.REPORT_WEEK_NO
            """
            cursor.execute(master_sql, {'year': year})
            m_rows = cursor.fetchall()
            for r in m_rows:
                ww, gb, b_t, e_t, b_c, e_c, b_e, e_e = r
                if b_t != e_t: f.write(f"| {ww} | {gb} | TARGET_CNT | {b_t} | {e_t} | {e_t-b_t:+d} |\n")
                if b_c != e_c: f.write(f"| {ww} | {gb} | COMPLETE_CNT | {b_c} | {e_c} | {e_c-b_c:+d} |\n")
                if b_e != e_e: f.write(f"| {ww} | {gb} | ERROR_CNT | {b_e} | {e_e} | {e_e-b_e:+d} |\n")
            
            f.write("\n")

            # 2. TS_INS_WEEK 상세 차이 (농장별/컬럼별)
            f.write("## 2. 주간 리포트 메인 (TS_INS_WEEK) 상세 차이\n\n")
            
            col_groups = {
                '두수/현황': ['MODON_REG_CNT', 'MODON_SANGSI_CNT', 'ALERT_TOTAL'],
                '교배(GB)': ['LAST_GB_CNT', 'LAST_GB_SUM'],
                '분만(BM)': ['LAST_BM_CNT', 'LAST_BM_TOTAL', 'LAST_BM_LIVE', 'LAST_BM_DEAD', 'LAST_BM_MUMMY'],
                '이유(EU)': ['LAST_EU_CNT', 'LAST_EU_JD_CNT', 'LAST_EU_AVG_JD'],
                '사고(SG)': ['LAST_SG_CNT', 'LAST_SG_SUM', 'LAST_SG_AVG_GYUNGIL'],
                '도폐(CL)': ['LAST_CL_CNT', 'LAST_CL_SUM'],
                '출하(SH)': ['LAST_SH_CNT', 'LAST_SH_SUM', 'LAST_SH_AVG_KG']
            }
            
            all_cols = [col for cols in col_groups.values() for col in cols]
            col_select = ', '.join([f'A.{c} AS BAK_{c}, B.{c} AS ETL_{c}' for c in all_cols])
            
            sql = f"""
                SELECT A.REPORT_WEEK_NO, A.FARM_NO, A.FARM_NM, {col_select}
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
            
            f.write("| 주차 | 농장 | 항목 | BAK (프로시저) | ETL (Python) | 차이 | % |\n")
            f.write("| :--- | :--- | :--- | ---: | ---: | ---: | ---: |\n")
            
            for row in rows:
                ww, farm_no, farm_nm = row[0], row[1], row[2]
                col_idx = 3
                for group, cols in col_groups.items():
                    for col in cols:
                        bak_val = float(row[col_idx] or 0)
                        etl_val = float(row[col_idx + 1] or 0)
                        col_idx += 2
                        
                        if abs(bak_val - etl_val) > 0.05:
                            diff = etl_val - bak_val
                            pct = (diff / bak_val * 100) if bak_val != 0 else 0
                            f.write(f"| {ww} | {farm_nm}({farm_no}) | {col} | {bak_val:.1f} | {etl_val:.1f} | {diff:+.1f} | {pct:+.1f}% |\n")

            f.write("\n")

            # 3. TS_INS_WEEK_SUB GUBUN별 건수 비교
            f.write("## 3. 상세 데이터 (TS_INS_WEEK_SUB) 건수 비교\n\n")
            f.write("| GUBUN | SUB_GUBUN | BAK 건수 | ETL 건수 | 차이 |\n")
            f.write("| :--- | :--- | ---: | ---: | ---: |\n")
            
            sub_cnt_sql = f"""
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
            """
            cursor.execute(sub_cnt_sql, {'year': year})
            for r in cursor.fetchall():
                gubun, sub, b_c, e_c = r
                diff = e_c - b_c
                f.write(f"| {gubun} | {sub or '-'} | {b_c} | {e_c} | {diff:+d} |\n")

            f.write("\n")

            # 4. TS_INS_WEEK_SUB STAT 수치 비교
            f.write("## 4. 상세 데이터 (STAT) 수치 비교\n\n")
            f.write("| 주차 | 농장 | 구분 | 컬럼 | BAK | ETL | 차이 |\n")
            f.write("| :--- | :--- | :--- | :--- | ---: | ---: | ---: |\n")
            
            stat_cols = ['CNT_1', 'CNT_2', 'CNT_3', 'VAL_1', 'VAL_2']
            for gubun in ['GB', 'BM', 'EU', 'SG', 'DOPE', 'SHIP']:
                cursor.execute(f"""
                    SELECT A.REPORT_WEEK_NO, A.FARM_NO, A.FARM_NM,
                           B.CNT_1, E.CNT_1, B.CNT_2, E.CNT_2, B.CNT_3, E.CNT_3,
                           B.VAL_1, E.VAL_1, B.VAL_2, E.VAL_2
                    FROM TS_INS_WEEK_BAK A
                    JOIN TS_INS_WEEK_SUB_BAK B ON B.MASTER_SEQ = A.MASTER_SEQ AND B.FARM_NO = A.FARM_NO
                    JOIN TS_INS_WEEK C ON C.FARM_NO = A.FARM_NO AND C.REPORT_YEAR = A.REPORT_YEAR 
                                      AND C.REPORT_WEEK_NO = A.REPORT_WEEK_NO
                    LEFT JOIN TS_INS_WEEK_SUB E ON E.MASTER_SEQ = C.MASTER_SEQ AND E.FARM_NO = C.FARM_NO
                                               AND E.GUBUN = B.GUBUN AND NVL(E.SUB_GUBUN, 'STAT') = 'STAT'
                    WHERE A.REPORT_YEAR = :year AND A.REPORT_WEEK_NO BETWEEN 45 AND 51
                      AND B.GUBUN = :gubun AND NVL(B.SUB_GUBUN, 'STAT') = 'STAT'
                    ORDER BY A.REPORT_WEEK_NO, A.FARM_NO
                """, {'year': year, 'gubun': gubun})
                
                for row in cursor.fetchall():
                    ww, f_no, f_nm = row[0], row[1], row[2]
                    idx = 3
                    for c_nm in stat_cols:
                        b_v, e_v = float(row[idx] or 0), float(row[idx+1] or 0)
                        idx += 2
                        if abs(b_v - e_v) > 0.05:
                            f.write(f"| {ww} | {f_nm}({f_no}) | {gubun} | {c_nm} | {b_v:.1f} | {e_v:.1f} | {e_v-b_v:+.1f} |\n")

        print(f"최종 리포트 생성 완료: {output_file}")

if __name__ == '__main__':
    main()
