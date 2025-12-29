#!/usr/bin/env python3
"""ETL 결과와 백업 데이터 상세 차이 리스트 추출 스크립트 (45~51주)"""

import sys
import os
from pathlib import Path

# 프로젝트 루트를 path에 추가
sys.path.insert(0, str(Path(__file__).parent))

from src.common import Config, Database

def main():
    config = Config()
    db = Database(config)
    
    output_file = "diff_report_45_51.md"
    year = 2024 # 기본값
    
    with db.get_connection() as conn:
        cursor = conn.cursor()
        
        # 연도 확인
        cursor.execute("SELECT DISTINCT REPORT_YEAR FROM TS_INS_WEEK_BAK WHERE REPORT_WEEK_NO BETWEEN 45 AND 51")
        years = [row[0] for row in cursor.fetchall()]
        if 2024 in years:
            year = 2024
        elif 2025 in years:
            year = 2025
            
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(f"# ETL 데이터 비교 상세 리포트 ({year}년 45~51주)\n\n")
            f.write(f"- 생성일시: {os.popen('date /t').read().strip()} {os.popen('time /t').read().strip()}\n")
            f.write(f"- 비교대상: `TS_INS_WEEK` vs `TS_INS_WEEK_BAK` / `TS_INS_WEEK_SUB` vs `TS_INS_WEEK_SUB_BAK` \n\n")

            # 1. TS_INS_WEEK 상세 차이
            f.write("## 1. 메인 테이블 (TS_INS_WEEK) 상세 차이\n\n")
            
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
                'ALERT (관리대상)': ['ALERT_TOTAL', 'ALERT_HUBO', 'ALERT_EU_MI', 'ALERT_SG_MI', 'ALERT_BM_DELAY', 'ALERT_EU_DELAY'],
                'MODON (두수)': ['MODON_REG_CNT', 'MODON_SANGSI_CNT'],
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
            
            f.write("| 주차 | 농장번호 | 농장명 | 컬럼명 | BAK 값 | ETL 값 | 차이 | % |\n")
            f.write("| :--- | :--- | :--- | :--- | ---: | ---: | ---: | ---: |\n")
            
            diff_count = 0
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
                            f.write(f"| {ww} | {farm_no} | {farm_nm} | {col} | {bak_val:.1f} | {etl_val:.1f} | {diff:+.1f} | {pct:+.1f}% |\n")
                            diff_count += 1
            
            if diff_count == 0:
                f.write("| - | - | - | 모두 일치 | - | - | - | - |\n")
            
            f.write(f"\n**총 {diff_count}개의 차이 발견**\n\n")

            # 2. TS_INS_WEEK_SUB 상세 차이 (STAT 위주)
            f.write("## 2. 상세 테이블 (TS_INS_WEEK_SUB) STAT 차이\n\n")
            
            stat_gubuns = ['GB', 'BM', 'EU', 'SG', 'DOPE', 'SHIP', 'CONFIG', 'ALERT', 'SCHEDULE']
            cols = ['CNT_1', 'CNT_2', 'CNT_3', 'CNT_4', 'CNT_5', 'VAL_1', 'VAL_2', 'VAL_3', 'VAL_4', 'VAL_5']
            
            f.write("| 주차 | 농장번호 | 구분 | 컬럼 | BAK 값 | ETL 값 | 차이 |\n")
            f.write("| :--- | :--- | :--- | :--- | ---: | ---: | ---: |\n")
            
            sub_diff_count = 0
            for gubun in stat_gubuns:
                cursor.execute(f"""
                    SELECT A.REPORT_WEEK_NO, A.FARM_NO,
                           B.CNT_1, E.CNT_1, B.CNT_2, E.CNT_2, B.CNT_3, E.CNT_3, B.CNT_4, E.CNT_4, B.CNT_5, E.CNT_5,
                           B.VAL_1, E.VAL_1, B.VAL_2, E.VAL_2, B.VAL_3, E.VAL_3, B.VAL_4, E.VAL_4, B.VAL_5, E.VAL_5
                    FROM TS_INS_WEEK_BAK A
                    JOIN TS_INS_WEEK_SUB_BAK B ON B.MASTER_SEQ = A.MASTER_SEQ AND B.FARM_NO = A.FARM_NO
                    JOIN TS_INS_WEEK C ON C.FARM_NO = A.FARM_NO AND C.REPORT_YEAR = A.REPORT_YEAR 
                                      AND C.REPORT_WEEK_NO = A.REPORT_WEEK_NO
                    LEFT JOIN TS_INS_WEEK_SUB E ON E.MASTER_SEQ = C.MASTER_SEQ AND E.FARM_NO = C.FARM_NO
                                               AND E.GUBUN = B.GUBUN AND NVL(E.SUB_GUBUN, 'STAT') = NVL(B.SUB_GUBUN, 'STAT')
                                               AND NVL(E.SORT_NO, 0) = NVL(B.SORT_NO, 0)
                    WHERE A.REPORT_YEAR = :year AND A.REPORT_WEEK_NO BETWEEN 45 AND 51
                      AND B.GUBUN = :gubun AND NVL(B.SUB_GUBUN, 'STAT') = 'STAT'
                    ORDER BY A.REPORT_WEEK_NO, A.FARM_NO
                """, {'year': year, 'gubun': gubun})
                
                rows = cursor.fetchall()
                for row in rows:
                    ww, farm_no = row[0], row[1]
                    col_idx = 2
                    for col_nm in cols:
                        bak_v = float(row[col_idx] or 0)
                        etl_v = float(row[col_idx + 1] or 0)
                        col_idx += 2
                        
                        if abs(bak_v - etl_v) > 0.05:
                            diff = etl_v - bak_v
                            f.write(f"| {ww} | {farm_no} | {gubun}/STAT | {col_nm} | {bak_v:.1f} | {etl_v:.1f} | {diff:+.1f} |\n")
                            sub_diff_count += 1
                            
            if sub_diff_count == 0:
                f.write("| - | - | - | 모두 일치 | - | - | - |\n")
                
            f.write(f"\n**총 {sub_diff_count}개의 상세 차이 발견**\n")

        print(f"리포트 생성이 완료되었습니다: {output_file}")

if __name__ == '__main__':
    main()
