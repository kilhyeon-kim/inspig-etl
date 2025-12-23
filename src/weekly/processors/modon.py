"""
모돈현황 팝업 데이터 추출 프로세서
SP_INS_WEEK_MODON_POPUP 프로시저 Python 전환

아키텍처 v2:
- FarmDataLoader에서 로드된 데이터를 Python으로 가공
- SQL 조회 제거, INSERT/UPDATE만 수행
- Oracle 의존도 최소화

역할:
- 산차별 × 상태별 모돈 현황 교차표 데이터 생성
- TS_INS_WEEK_SUB (GUBUN='MODON') 저장
- TS_INS_WEEK 테이블에 모돈 합계두수 업데이트
"""
import logging
from typing import Any, Dict, List, Optional

from .base import BaseProcessor

logger = logging.getLogger(__name__)

# 상태코드 상수
STATUS_HUBO = '010001'      # 후보돈
STATUS_IMSIN = '010002'     # 임신돈
STATUS_POYU = '010003'      # 포유돈
STATUS_DAERI = '010004'     # 대리모
STATUS_EUMO = '010005'      # 이유모
STATUS_JAEBAL = '010006'    # 재발
STATUS_YUSAN = '010007'     # 유산

# 산차 구분 정의
PARITY_CONFIG = [
    {'parity': '후보돈', 'sort_no': 1, 'sancha': 0, 'is_hubo': True},
    {'parity': '0산', 'sort_no': 2, 'sancha': 0, 'is_hubo': False},
    {'parity': '1산', 'sort_no': 3, 'sancha': 1, 'is_hubo': False},
    {'parity': '2산', 'sort_no': 4, 'sancha': 2, 'is_hubo': False},
    {'parity': '3산', 'sort_no': 5, 'sancha': 3, 'is_hubo': False},
    {'parity': '4산', 'sort_no': 6, 'sancha': 4, 'is_hubo': False},
    {'parity': '5산', 'sort_no': 7, 'sancha': 5, 'is_hubo': False},
    {'parity': '6산', 'sort_no': 8, 'sancha': 6, 'is_hubo': False},
    {'parity': '7산', 'sort_no': 9, 'sancha': 7, 'is_hubo': False},
    {'parity': '8산↑', 'sort_no': 10, 'sancha': 8, 'is_hubo': False},
]


class ModonProcessor(BaseProcessor):
    """모돈현황 팝업 프로세서 (v2 - Python 가공)"""

    PROC_NAME = 'ModonProcessor'

    def process(self, dt_from: str, dt_to: str, **kwargs) -> Dict[str, Any]:
        """모돈현황 데이터 추출

        Args:
            dt_from: 시작일 (YYYYMMDD) - 사용하지 않음
            dt_to: 종료일 (YYYYMMDD) - 기준일

        Returns:
            처리 결과 딕셔너리
        """
        self.logger.info(f"모돈현황 팝업 시작: 농장={self.farm_no}, 기준일={dt_to}")

        # 1. 기존 데이터 삭제
        self._delete_existing()

        # 2. FarmDataLoader에서 현재 살아있는 모돈만 사용
        # data_loader.modon에는 2년 이내 도폐사 모돈도 포함되어 있음
        # 모돈현황은 현재 살아있는 모돈만 집계해야 함
        modon_list = self.data_loader.get_current_modon() if self.data_loader else []

        # 3. 산차별 × 상태별 집계 (Python)
        parity_stats = self._calculate_parity_status(modon_list)

        # 4. TS_INS_WEEK_SUB INSERT
        proc_cnt = self._insert_modon_data(parity_stats)

        # 5. 합계두수 계산 (Python)
        total_cnt, sangsi_cnt = self._calculate_totals(parity_stats)

        # 6. 이전 주차 데이터 조회 및 증감 계산
        prev_data = self._get_previous_data()
        if prev_data:
            self._update_changes_python(parity_stats, prev_data)

        # 7. TS_INS_WEEK 업데이트
        self._update_week_totals(total_cnt, sangsi_cnt, prev_data)

        self.logger.info(f"모돈현황 팝업 완료: 농장={self.farm_no}, 현재={total_cnt}, 상시={sangsi_cnt}")

        return {
            'status': 'success',
            'proc_cnt': proc_cnt,
            'total_cnt': total_cnt,
            'sangsi_cnt': sangsi_cnt,
        }

    def _delete_existing(self) -> None:
        """기존 MODON 데이터 삭제"""
        sql = """
        DELETE FROM TS_INS_WEEK_SUB
        WHERE MASTER_SEQ = :master_seq
          AND FARM_NO = :farm_no
          AND GUBUN = 'MODON'
        """
        self.execute(sql, {'master_seq': self.master_seq, 'farm_no': self.farm_no})

    def _get_parity_label(self, modon: Dict) -> str:
        """모돈의 산차 레이블 결정

        Args:
            modon: 모돈 데이터 딕셔너리

        Returns:
            산차 레이블 (예: '후보돈', '1산', '8산↑')
        """
        # FarmDataLoader에서 계산된 상태코드 사용
        calc_status = modon.get('CALC_STATUS_CD', '')
        last_wk = self.data_loader._modon_last_wk.get(str(modon.get('MODON_NO', ''))) if self.data_loader else None

        # 산차 결정: 마지막 작업의 산차 또는 IN_SANCHA
        if last_wk:
            sancha = last_wk.get('SANCHA') or modon.get('IN_SANCHA', 0) or 0
        else:
            sancha = modon.get('IN_SANCHA', 0) or 0

        # 후보돈 판정: 0산 + 후보돈 상태
        if sancha == 0 and calc_status == STATUS_HUBO:
            return '후보돈'

        # 산차별 레이블
        if sancha == 0:
            return '0산'
        elif sancha == 1:
            return '1산'
        elif sancha == 2:
            return '2산'
        elif sancha == 3:
            return '3산'
        elif sancha == 4:
            return '4산'
        elif sancha == 5:
            return '5산'
        elif sancha == 6:
            return '6산'
        elif sancha == 7:
            return '7산'
        else:
            return '8산↑'

    def _calculate_parity_status(self, modon_list: List[Dict]) -> Dict[str, Dict[str, int]]:
        """산차별 × 상태별 집계

        Args:
            modon_list: FarmDataLoader에서 로드된 모돈 리스트

        Returns:
            산차별 상태 집계 딕셔너리
            {
                '후보돈': {'hubo': 5, 'imsin': 0, 'poyu': 0, 'eumo': 0, 'sago': 0},
                '1산': {'hubo': 0, 'imsin': 10, 'poyu': 5, ...},
                ...
            }
        """
        # 초기화
        result = {}
        for config in PARITY_CONFIG:
            result[config['parity']] = {
                'hubo': 0,
                'imsin': 0,
                'poyu': 0,
                'eumo': 0,
                'sago': 0,
                'sort_no': config['sort_no'],
            }

        # 집계
        for modon in modon_list:
            parity = self._get_parity_label(modon)
            calc_status = modon.get('CALC_STATUS_CD', '')

            if parity not in result:
                continue

            # 상태별 집계
            if calc_status == STATUS_HUBO:
                result[parity]['hubo'] += 1
            elif calc_status == STATUS_IMSIN:
                result[parity]['imsin'] += 1
            elif calc_status in (STATUS_POYU, STATUS_DAERI):
                result[parity]['poyu'] += 1
            elif calc_status == STATUS_EUMO:
                result[parity]['eumo'] += 1
            elif calc_status in (STATUS_JAEBAL, STATUS_YUSAN):
                result[parity]['sago'] += 1

        return result

    def _insert_modon_data(self, parity_stats: Dict[str, Dict[str, int]]) -> int:
        """산차별 × 상태별 모돈 현황 데이터 INSERT

        모든 산차(후보돈, 0산~8산↑)에 대해 행을 생성합니다.
        데이터가 없는 산차도 0으로 채워서 INSERT합니다.
        (Oracle MERGE INTO 로직과 동일)

        Args:
            parity_stats: Python에서 계산한 산차별 상태 집계

        Returns:
            INSERT된 행 수
        """
        insert_count = 0

        sql = """
        INSERT INTO TS_INS_WEEK_SUB (
            MASTER_SEQ, FARM_NO, GUBUN, SORT_NO, CODE_1,
            CNT_1, CNT_2, CNT_3, CNT_4, CNT_5, CNT_6
        ) VALUES (
            :master_seq, :farm_no, 'MODON', :sort_no, :code_1,
            :cnt_1, :cnt_2, :cnt_3, :cnt_4, :cnt_5, 0
        )
        """

        # 모든 산차에 대해 INSERT (데이터 없으면 0으로)
        for config in PARITY_CONFIG:
            parity = config['parity']
            stats = parity_stats.get(parity, {
                'hubo': 0, 'imsin': 0, 'poyu': 0, 'eumo': 0, 'sago': 0
            })

            self.execute(sql, {
                'master_seq': self.master_seq,
                'farm_no': self.farm_no,
                'sort_no': config['sort_no'],
                'code_1': parity,
                'cnt_1': stats.get('hubo', 0),
                'cnt_2': stats.get('imsin', 0),
                'cnt_3': stats.get('poyu', 0),
                'cnt_4': stats.get('eumo', 0),
                'cnt_5': stats.get('sago', 0),
            })
            insert_count += 1

        return insert_count

    def _calculate_totals(self, parity_stats: Dict[str, Dict[str, int]]) -> tuple:
        """현재모돈/상시모돈 합계두수 계산 (Python)

        Args:
            parity_stats: 산차별 상태 집계

        Returns:
            (total_cnt, sangsi_cnt) 튜플
        """
        total_cnt = 0  # 현재모돈 (후보돈 제외)
        sangsi_cnt = 0  # 상시모돈 (후보돈 포함)

        for parity, stats in parity_stats.items():
            row_total = stats['hubo'] + stats['imsin'] + stats['poyu'] + stats['eumo'] + stats['sago']

            sangsi_cnt += row_total

            if parity != '후보돈':
                total_cnt += row_total

        return total_cnt, sangsi_cnt

    def _get_previous_data(self) -> Optional[Dict[str, Any]]:
        """이전 주차 데이터 조회"""
        sql = """
        SELECT SEQ
        FROM (
            SELECT SEQ
            FROM TS_INS_MASTER
            WHERE DAY_GB = 'WEEK'
              AND SEQ < :master_seq
              AND STATUS_CD = 'COMPLETE'
            ORDER BY SEQ DESC
        )
        WHERE ROWNUM = 1
        """
        result = self.fetch_one(sql, {'master_seq': self.master_seq})

        if not result:
            return None

        prev_master_seq = result[0]

        # 이전 주차 합계두수 조회
        sql_prev = """
        SELECT NVL(MODON_REG_CNT, 0), NVL(MODON_SANGSI_CNT, 0)
        FROM TS_INS_WEEK
        WHERE MASTER_SEQ = :master_seq
          AND FARM_NO = :farm_no
        """
        result = self.fetch_one(sql_prev, {
            'master_seq': prev_master_seq,
            'farm_no': self.farm_no,
        })

        if not result:
            return None

        # 이전 주차 산차별 데이터 조회
        sql_prev_parity = """
        SELECT CODE_1, CNT_1, CNT_2, CNT_3, CNT_4, CNT_5
        FROM TS_INS_WEEK_SUB
        WHERE MASTER_SEQ = :master_seq
          AND FARM_NO = :farm_no
          AND GUBUN = 'MODON'
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(sql_prev_parity, {
                'master_seq': prev_master_seq,
                'farm_no': self.farm_no,
            })
            prev_parity_data = {}
            for row in cursor.fetchall():
                code_1 = row[0]
                prev_parity_data[code_1] = {
                    'hubo': row[1] or 0,
                    'imsin': row[2] or 0,
                    'poyu': row[3] or 0,
                    'eumo': row[4] or 0,
                    'sago': row[5] or 0,
                }
        finally:
            cursor.close()

        return {
            'master_seq': prev_master_seq,
            'total_cnt': result[0],
            'sangsi_cnt': result[1],
            'parity_data': prev_parity_data,
        }

    def _update_changes_python(self, parity_stats: Dict[str, Dict[str, int]],
                                prev_data: Dict[str, Any]) -> None:
        """이전 주차 대비 증감 계산하여 UPDATE (Python)

        Args:
            parity_stats: 현재 주차 산차별 집계
            prev_data: 이전 주차 데이터
        """
        prev_parity = prev_data.get('parity_data', {})

        sql = """
        UPDATE TS_INS_WEEK_SUB
        SET CNT_6 = :cnt_6
        WHERE MASTER_SEQ = :master_seq
          AND FARM_NO = :farm_no
          AND GUBUN = 'MODON'
          AND CODE_1 = :code_1
        """

        for parity, stats in parity_stats.items():
            cur_total = stats['hubo'] + stats['imsin'] + stats['poyu'] + stats['eumo'] + stats['sago']

            prev_stats = prev_parity.get(parity, {})
            prev_total = (prev_stats.get('hubo', 0) + prev_stats.get('imsin', 0) +
                         prev_stats.get('poyu', 0) + prev_stats.get('eumo', 0) +
                         prev_stats.get('sago', 0))

            change = cur_total - prev_total

            self.execute(sql, {
                'cnt_6': change,
                'master_seq': self.master_seq,
                'farm_no': self.farm_no,
                'code_1': parity,
            })

    def _update_week_totals(
        self,
        total_cnt: int,
        sangsi_cnt: int,
        prev_data: Optional[Dict[str, Any]],
    ) -> None:
        """TS_INS_WEEK 테이블에 모돈 합계두수 UPDATE"""
        if prev_data:
            sql = """
            UPDATE TS_INS_WEEK
            SET MODON_REG_CNT = :total_cnt,
                MODON_REG_CHG = :total_cnt - :prev_total,
                MODON_SANGSI_CNT = :sangsi_cnt,
                MODON_SANGSI_CHG = :sangsi_cnt - :prev_sangsi
            WHERE MASTER_SEQ = :master_seq
              AND FARM_NO = :farm_no
            """
            self.execute(sql, {
                'total_cnt': total_cnt,
                'prev_total': prev_data['total_cnt'],
                'sangsi_cnt': sangsi_cnt,
                'prev_sangsi': prev_data['sangsi_cnt'],
                'master_seq': self.master_seq,
                'farm_no': self.farm_no,
            })
        else:
            sql = """
            UPDATE TS_INS_WEEK
            SET MODON_REG_CNT = :total_cnt,
                MODON_REG_CHG = NULL,
                MODON_SANGSI_CNT = :sangsi_cnt,
                MODON_SANGSI_CHG = NULL
            WHERE MASTER_SEQ = :master_seq
              AND FARM_NO = :farm_no
            """
            self.execute(sql, {
                'total_cnt': total_cnt,
                'sangsi_cnt': sangsi_cnt,
                'master_seq': self.master_seq,
                'farm_no': self.farm_no,
            })
