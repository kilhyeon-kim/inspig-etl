"""
기본 프로세서 클래스
모든 주간 리포트 프로세서의 기반 클래스

아키텍처 v2:
- FarmDataLoader에서 모든 원시 데이터를 1회 로드
- 각 프로세서는 로드된 데이터를 받아서 Python으로 가공
- Oracle 의존도 최소화 (INSERT/UPDATE만 수행)

Thread-safety:
- db_lock: 병렬 실행 시 DB 작업 동기화용
- 동일 connection을 여러 프로세서가 공유할 때 사용
"""
import logging
import threading
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ..data_loader import FarmDataLoader

logger = logging.getLogger(__name__)


class BaseProcessor(ABC):
    """주간 리포트 프로세서 기본 클래스

    공통 기능:
    - 사전 로드된 데이터 수신 (FarmDataLoader)
    - Python 기반 데이터 가공
    - 로깅
    - 에러 처리
    - SUB 테이블 저장 (INSERT만)

    v2 변경사항:
    - data_loader: FarmDataLoader 인스턴스를 통해 원시 데이터 접근
    - _data: 로드된 데이터 캐시
    - SQL 조회 제거, Python 가공으로 전환
    """

    # 서브클래스에서 정의할 프로세서 이름
    PROC_NAME: str = 'BaseProcessor'

    def __init__(self, conn, master_seq: int, farm_no: int, locale: str = 'KOR',
                 data_loader: Optional['FarmDataLoader'] = None,
                 db_lock: Optional[threading.Lock] = None):
        """
        Args:
            conn: Oracle DB 연결 객체
            master_seq: 마스터 시퀀스
            farm_no: 농장 번호
            locale: 로케일 (KOR, VNM 등)
            data_loader: FarmDataLoader 인스턴스 (사전 로드된 데이터)
            db_lock: DB 작업 동기화용 Lock (병렬 실행 시 필요)
        """
        self.conn = conn
        self.master_seq = master_seq
        self.farm_no = farm_no
        self.locale = locale
        self.data_loader = data_loader
        self.db_lock = db_lock  # 병렬 실행 시 DB 작업 동기화용
        self._data: Dict[str, Any] = {}  # 로드된 데이터 캐시
        self.logger = logging.getLogger(f"{__name__}.{self.PROC_NAME}")

    @abstractmethod
    def process(self, dt_from: str, dt_to: str, **kwargs) -> Dict[str, Any]:
        """프로세서 실행

        Args:
            dt_from: 시작일 (YYYYMMDD)
            dt_to: 종료일 (YYYYMMDD)
            **kwargs: 추가 파라미터

        Returns:
            처리 결과 딕셔너리
        """
        pass

    def _with_db_lock(self, func):
        """DB Lock을 적용하여 함수 실행

        병렬 실행 시 DB 작업 동기화를 위한 헬퍼 메서드
        """
        if self.db_lock:
            with self.db_lock:
                return func()
        return func()

    def save_sub(self, sub_type: str, data: Dict[str, Any]) -> int:
        """TS_INS_WEEK_SUB 테이블에 데이터 저장

        Args:
            sub_type: 서브 타입 (MODON_STAT, GB_LIST 등)
            data: JSON으로 저장할 데이터

        Returns:
            생성된 SEQ
        """
        import json

        def _execute():
            cursor = self.conn.cursor()
            try:
                # 시퀀스 값 조회
                cursor.execute("SELECT SEQ_TS_INS_WEEK_SUB.NEXTVAL FROM DUAL")
                seq = cursor.fetchone()[0]

                # INSERT
                sql = """
                INSERT INTO TS_INS_WEEK_SUB (
                    SEQ, MASTER_SEQ, FARM_NO, SUB_TYPE, JSON_DATA, INS_DT
                ) VALUES (
                    :seq, :master_seq, :farm_no, :sub_type, :json_data, SYSDATE
                )
                """
                cursor.execute(sql, {
                    'seq': seq,
                    'master_seq': self.master_seq,
                    'farm_no': self.farm_no,
                    'sub_type': sub_type,
                    'json_data': json.dumps(data, ensure_ascii=False, default=str),
                })

                self.logger.debug(f"SUB 저장: {sub_type} (SEQ={seq})")
                return seq

            finally:
                cursor.close()

        return self._with_db_lock(_execute)

    def save_subs(self, items: List[Dict[str, Any]]) -> int:
        """TS_INS_WEEK_SUB 테이블에 여러 데이터 저장

        Args:
            items: [{'sub_type': 'TYPE', 'data': {...}}, ...]

        Returns:
            저장된 레코드 수
        """
        count = 0
        for item in items:
            self.save_sub(item['sub_type'], item['data'])
            count += 1
        return count

    def update_week(self, updates: Dict[str, Any]) -> None:
        """TS_INS_WEEK 테이블 컬럼 업데이트

        Args:
            updates: 업데이트할 컬럼과 값
        """
        if not updates:
            return

        def _execute():
            cursor = self.conn.cursor()
            try:
                # SET 절 생성
                set_clause = ', '.join([f"{k} = :{k}" for k in updates.keys()])
                sql = f"""
                UPDATE TS_INS_WEEK
                SET {set_clause}
                WHERE MASTER_SEQ = :master_seq AND FARM_NO = :farm_no
                """

                params = dict(updates)
                params['master_seq'] = self.master_seq
                params['farm_no'] = self.farm_no

                cursor.execute(sql, params)
                self.logger.debug(f"WEEK 업데이트: {list(updates.keys())}")

            finally:
                cursor.close()

        self._with_db_lock(_execute)

    def fetch_all(self, sql: str, params: Optional[Dict] = None) -> List[tuple]:
        """SELECT 쿼리 실행 후 전체 결과 반환"""
        def _execute():
            cursor = self.conn.cursor()
            try:
                cursor.execute(sql, params or {})
                return cursor.fetchall()
            finally:
                cursor.close()

        return self._with_db_lock(_execute)

    def fetch_dict(self, sql: str, params: Optional[Dict] = None) -> List[Dict]:
        """SELECT 쿼리 실행 후 딕셔너리 리스트로 반환"""
        def _execute():
            cursor = self.conn.cursor()
            try:
                cursor.execute(sql, params or {})
                columns = [col[0] for col in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
            finally:
                cursor.close()

        return self._with_db_lock(_execute)

    def fetch_one(self, sql: str, params: Optional[Dict] = None) -> Optional[tuple]:
        """SELECT 쿼리 실행 후 단일 결과 반환"""
        def _execute():
            cursor = self.conn.cursor()
            try:
                cursor.execute(sql, params or {})
                return cursor.fetchone()
            finally:
                cursor.close()

        return self._with_db_lock(_execute)

    def execute(self, sql: str, params: Optional[Dict] = None) -> int:
        """INSERT/UPDATE/DELETE 실행 후 영향받은 행 수 반환"""
        def _execute():
            cursor = self.conn.cursor()
            try:
                cursor.execute(sql, params or {})
                return cursor.rowcount
            finally:
                cursor.close()

        return self._with_db_lock(_execute)

    # ========================================
    # Python 데이터 가공 헬퍼 메서드 (v2)
    # ========================================

    def get_loaded_data(self) -> Dict[str, Any]:
        """FarmDataLoader에서 로드된 데이터 반환"""
        if self.data_loader:
            return self.data_loader.get_data()
        return {}

    def filter_by_period(self, data: List[Dict], date_field: str,
                         dt_from: str, dt_to: str) -> List[Dict]:
        """기간으로 데이터 필터링

        Args:
            data: 필터링할 데이터 리스트
            date_field: 날짜 필드명 (예: 'WK_DT', 'BUN_DT')
            dt_from: 시작일 (YYYYMMDD)
            dt_to: 종료일 (YYYYMMDD)

        Returns:
            필터링된 데이터 리스트
        """
        return [
            row for row in data
            if row.get(date_field) and dt_from <= str(row[date_field]) <= dt_to
        ]

    def filter_by_code(self, data: List[Dict], code_field: str, code_value: str) -> List[Dict]:
        """코드 값으로 데이터 필터링

        Args:
            data: 필터링할 데이터 리스트
            code_field: 코드 필드명 (예: 'WK_CD', 'SAGO_GUBUN_CD')
            code_value: 코드 값 (예: '050001')

        Returns:
            필터링된 데이터 리스트
        """
        return [row for row in data if str(row.get(code_field, '')) == code_value]

    def filter_by_codes(self, data: List[Dict], code_field: str, code_values: List[str]) -> List[Dict]:
        """여러 코드 값으로 데이터 필터링

        Args:
            data: 필터링할 데이터 리스트
            code_field: 코드 필드명
            code_values: 코드 값 리스트

        Returns:
            필터링된 데이터 리스트
        """
        return [row for row in data if str(row.get(code_field, '')) in code_values]

    def group_by(self, data: List[Dict], key_field: str) -> Dict[str, List[Dict]]:
        """데이터를 특정 필드로 그룹핑

        Args:
            data: 그룹핑할 데이터 리스트
            key_field: 그룹핑 키 필드명

        Returns:
            그룹핑된 딕셔너리
        """
        result: Dict[str, List[Dict]] = {}
        for row in data:
            key = str(row.get(key_field, ''))
            if key not in result:
                result[key] = []
            result[key].append(row)
        return result

    def group_by_multi(self, data: List[Dict], key_fields: List[str]) -> Dict[tuple, List[Dict]]:
        """여러 필드로 그룹핑

        Args:
            data: 그룹핑할 데이터 리스트
            key_fields: 그룹핑 키 필드명 리스트

        Returns:
            그룹핑된 딕셔너리 (키는 튜플)
        """
        result: Dict[tuple, List[Dict]] = {}
        for row in data:
            key = tuple(str(row.get(f, '')) for f in key_fields)
            if key not in result:
                result[key] = []
            result[key].append(row)
        return result

    def count(self, data: List[Dict]) -> int:
        """데이터 개수 집계"""
        return len(data)

    def sum_field(self, data: List[Dict], field: str) -> float:
        """필드 합계 집계"""
        return sum(row.get(field, 0) or 0 for row in data)

    def avg_field(self, data: List[Dict], field: str) -> float:
        """필드 평균 집계"""
        values = [row.get(field, 0) or 0 for row in data]
        return sum(values) / len(values) if values else 0

    def min_field(self, data: List[Dict], field: str) -> Any:
        """필드 최소값"""
        values = [row.get(field) for row in data if row.get(field) is not None]
        return min(values) if values else None

    def max_field(self, data: List[Dict], field: str) -> Any:
        """필드 최대값"""
        values = [row.get(field) for row in data if row.get(field) is not None]
        return max(values) if values else None

    def count_by_code(self, data: List[Dict], code_field: str) -> Dict[str, int]:
        """코드별 개수 집계

        Args:
            data: 집계할 데이터 리스트
            code_field: 코드 필드명

        Returns:
            코드별 개수 딕셔너리
        """
        result: Dict[str, int] = {}
        for row in data:
            code = str(row.get(code_field, ''))
            result[code] = result.get(code, 0) + 1
        return result

    def sum_by_code(self, data: List[Dict], code_field: str, value_field: str) -> Dict[str, float]:
        """코드별 합계 집계

        Args:
            data: 집계할 데이터 리스트
            code_field: 코드 필드명
            value_field: 합계할 필드명

        Returns:
            코드별 합계 딕셔너리
        """
        result: Dict[str, float] = {}
        for row in data:
            code = str(row.get(code_field, ''))
            value = row.get(value_field, 0) or 0
            result[code] = result.get(code, 0) + value
        return result

    def calculate_date_diff(self, date1: str, date2: str) -> int:
        """두 날짜 간의 일수 차이 계산

        Args:
            date1: 첫번째 날짜 (YYYYMMDD 또는 YYYY-MM-DD)
            date2: 두번째 날짜 (YYYYMMDD 또는 YYYY-MM-DD)

        Returns:
            일수 차이 (date2 - date1)
        """
        if not date1 or not date2:
            return 0

        # 날짜 형식 통일
        d1_str = str(date1).replace('-', '')[:8]
        d2_str = str(date2).replace('-', '')[:8]

        try:
            d1 = datetime.strptime(d1_str, '%Y%m%d')
            d2 = datetime.strptime(d2_str, '%Y%m%d')
            return (d2 - d1).days
        except ValueError:
            return 0

    def add_days(self, date_str: str, days: int) -> str:
        """날짜에 일수 더하기

        Args:
            date_str: 날짜 (YYYYMMDD)
            days: 더할 일수

        Returns:
            결과 날짜 (YYYYMMDD)
        """
        if not date_str:
            return ''

        d_str = str(date_str).replace('-', '')[:8]
        try:
            d = datetime.strptime(d_str, '%Y%m%d')
            result = d + timedelta(days=days)
            return result.strftime('%Y%m%d')
        except ValueError:
            return ''

    def pivot_data(self, data: List[Dict], row_key: str, col_key: str,
                   value_field: str, agg: str = 'sum') -> Dict[str, Dict[str, Any]]:
        """데이터 피벗

        Args:
            data: 피벗할 데이터
            row_key: 행 키 필드
            col_key: 열 키 필드
            value_field: 값 필드
            agg: 집계 방식 ('sum', 'count', 'first')

        Returns:
            피벗된 딕셔너리 {row: {col: value}}
        """
        result: Dict[str, Dict[str, Any]] = {}

        for row in data:
            r_key = str(row.get(row_key, ''))
            c_key = str(row.get(col_key, ''))
            value = row.get(value_field, 0) or 0

            if r_key not in result:
                result[r_key] = {}

            if agg == 'sum':
                result[r_key][c_key] = result[r_key].get(c_key, 0) + value
            elif agg == 'count':
                result[r_key][c_key] = result[r_key].get(c_key, 0) + 1
            elif agg == 'first':
                if c_key not in result[r_key]:
                    result[r_key][c_key] = value

        return result

    def sort_data(self, data: List[Dict], sort_field: str, reverse: bool = False) -> List[Dict]:
        """데이터 정렬

        Args:
            data: 정렬할 데이터
            sort_field: 정렬 필드
            reverse: 역순 정렬 여부

        Returns:
            정렬된 데이터
        """
        return sorted(data, key=lambda x: x.get(sort_field, ''), reverse=reverse)

    def top_n(self, data: List[Dict], n: int, sort_field: str, reverse: bool = True) -> List[Dict]:
        """상위 N개 데이터 추출

        Args:
            data: 데이터
            n: 추출할 개수
            sort_field: 정렬 필드
            reverse: 역순 정렬 여부 (기본 True = 내림차순)

        Returns:
            상위 N개 데이터
        """
        sorted_data = self.sort_data(data, sort_field, reverse)
        return sorted_data[:n]
