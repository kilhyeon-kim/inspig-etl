"""
농장별 처리 모듈
SP_INS_WEEK_FARM_PROCESS 프로시저 Python 전환

역할:
- 단일 농장에 대한 주간 리포트 생성
- FarmDataLoader로 모든 원시 데이터 1회 로드
- 각 프로세서에 로드된 데이터 전달
- 오류 발생 시 해당 농장만 ERROR 상태로 기록

아키텍처 v2:
- Oracle 의존도 최소화
- 데이터 조회 1회 → Python 가공 → 결과 저장
"""
import logging
import hashlib
import secrets
import time
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from ..common import now_kst
from .data_loader import FarmDataLoader
from .processors import (
    ConfigProcessor,
    ModonProcessor,
    AlertProcessor,
    MatingProcessor,
    FarrowingProcessor,
    WeaningProcessor,
    AccidentProcessor,
    CullingProcessor,
    ShipmentProcessor,
    ScheduleProcessor,
)

logger = logging.getLogger(__name__)


class FarmProcessor:
    """농장별 주간 리포트 처리 클래스

    SP_INS_WEEK_FARM_PROCESS 프로시저의 Python 버전
    """

    def __init__(self, conn, master_seq: int, farm_no: int, locale: str = 'KOR'):
        """
        Args:
            conn: Oracle DB 연결 객체
            master_seq: 마스터 시퀀스
            farm_no: 농장 번호
            locale: 로케일 (KOR, VNM 등)
        """
        self.conn = conn
        self.master_seq = master_seq
        self.farm_no = farm_no
        self.locale = locale
        self.logger = logging.getLogger(f"{__name__}.Farm{farm_no}")

    def process(
        self,
        dt_from: str,
        dt_to: str,
        national_price: int = 0,
    ) -> Dict[str, Any]:
        """농장 주간 리포트 생성

        Args:
            dt_from: 시작일 (YYYYMMDD)
            dt_to: 종료일 (YYYYMMDD)
            national_price: 전국 탕박 평균 단가

        Returns:
            처리 결과 딕셔너리
        """
        self.logger.info(f"농장 처리 시작: {self.farm_no}, 기간={dt_from}~{dt_to}")

        try:
            # 1. 기존 데이터 삭제 (재실행 대비)
            self._delete_existing_data()

            # 2. TS_INS_WEEK 상태 업데이트 (RUNNING)
            self._update_status('RUNNING')

            # ========================================
            # v2: FarmDataLoader로 모든 데이터 1회 로드
            # ========================================
            self.logger.info(f"데이터 로드 시작: {self.farm_no}")
            data_loader = FarmDataLoader(
                conn=self.conn,
                farm_no=self.farm_no,
                dt_from=dt_from,
                dt_to=dt_to,
                locale=self.locale,
            )
            data_loader.load()
            self.logger.info(f"데이터 로드 완료: {self.farm_no}")

            # 3. 각 프로세서 순차 실행 (data_loader 전달)
            results = {}
            farm_start_time = time.time()

            # 프로세서 목록 정의
            processors = [
                ('ConfigProcessor', ConfigProcessor, {'dt_from': dt_from, 'dt_to': dt_to}),
                ('AlertProcessor', AlertProcessor, {'dt_from': dt_from, 'dt_to': dt_to}),
                ('ModonProcessor', ModonProcessor, {'dt_from': dt_from, 'dt_to': dt_to}),
                ('MatingProcessor', MatingProcessor, {'dt_from': dt_from, 'dt_to': dt_to}),
                ('FarrowingProcessor', FarrowingProcessor, {'dt_from': dt_from, 'dt_to': dt_to}),
                ('WeaningProcessor', WeaningProcessor, {'dt_from': dt_from, 'dt_to': dt_to}),
                ('AccidentProcessor', AccidentProcessor, {'dt_from': dt_from, 'dt_to': dt_to}),
                ('CullingProcessor', CullingProcessor, {'dt_from': dt_from, 'dt_to': dt_to}),
                ('ShipmentProcessor', ShipmentProcessor, {'dt_from': dt_from, 'dt_to': dt_to, 'national_price': national_price}),
            ]

            # 금주 예정용 날짜 범위
            this_dt_from = (datetime.strptime(dt_to, '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')
            this_dt_to = (datetime.strptime(dt_to, '%Y%m%d') + timedelta(days=7)).strftime('%Y%m%d')
            processors.append(
                ('ScheduleProcessor', ScheduleProcessor, {'dt_from': this_dt_from, 'dt_to': this_dt_to})
            )

            # 각 프로세서 실행 및 시간 측정
            for proc_name, proc_class, proc_kwargs in processors:
                proc_start = time.time()
                try:
                    proc = proc_class(
                        self.conn, self.master_seq, self.farm_no, self.locale,
                        data_loader=data_loader
                    )
                    result = proc.process(**proc_kwargs)
                    elapsed_ms = int((time.time() - proc_start) * 1000)

                    # 결과 저장
                    result_key = proc_name.replace('Processor', '').lower()
                    results[result_key] = result

                    # 정상 처리 로그 기록
                    self._log_success(proc_name, elapsed_ms)
                    self.logger.debug(f"{proc_name} 완료: {elapsed_ms}ms")

                except Exception as proc_error:
                    elapsed_ms = int((time.time() - proc_start) * 1000)
                    self._log_processor_error(proc_name, str(proc_error), elapsed_ms)
                    raise  # 상위로 전파

            # 4. 상태 업데이트 (COMPLETE) + 공유 토큰 생성
            self._update_complete()

            self.conn.commit()

            self.logger.info(f"농장 처리 완료: {self.farm_no}")

            return {
                'status': 'success',
                'farm_no': self.farm_no,
                'results': results,
            }

        except Exception as e:
            self.logger.error(f"농장 처리 실패: {self.farm_no}, 오류: {e}", exc_info=True)

            # 상태 업데이트 (ERROR)
            self._update_status('ERROR')

            # 오류 로그 기록
            self._log_error(str(e))

            self.conn.commit()

            return {
                'status': 'error',
                'farm_no': self.farm_no,
                'error': str(e),
            }

    def _delete_existing_data(self) -> None:
        """기존 데이터 삭제 (재실행 대비)"""
        cursor = self.conn.cursor()
        try:
            # TS_INS_WEEK_SUB 삭제
            cursor.execute("""
                DELETE FROM TS_INS_WEEK_SUB
                WHERE MASTER_SEQ = :master_seq AND FARM_NO = :farm_no
            """, {'master_seq': self.master_seq, 'farm_no': self.farm_no})

            self.logger.debug(f"기존 SUB 데이터 삭제: {cursor.rowcount}건")

        finally:
            cursor.close()

    def _update_status(self, status: str) -> None:
        """TS_INS_WEEK 상태 업데이트"""
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                UPDATE TS_INS_WEEK
                SET STATUS_CD = :status
                WHERE MASTER_SEQ = :master_seq AND FARM_NO = :farm_no
            """, {'status': status, 'master_seq': self.master_seq, 'farm_no': self.farm_no})
        finally:
            cursor.close()

    def _update_complete(self) -> None:
        """완료 상태 업데이트 + 공유 토큰 생성"""
        cursor = self.conn.cursor()
        try:
            # 토큰 생성 (한국 시간 기준)
            kst_now = now_kst()
            token_data = f"{self.master_seq}-{self.farm_no}-{kst_now.strftime('%Y%m%d%H%M%S')}-{secrets.token_hex(16)}"
            share_token = hashlib.sha256(token_data.encode()).hexdigest().lower()

            # ETL 수행일 + 6일 = 수행일 포함 7일간 열람 가능 (한국 시간 기준)
            expire_dt = (kst_now + timedelta(days=6)).strftime('%Y%m%d')

            cursor.execute("""
                UPDATE TS_INS_WEEK
                SET STATUS_CD = 'COMPLETE',
                    SHARE_TOKEN = :share_token,
                    TOKEN_EXPIRE_DT = :expire_dt
                WHERE MASTER_SEQ = :master_seq AND FARM_NO = :farm_no
            """, {
                'share_token': share_token,
                'expire_dt': expire_dt,
                'master_seq': self.master_seq,
                'farm_no': self.farm_no,
            })
        finally:
            cursor.close()

    def _get_master_info(self) -> tuple:
        """마스터 정보 조회 (연도, 주차)"""
        cursor = self.conn.cursor()
        try:
            cursor.execute("""
                SELECT REPORT_YEAR, REPORT_WEEK_NO
                FROM TS_INS_MASTER
                WHERE SEQ = :master_seq
            """, {'master_seq': self.master_seq})
            row = cursor.fetchone()
            return (row[0] if row else 0, row[1] if row else 0)
        finally:
            cursor.close()

    def _log_success(self, proc_name: str, elapsed_ms: int) -> None:
        """정상 처리 로그 기록 (TS_INS_JOB_LOG)

        Args:
            proc_name: 프로세서 이름
            elapsed_ms: 소요시간 (밀리초)
        """
        cursor = self.conn.cursor()
        try:
            year, week_no = self._get_master_info()

            cursor.execute("""
                INSERT INTO TS_INS_JOB_LOG (
                    SEQ, MASTER_SEQ, FARM_NO, JOB_NM, PROC_NM,
                    DAY_GB, REPORT_YEAR, REPORT_WEEK_NO,
                    STATUS_CD, ELAPSED_MS,
                    LOG_INS_DT, START_DT, END_DT
                ) VALUES (
                    SEQ_TS_INS_JOB_LOG.NEXTVAL,
                    :master_seq, :farm_no, 'PYTHON_ETL', :proc_name,
                    'WEEK', :year, :week_no,
                    'SUCCESS', :elapsed_ms,
                    SYSDATE, SYSDATE, SYSDATE
                )
            """, {
                'master_seq': self.master_seq,
                'farm_no': self.farm_no,
                'proc_name': proc_name,
                'year': year,
                'week_no': week_no,
                'elapsed_ms': elapsed_ms,
            })
        finally:
            cursor.close()

    def _log_processor_error(self, proc_name: str, error_msg: str, elapsed_ms: int) -> None:
        """프로세서별 오류 로그 기록 (TS_INS_JOB_LOG)

        Args:
            proc_name: 프로세서 이름
            error_msg: 오류 메시지
            elapsed_ms: 소요시간 (밀리초)
        """
        cursor = self.conn.cursor()
        try:
            year, week_no = self._get_master_info()

            cursor.execute("""
                INSERT INTO TS_INS_JOB_LOG (
                    SEQ, MASTER_SEQ, FARM_NO, JOB_NM, PROC_NM,
                    DAY_GB, REPORT_YEAR, REPORT_WEEK_NO,
                    STATUS_CD, ELAPSED_MS, ERROR_MSG,
                    LOG_INS_DT, START_DT, END_DT
                ) VALUES (
                    SEQ_TS_INS_JOB_LOG.NEXTVAL,
                    :master_seq, :farm_no, 'PYTHON_ETL', :proc_name,
                    'WEEK', :year, :week_no,
                    'ERROR', :elapsed_ms, :error_msg,
                    SYSDATE, SYSDATE, SYSDATE
                )
            """, {
                'master_seq': self.master_seq,
                'farm_no': self.farm_no,
                'proc_name': proc_name,
                'year': year,
                'week_no': week_no,
                'elapsed_ms': elapsed_ms,
                'error_msg': error_msg[:4000],
            })
        finally:
            cursor.close()

    def _log_error(self, error_msg: str) -> None:
        """농장 전체 오류 로그 기록 (TS_INS_JOB_LOG)"""
        cursor = self.conn.cursor()
        try:
            year, week_no = self._get_master_info()

            cursor.execute("""
                INSERT INTO TS_INS_JOB_LOG (
                    SEQ, MASTER_SEQ, FARM_NO, JOB_NM, PROC_NM,
                    DAY_GB, REPORT_YEAR, REPORT_WEEK_NO,
                    STATUS_CD, ERROR_MSG,
                    LOG_INS_DT, START_DT, END_DT
                ) VALUES (
                    SEQ_TS_INS_JOB_LOG.NEXTVAL,
                    :master_seq, :farm_no, 'PYTHON_ETL', 'FarmProcessor',
                    'WEEK', :year, :week_no,
                    'ERROR', :error_msg,
                    SYSDATE, SYSDATE, SYSDATE
                )
            """, {
                'master_seq': self.master_seq,
                'farm_no': self.farm_no,
                'year': year,
                'week_no': week_no,
                'error_msg': error_msg[:4000],
            })
        finally:
            cursor.close()
