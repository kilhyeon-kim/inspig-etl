"""
Oracle 데이터베이스 연결 관리
"""
import logging
from contextlib import contextmanager
from typing import Any, Generator, List, Optional

# cx_Oracle (운영 서버) 또는 oracledb (로컬 개발) 지원
try:
    import cx_Oracle as oracledb
    ORACLE_LIB = "cx_Oracle"
except ImportError:
    import oracledb
    ORACLE_LIB = "oracledb"

from .config import Config

logger = logging.getLogger(__name__)


class Database:
    """Oracle 데이터베이스 연결 관리 클래스"""

    def __init__(self, config: Optional[Config] = None):
        self.config = config or Config()
        self._connection = None
        logger.info(f"Oracle library: {ORACLE_LIB}")

    def connect(self):
        """데이터베이스 연결"""
        if self._connection is None:
            db_config = self.config.database
            self._connection = oracledb.connect(
                user=db_config['user'],
                password=db_config['password'],
                dsn=db_config['dsn']
            )
            logger.info("Oracle DB 연결 성공")
        return self._connection

    def close(self):
        """연결 종료"""
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info("Oracle DB 연결 종료")

    @contextmanager
    def get_connection(self) -> Generator:
        """컨텍스트 매니저로 연결 관리"""
        try:
            conn = self.connect()
            yield conn
        finally:
            self.close()

    @contextmanager
    def get_cursor(self) -> Generator:
        """커서 컨텍스트 매니저"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                yield cursor
            finally:
                cursor.close()

    def execute(self, sql: str, params: Optional[dict] = None) -> None:
        """SQL 실행 (INSERT, UPDATE, DELETE)"""
        with self.get_cursor() as cursor:
            cursor.execute(sql, params or {})
            self._connection.commit()

    def execute_many(self, sql: str, params_list: List[dict]) -> None:
        """배치 SQL 실행"""
        with self.get_cursor() as cursor:
            cursor.executemany(sql, params_list)
            self._connection.commit()

    def fetch_all(self, sql: str, params: Optional[dict] = None) -> List[tuple]:
        """전체 결과 조회"""
        with self.get_cursor() as cursor:
            cursor.execute(sql, params or {})
            return cursor.fetchall()

    def fetch_one(self, sql: str, params: Optional[dict] = None) -> Optional[tuple]:
        """단일 결과 조회"""
        with self.get_cursor() as cursor:
            cursor.execute(sql, params or {})
            return cursor.fetchone()

    def fetch_dict(self, sql: str, params: Optional[dict] = None) -> List[dict]:
        """딕셔너리 형태로 결과 조회"""
        with self.get_cursor() as cursor:
            cursor.execute(sql, params or {})
            columns = [col[0] for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def call_procedure(self, proc_name: str, params: Optional[list] = None) -> None:
        """프로시저 호출"""
        logger.info(f"프로시저 호출: {proc_name}, params={params}")
        with self.get_cursor() as cursor:
            cursor.callproc(proc_name, params or [])
            self._connection.commit()
        logger.info(f"프로시저 완료: {proc_name}")

    def call_function(self, func_name: str, return_type: Any, params: Optional[list] = None) -> Any:
        """함수 호출"""
        logger.info(f"함수 호출: {func_name}, params={params}")
        with self.get_cursor() as cursor:
            result = cursor.callfunc(func_name, return_type, params or [])
            self._connection.commit()
        logger.info(f"함수 완료: {func_name}, result={result}")
        return result
