"""
Oracle 데이터베이스 연결 관리

연결 풀(Pool) 지원:
- 병렬 처리 시 각 스레드가 독립 연결을 사용
- 연결 풀 크기: min=2, max=10 (설정 가능)
- 스레드별 독립 연결로 thread-safe 보장
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
    """Oracle 데이터베이스 연결 관리 클래스

    병렬 처리를 위해 연결 풀(Pool)을 사용:
    - use_pool=True: 연결 풀 사용 (병렬 처리용)
    - use_pool=False: 단일 연결 사용 (기본, 순차 처리용)
    """

    def __init__(self, config: Optional[Config] = None, use_pool: bool = False,
                 pool_min: int = 2, pool_max: int = 10):
        self.config = config or Config()
        self._connection = None
        self._pool = None
        self.use_pool = use_pool
        self.pool_min = pool_min
        self.pool_max = pool_max
        logger.info(f"Oracle library: {ORACLE_LIB}, use_pool: {use_pool}")

    def _create_pool(self):
        """연결 풀 생성"""
        if self._pool is None:
            db_config = self.config.database
            self._pool = oracledb.SessionPool(
                user=db_config['user'],
                password=db_config['password'],
                dsn=db_config['dsn'],
                min=self.pool_min,
                max=self.pool_max,
                increment=1,
                threaded=True,  # 멀티스레드 지원
                getmode=oracledb.SPOOL_ATTRVAL_WAIT,  # 연결 대기
            )
            logger.info(f"Oracle 연결 풀 생성: min={self.pool_min}, max={self.pool_max}")
        return self._pool

    def connect(self):
        """데이터베이스 연결 (단일 연결 모드)"""
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
        """연결/풀 종료"""
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info("Oracle DB 연결 종료")
        if self._pool:
            self._pool.close()
            self._pool = None
            logger.info("Oracle 연결 풀 종료")

    @contextmanager
    def get_connection(self) -> Generator:
        """컨텍스트 매니저로 연결 관리

        use_pool=True: 풀에서 연결 획득 후 반환 (스레드별 독립 연결)
        use_pool=False: 단일 연결 사용 (기존 방식)
        """
        if self.use_pool:
            # 연결 풀에서 새 연결 획득
            pool = self._create_pool()
            conn = pool.acquire()
            try:
                yield conn
            finally:
                pool.release(conn)
        else:
            # 단일 연결 사용 (기존 방식)
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

    def commit(self) -> None:
        """트랜잭션 커밋"""
        if self._connection:
            self._connection.commit()

    def rollback(self) -> None:
        """트랜잭션 롤백"""
        if self._connection:
            self._connection.rollback()
            logger.debug("트랜잭션 롤백 완료")
