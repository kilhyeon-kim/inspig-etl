#!/usr/bin/env python3
"""
InsightPig 주간 리포트 ETL
- Oracle Job (JOB_INS_WEEKLY_REPORT) 대체
- SP_INS_WEEK_MAIN 프로시저 호출

실행 방법:
    python weekly_report_etl.py              # config.ini 설정대로 실행
    python weekly_report_etl.py --test       # 테스트 모드 (금주 데이터)
    python weekly_report_etl.py --base-date 2024-12-15  # 특정 기준일
"""

import argparse
import configparser
import logging
import os
import sys
from datetime import datetime

# cx_Oracle (운영 서버) 또는 oracledb (로컬 개발) 지원
try:
    import cx_Oracle as oracledb
    ORACLE_LIB = "cx_Oracle"
except ImportError:
    import oracledb
    ORACLE_LIB = "oracledb"


def setup_logger(log_path: str = None) -> logging.Logger:
    """로깅 설정"""
    if log_path is None:
        log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")

    os.makedirs(log_path, exist_ok=True)

    log_file = os.path.join(log_path, f"weekly_{datetime.now():%Y%m%d}.log")

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

    logger = logging.getLogger("inspig_weekly_etl")
    logger.info(f"Oracle library: {ORACLE_LIB}")
    return logger


def load_config() -> configparser.ConfigParser:
    """설정 파일 로드"""
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.ini")

    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"설정 파일을 찾을 수 없습니다: {config_path}\n"
            "config.ini.example을 복사하여 config.ini를 생성하세요."
        )

    config.read(config_path, encoding='utf-8')
    return config


def get_connection(config: configparser.ConfigParser):
    """Oracle DB 연결"""
    dsn = config['database']['dsn']
    user = config['database']['user']
    password = config['database']['password']

    return oracledb.connect(user=user, password=password, dsn=dsn)


def call_procedure(conn, proc_name: str, params: list = None, logger: logging.Logger = None):
    """프로시저 호출"""
    if logger:
        logger.info(f"프로시저 호출: {proc_name}, params={params}")

    with conn.cursor() as cursor:
        cursor.callproc(proc_name, params or [])
        conn.commit()

    if logger:
        logger.info(f"프로시저 완료: {proc_name}")


def parse_args() -> argparse.Namespace:
    """CLI 인자 파싱"""
    parser = argparse.ArgumentParser(
        description='InsightPig 주간 리포트 ETL',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예시:
  python weekly_report_etl.py              # 운영 모드 (config.ini 설정)
  python weekly_report_etl.py --test       # 테스트 모드 (금주 데이터)
  python weekly_report_etl.py --base-date 2024-12-15  # 특정 기준일
        """
    )
    parser.add_argument(
        '--test',
        action='store_true',
        help='테스트 모드 (금주 데이터만 처리, config.ini 설정보다 우선)'
    )
    parser.add_argument(
        '--base-date',
        type=str,
        help='기준일 (YYYY-MM-DD 형식, 미지정 시 현재일)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='실제 실행 없이 설정만 확인'
    )
    return parser.parse_args()


def run_weekly_report():
    """주간 리포트 생성 (기존 Oracle Job 대체)"""
    args = parse_args()
    config = load_config()

    # 로깅 설정
    log_path = config.get('logging', 'log_path', fallback=None)
    logger = setup_logger(log_path)

    # 처리 옵션
    # CLI 옵션이 config.ini보다 우선
    test_mode = 'Y' if args.test else config.get('processing', 'test_mode', fallback='N')
    base_date = args.base_date  # None이면 프로시저에서 현재일 사용
    parallel = int(config.get('processing', 'parallel', fallback='4'))

    logger.info("=" * 60)
    logger.info("InsightPig Weekly ETL 시작")
    logger.info("=" * 60)
    logger.info(f"  테스트 모드: {test_mode}")
    logger.info(f"  기준일: {base_date or '현재일'}")
    logger.info(f"  병렬처리: {parallel}")
    logger.info(f"  Oracle 라이브러리: {ORACLE_LIB}")

    if args.dry_run:
        logger.info("DRY-RUN 모드: 실제 프로시저는 호출하지 않습니다.")
        return True

    try:
        with get_connection(config) as conn:
            logger.info("Oracle DB 연결 성공")

            # SP_INS_WEEK_MAIN 호출
            # 파라미터: P_DAY_GB, P_BASE_DATE, P_PARALLEL, P_TEST_YN
            call_procedure(
                conn,
                'SP_INS_WEEK_MAIN',
                ['WEEK', base_date, parallel, test_mode],
                logger
            )

        logger.info("=" * 60)
        logger.info("Weekly ETL 완료")
        logger.info("=" * 60)
        return True

    except Exception as e:
        logger.error(f"Weekly ETL 실패: {e}", exc_info=True)
        raise


if __name__ == '__main__':
    try:
        success = run_weekly_report()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
