#!/usr/bin/env python3
"""
InsightPig 기상청 날씨 데이터 수집 ETL
- 1시간마다 실행 (crontab: 0 * * * *)
- TA_FARM의 격자 좌표 기반으로 기상청 단기예보 API 호출
- TM_WEATHER (일별), TM_WEATHER_HOURLY (시간별) 테이블에 저장

수집 데이터:
    [1] 단기예보 (getVilageFcst): 오늘~+3일 예보 (IS_FORECAST='Y')
    [2] 초단기실황 (getUltraSrtNcst): 현재 시각 실측 (IS_FORECAST='N')
    [3] ASOS 일자료: 어제까지 실측 (IS_FORECAST='N') - --asos 옵션

실행 방법:
    python weather_etl.py                      # 기본 실행 (예보 + 실황)
    python weather_etl.py --asos               # ASOS 일자료도 함께 수집
    python weather_etl.py --asos --asos-days 14        # ASOS 14일치 수집
    python weather_etl.py --asos --asos-start 20250101 --asos-end 20250107  # 기간 지정
    python weather_etl.py --update-grid        # 농장 격자 좌표 업데이트 후 실행
    python weather_etl.py --update-asos        # 농장 ASOS 관측소 매핑 업데이트 후 실행
    python weather_etl.py --grid-only          # 격자 좌표만 업데이트 (날씨 수집 안함)
    python weather_etl.py --dry-run            # 실행 없이 설정 확인

스케줄 (crontab):
    # 매 시간: 예보 + 실황
    0 * * * * cd /data/etl/inspig && ./venv/bin/python weather_etl.py >> logs/weather_cron.log 2>&1
    # 매일 자정: ASOS 일자료 포함
    0 0 * * * cd /data/etl/inspig && ./venv/bin/python weather_etl.py --asos >> logs/weather_cron.log 2>&1

참고:
    - 단기예보: 오늘~+3일 예보 데이터
    - 초단기실황: 현재 시각 기준 실측 데이터 (격자 기반)
    - ASOS: 관측소 기반 일별 실측 데이터 (TM_WEATHER_ASOS 테이블)
"""

import argparse
import logging
import os
import sys
from datetime import datetime

# 프로젝트 경로 추가
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.common import Config, Database, setup_logger
from src.collectors.weather import WeatherCollector, update_farm_weather_grid, update_farm_asos_mapping


def parse_args() -> argparse.Namespace:
    """CLI 인자 파싱"""
    parser = argparse.ArgumentParser(
        description='InsightPig 기상청 날씨 데이터 수집 ETL',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예시:
  python weather_etl.py                      # 기본 실행 (예보 + 실황)
  python weather_etl.py --asos               # ASOS 일자료도 함께 수집 (기본 7일)
  python weather_etl.py --asos --asos-days 14        # ASOS 14일치 수집
  python weather_etl.py --asos --asos-start 20250101 --asos-end 20250107  # 기간 지정
  python weather_etl.py --update-grid        # 농장 격자 좌표 업데이트 후 실행
  python weather_etl.py --update-asos        # 농장 ASOS 관측소 매핑 업데이트 후 실행
  python weather_etl.py --grid-only          # 격자 좌표만 업데이트 (날씨 수집 안함)
  python weather_etl.py --dry-run            # 실행 없이 설정 확인
        """
    )
    parser.add_argument(
        '--asos',
        action='store_true',
        help='ASOS 일자료(관측소 기반 실측)도 함께 수집'
    )
    parser.add_argument(
        '--asos-days',
        type=int,
        default=7,
        metavar='N',
        help='ASOS 조회 일수 (기본 7일, --asos-start/end 미지정시 사용)'
    )
    parser.add_argument(
        '--asos-start',
        type=str,
        metavar='YYYYMMDD',
        help='ASOS 시작일 (지정시 --asos-days 무시)'
    )
    parser.add_argument(
        '--asos-end',
        type=str,
        metavar='YYYYMMDD',
        help='ASOS 종료일 (지정시 --asos-days 무시)'
    )
    parser.add_argument(
        '--update-grid',
        action='store_true',
        help='실행 전 TA_FARM의 WEATHER_NX_N/NY_N 격자 좌표 업데이트'
    )
    parser.add_argument(
        '--update-asos',
        action='store_true',
        help='실행 전 TA_FARM의 ASOS_STN_ID 관측소 매핑 업데이트'
    )
    parser.add_argument(
        '--grid-only',
        action='store_true',
        help='격자 좌표만 업데이트하고 날씨 데이터 수집은 하지 않음'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='실제 실행 없이 설정만 확인'
    )
    return parser.parse_args()


def run_weather_etl():
    """기상청 날씨 데이터 수집"""
    args = parse_args()
    config = Config()

    # 로깅 설정
    log_path = config.logging.get('log_path')
    logger = setup_logger("weather_etl", log_path)

    logger.info("=" * 60)
    logger.info("InsightPig Weather ETL 시작")
    logger.info("=" * 60)
    logger.info(f"  실행 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"  ASOS 수집: {'Y' if args.asos else 'N'}")
    if args.asos:
        if args.asos_start and args.asos_end:
            logger.info(f"  ASOS 기간: {args.asos_start} ~ {args.asos_end}")
        else:
            logger.info(f"  ASOS 일수: {args.asos_days}일")
    logger.info(f"  격자 업데이트: {'Y' if args.update_grid or args.grid_only else 'N'}")
    logger.info(f"  ASOS 매핑 업데이트: {'Y' if args.update_asos else 'N'}")
    logger.info(f"  격자만 업데이트: {'Y' if args.grid_only else 'N'}")

    if args.dry_run:
        logger.info("DRY-RUN 모드: 실제 수집은 하지 않습니다.")
        return True

    try:
        db = Database(config)

        # 격자 좌표 업데이트 (옵션)
        if args.update_grid or args.grid_only:
            logger.info("농장 격자 좌표 업데이트 시작...")
            updated = update_farm_weather_grid(db)
            logger.info(f"농장 격자 좌표 업데이트: {updated}건")

            # --grid-only 옵션이면 여기서 종료
            if args.grid_only:
                logger.info("=" * 60)
                logger.info("격자 좌표 업데이트 완료 (날씨 수집 생략)")
                logger.info("=" * 60)
                db.close()
                return True

        # ASOS 관측소 매핑 업데이트 (옵션)
        if args.update_asos:
            logger.info("농장 ASOS 관측소 매핑 업데이트 시작...")
            updated = update_farm_asos_mapping(db)
            logger.info(f"농장 ASOS 매핑 업데이트: {updated}건")

        # 날씨 데이터 수집
        collector = WeatherCollector(config, db)
        result = collector.run(
            collect_asos=args.asos,
            asos_days_back=args.asos_days,
            asos_start_dt=args.asos_start,
            asos_end_dt=args.asos_end
        )

        logger.info("=" * 60)
        logger.info("Weather ETL 완료:")
        logger.info(f"  예보 일별: {result['daily']}건")
        logger.info(f"  예보 시간별: {result['hourly']}건")
        logger.info(f"  초단기실황: {result['ncst']}건")
        if args.asos:
            logger.info(f"  ASOS 일자료: {result.get('asos', 0)}건")
        logger.info("=" * 60)

        db.close()
        return True

    except Exception as e:
        logger.error(f"Weather ETL 실패: {e}", exc_info=True)
        raise


if __name__ == '__main__':
    try:
        success = run_weather_etl()
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
