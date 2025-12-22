#!/usr/bin/env python3
"""
InsightPig ETL 메인 실행 스크립트

실행 방법:
    python run_etl.py              # 기본 실행 (주간 리포트)
    python run_etl.py --test       # 테스트 모드
    python run_etl.py --dry-run    # 설정 확인만
    python run_etl.py weather      # 기상청 수집만
    python run_etl.py weekly       # 주간 리포트만

수동 실행 (웹시스템에서 호출):
    python run_etl.py --manual --farm-no 12345
    python run_etl.py --manual --farm-no 12345 --dt-from 20251215 --dt-to 20251221
"""

import argparse
import sys
from datetime import datetime

# 프로젝트 루트를 path에 추가
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from src.common import Config, setup_logger
from src.weekly import WeeklyReportOrchestrator
from src.collectors import WeatherCollector, ProductivityCollector


def parse_args():
    """CLI 인자 파싱"""
    parser = argparse.ArgumentParser(
        description='InsightPig ETL 실행',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
예시:
  python run_etl.py                    # 전체 ETL (기본)
  python run_etl.py weekly             # 주간 리포트만
  python run_etl.py weather            # 기상청 수집만
  python run_etl.py --test             # 테스트 모드 (금주 데이터)
  python run_etl.py --base-date 2024-12-15  # 특정 기준일
  python run_etl.py --dry-run          # 설정 확인만
  python run_etl.py --init             # 테스트 데이터 초기화 후 배치 실행
  python run_etl.py --init --dry-run   # 초기화 설정만 확인
  python run_etl.py --init --farm-list "1387,2807"  # 특정 농장만 테스트

수동 실행 (웹시스템에서 호출):
  python run_etl.py --manual --farm-no 12345
  python run_etl.py --manual --farm-no 12345 --dt-from 20251215 --dt-to 20251221
        """
    )

    parser.add_argument(
        'command',
        nargs='?',
        default='all',
        choices=['all', 'weekly', 'weather', 'productivity'],
        help='실행할 ETL 작업 (기본: all)'
    )

    parser.add_argument(
        '--test',
        action='store_true',
        help='테스트 모드 (금주 데이터 처리)'
    )

    parser.add_argument(
        '--base-date',
        type=str,
        help='기준일 (YYYY-MM-DD 형식)'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='실제 실행 없이 설정만 확인'
    )

    parser.add_argument(
        '--skip-weather',
        action='store_true',
        help='기상청 데이터 수집 스킵'
    )

    parser.add_argument(
        '--init',
        action='store_true',
        help='테스트 데이터 초기화 후 배치 실행 (테스트용)'
    )

    parser.add_argument(
        '--farm-list',
        type=str,
        default='1387,2807,4448,1456,1517',
        help='테스트용 농장 목록 (콤마 구분)'
    )

    # 수동 실행 관련 인자
    parser.add_argument(
        '--manual',
        action='store_true',
        help='수동 실행 모드 (웹시스템에서 특정 농장 ETL 호출)'
    )

    parser.add_argument(
        '--farm-no',
        type=int,
        help='수동 실행 대상 농장번호 (--manual과 함께 사용)'
    )

    parser.add_argument(
        '--dt-from',
        type=str,
        help='리포트 시작일 (YYYYMMDD, --manual과 함께 사용)'
    )

    parser.add_argument(
        '--dt-to',
        type=str,
        help='리포트 종료일 (YYYYMMDD, --manual과 함께 사용)'
    )

    return parser.parse_args()


def main():
    """메인 함수"""
    args = parse_args()

    # 기준일 변환
    base_date = None
    if args.base_date:
        try:
            dt = datetime.strptime(args.base_date, '%Y-%m-%d')
            base_date = dt.strftime('%Y%m%d')
        except ValueError:
            print(f"ERROR: 잘못된 날짜 형식: {args.base_date}")
            print("       YYYY-MM-DD 형식으로 입력하세요.")
            sys.exit(1)

    try:
        config = Config()
        logger = setup_logger("run_etl", config.logging.get('log_path'))

        # ========================================
        # 수동 실행 모드 (웹시스템에서 특정 농장 ETL 호출)
        # ========================================
        if args.manual:
            if not args.farm_no:
                print("ERROR: --manual 모드에서는 --farm-no가 필수입니다.")
                sys.exit(1)

            print("=" * 60)
            print("수동 ETL 실행 모드")
            print("=" * 60)
            print(f"농장번호: {args.farm_no}")
            print(f"기간: {args.dt_from or 'auto'} ~ {args.dt_to or 'auto'}")
            print()

            orchestrator = WeeklyReportOrchestrator(config)
            result = orchestrator.run_single_farm(
                farm_no=args.farm_no,
                dt_from=args.dt_from,
                dt_to=args.dt_to,
            )
            print(f"결과: {result}")

            if result.get('status') == 'success':
                print("\n수동 ETL 완료")
                sys.exit(0)
            else:
                print(f"\n수동 ETL 실패: {result.get('error')}")
                sys.exit(1)

        # 테스트 초기화 모드
        if args.init:
            print("=" * 60)
            print("테스트 초기화 모드")
            print("=" * 60)
            print(f"농장 목록: {args.farm_list}")
            print()

            if args.dry_run:
                print("DRY-RUN: 실제 초기화/실행 없이 설정만 확인")
                print("  - 삭제 대상 테이블: TS_INS_WEEK_SUB, TS_INS_WEEK, TS_INS_MASTER, TS_INS_JOB_LOG")
                print("  - 실행 날짜: 20251110, 20251117, 20251124, 20251201, 20251208, 20251215, 20251222")
                sys.exit(0)

            orchestrator = WeeklyReportOrchestrator(config)

            # Step 1: 테이블 초기화
            print("\n[Step 1] 테이블 초기화")
            init_result = orchestrator.initialize_test_data()
            print(f"초기화 결과: {init_result}")

            # Step 2: 배치 실행
            print("\n[Step 2] 배치 실행")
            batch_result = orchestrator.run_test_batch(farm_list=args.farm_list)
            print(f"배치 결과: {batch_result}")

            print("\n" + "=" * 60)
            print("테스트 초기화 완료")
            print("=" * 60)
            sys.exit(0)

        if args.command == 'all' or args.command == 'weekly':
            # 주간 리포트 ETL (전체 또는 weekly)
            orchestrator = WeeklyReportOrchestrator(config)
            result = orchestrator.run(
                base_date=base_date,
                test_mode=args.test,
                skip_productivity=True,  # 현재 스킵
                skip_weather=args.skip_weather or args.command == 'weekly',
                dry_run=args.dry_run,
            )
            print(f"결과: {result}")

        elif args.command == 'weather':
            # 기상청 데이터만 수집
            if args.dry_run:
                print("DRY-RUN: 기상청 데이터 수집")
            else:
                collector = WeatherCollector(config)
                count = collector.run()
                print(f"기상청 데이터 수집 완료: {count}건")

        elif args.command == 'productivity':
            # 생산성 데이터만 수집
            if args.dry_run:
                print("DRY-RUN: 생산성 데이터 수집")
            else:
                collector = ProductivityCollector(config)
                count = collector.run(stat_date=base_date)
                print(f"생산성 데이터 수집 완료: {count}건")

        sys.exit(0)

    except FileNotFoundError as e:
        print(f"ERROR: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
