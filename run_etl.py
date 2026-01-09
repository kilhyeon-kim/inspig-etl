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
from datetime import datetime, timedelta

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
  python run_etl.py                    # 운영 모드 ETL (기본, 크론 등록용)
  python run_etl.py weekly             # 주간 리포트만
  python run_etl.py weather            # 기상청 수집만
  python run_etl.py --test             # 테스트 모드 (기존 데이터 삭제 안함)
  python run_etl.py --test --init-week # 테스트 + 해당 주차 데이터만 삭제
  python run_etl.py --test --init-all  # 테스트 + 전체 데이터 삭제
  python run_etl.py --base-date 2024-12-15  # 특정 기준일
  python run_etl.py --dry-run          # 설정 확인만
  python run_etl.py --exclude 848      # 848 농장 제외하고 ETL 실행
  python run_etl.py --exclude "848,1234"  # 여러 농장 제외

날짜 범위 배치 실행 (주간 단위):
  python run_etl.py --date-from 2025-11-10 --date-to 2025-12-22 --test
  # 2025-11-10부터 7일 간격으로 2025-12-22까지 순차 실행

수동 실행 (웹시스템에서 호출):
  python run_etl.py --manual --farm-no 12345
  python run_etl.py --manual --farm-no 12345 --dt-from 20251215 --dt-to 20251221

데이터 삭제 정책:
  - 운영 모드 (옵션 없음): 기존 데이터 삭제 안함
  - --test: 기존 데이터 삭제 안함
  - --test --init-week: 해당 주차 데이터만 삭제
  - --test --init-all: 전체 테스트 데이터 삭제
        """
    )

    parser.add_argument(
        'command',
        nargs='?',
        default='all',
        choices=['all', 'weekly', 'monthly', 'quarterly', 'weather', 'productivity'],
        help='실행할 ETL 작업 (기본: all)'
    )

    parser.add_argument(
        '--day-gb',
        type=str,
        default='WEEK',
        choices=['WEEK', 'MONTH', 'QUARTER'],
        help='리포트 구분 (WEEK, MONTH, QUARTER, 기본: WEEK)'
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
        help='초기화 배치 실행 (테이블 초기화 + 테스트 배치 실행)'
    )

    parser.add_argument(
        '--init-all',
        action='store_true',
        help='--test와 함께 사용시 전체 테스트 데이터 삭제'
    )

    parser.add_argument(
        '--init-week',
        action='store_true',
        help='--test와 함께 사용시 해당 주차 데이터만 삭제'
    )

    parser.add_argument(
        '--farm-list',
        type=str,
        default='1387,2807,848,4223,1013',
        help='테스트용 농장 목록 (콤마 구분)'
    )

    parser.add_argument(
        '--exclude',
        type=str,
        default=None,
        help='제외할 농장번호 (콤마 구분, 예: "848,1234")'
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

    # 배치 날짜 범위 실행 (주간 단위)
    parser.add_argument(
        '--date-from',
        type=str,
        help='배치 시작일 (YYYY-MM-DD) - +7일씩 증가하며 date-to까지 실행'
    )

    parser.add_argument(
        '--date-to',
        type=str,
        help='배치 종료일 (YYYY-MM-DD)'
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
            print(f"리포트구분: {args.day_gb}")
            print(f"기간: {args.dt_from or 'auto'} ~ {args.dt_to or 'auto'}")
            print()

            # 현재는 WEEK만 구현
            if args.day_gb != 'WEEK':
                print(f"ERROR: {args.day_gb} 리포트는 아직 구현되지 않았습니다.")
                print("       현재 WEEK 리포트만 지원됩니다.")
                sys.exit(1)

            orchestrator = WeeklyReportOrchestrator(config)

            # run_single_farm은 ins_date(기준일)을 받아 자동으로 지난주를 계산
            # --dt-from, --dt-to 옵션은 현재 무시됨 (추후 확장 가능)
            # --base-date가 있으면 기준일로 사용
            ins_date = base_date  # YYYYMMDD 형식 또는 None

            result = orchestrator.run_single_farm(
                farm_no=args.farm_no,
                ins_date=ins_date,
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

        # ========================================
        # 배치 날짜 범위 실행 모드 (date-from ~ date-to)
        # ========================================
        if args.date_from and args.date_to:
            try:
                start_date = datetime.strptime(args.date_from, '%Y-%m-%d')
                end_date = datetime.strptime(args.date_to, '%Y-%m-%d')
            except ValueError as e:
                print(f"ERROR: 잘못된 날짜 형식: {e}")
                print("       YYYY-MM-DD 형식으로 입력하세요.")
                sys.exit(1)

            if start_date > end_date:
                print("ERROR: date-from이 date-to보다 클 수 없습니다.")
                sys.exit(1)

            # 날짜 목록 생성 (+7일씩)
            date_list = []
            current_date = start_date
            while current_date <= end_date:
                date_list.append(current_date.strftime('%Y%m%d'))
                current_date += timedelta(days=7)

            print("=" * 60)
            print("배치 날짜 범위 실행 모드")
            print("=" * 60)
            print(f"시작일: {args.date_from}")
            print(f"종료일: {args.date_to}")
            print(f"실행 날짜 목록: {', '.join(date_list)}")
            print(f"총 {len(date_list)}회 실행 예정")
            print()

            if args.dry_run:
                print("DRY-RUN: 실제 실행 없이 설정만 확인")
                sys.exit(0)

            orchestrator = WeeklyReportOrchestrator(config)
            results = []

            # farm_list: --test 모드에서만 사용
            batch_farm_list = args.farm_list if args.test else None

            for i, run_date in enumerate(date_list, 1):
                print("-" * 40)
                print(f"[{i}/{len(date_list)}] 기준일: {run_date}")
                print("-" * 40)

                # 첫 번째 실행에서만 전체 삭제 (init_all=True)
                # 이후 실행에서는 해당 주차만 삭제 (init_week=True)
                # 주의: --test 모드에서만 삭제 적용 (운영 모드에서는 test_mode=False로 삭제 차단됨)
                is_first = (i == 1)
                result = orchestrator.run(
                    base_date=run_date,
                    test_mode=args.test,
                    skip_productivity=False,  # 생산성 데이터 수집 (TS_PRODUCTIVITY)
                    skip_weather=True,  # 배치에서는 기상청 스킵
                    dry_run=False,
                    init_all=is_first and args.test,  # 첫 번째 + 테스트 모드만 전체 삭제
                    init_week=(not is_first) and args.test,  # 이후 + 테스트 모드만 해당 주차 삭제
                    farm_list=batch_farm_list,  # 대상 농장 (--test 모드, 콤마 구분)
                    exclude_farms=args.exclude,  # 제외할 농장 (콤마 구분)
                )
                results.append({
                    'date': run_date,
                    'status': result.get('status', 'unknown'),
                    'week_no': result.get('week_no'),
                })
                print(f"결과: {result.get('status')}")
                print()

            # 결과 요약
            print("=" * 60)
            print("배치 실행 완료")
            print("=" * 60)
            success_cnt = sum(1 for r in results if r['status'] == 'success')
            print(f"성공: {success_cnt}/{len(results)}")
            for r in results:
                status_icon = "✓" if r['status'] == 'success' else "✗"
                print(f"  {status_icon} {r['date']} (Week {r['week_no']})")
            sys.exit(0)

        if args.command == 'all' or args.command == 'weekly':
            # 주간 리포트 ETL (전체 또는 weekly)
            orchestrator = WeeklyReportOrchestrator(config)

            # farm_list: --test 모드에서만 사용
            farm_list = args.farm_list if args.test else None

            result = orchestrator.run(
                base_date=base_date,
                test_mode=args.test,
                skip_productivity=False,  # 생산성 수집 활성화
                skip_weather=True,  # 기상청 수집 스킵 (별도 API 사용)
                dry_run=args.dry_run,
                init_all=args.init_all,  # --test --init-all: 전체 데이터 삭제
                init_week=args.init_week,  # --test --init-week: 해당 주차만 삭제
                farm_list=farm_list,  # 대상 농장 (--test 모드, 콤마 구분)
                exclude_farms=args.exclude,  # 제외할 농장 (콤마 구분)
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
