# InsightPig ETL 시스템 개요

## 1. 프로젝트 개요

InsightPig ETL은 양돈 농장의 생산 데이터를 집계하여 리포트를 생성하는 배치 시스템입니다.
기존 Oracle Stored Procedure 기반 시스템을 Python으로 전환하여 유지보수성과 확장성을 개선하였습니다.

### 1.1 목적
- 농장별 생산 데이터 집계 및 리포트 생성 (주간/월간/분기)
- 기상청/생산성 외부 API 데이터 수집
- 웹시스템에서 조회할 수 있는 리포트 데이터 제공

### 1.2 리포트 종류

| 종류 | DAY_GB | 실행 주기 | 설명 |
|------|--------|----------|------|
| **주간 리포트** | WEEK | 매주 월요일 새벽 2시 | 지난주 데이터 집계 |
| 월간 리포트 | MON | 매월 1일 새벽 3시 | 지난달 데이터 집계 (예정) |
| 분기 리포트 | QT | 분기 첫날 새벽 4시 | 지난 분기 데이터 집계 (예정) |


## 2. 시스템 아키텍처

```
┌─────────────────────────────────────────────────────────────────┐
│                       InsightPig ETL                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐      │
│  │   run_etl.py │───▶│ Orchestrator │───▶│  Collectors  │      │
│  │   (Entry)    │    │              │    │  - Weather   │      │
│  └──────────────┘    │              │    │  - Product.  │      │
│                      └──────┬───────┘    └──────────────┘      │
│                             │                                   │
│           ┌─────────────────┼─────────────────┐                │
│           ▼                 ▼                 ▼                │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│  │ 주간 리포트   │  │ 월간 리포트   │  │ 분기 리포트   │         │
│  │ (WEEK)       │  │ (MON)        │  │ (QT)         │         │
│  └──────┬───────┘  └──────────────┘  └──────────────┘         │
│         │                                                       │
│         ▼                                                       │
│  ┌──────────────────────────────────────────────────┐          │
│  │                  FarmProcessor                    │          │
│  │  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐    │          │
│  │  │ Mating │ │Farrow- │ │Weaning │ │Culling │... │          │
│  │  │        │ │  ing   │ │        │ │        │    │          │
│  │  └────────┘ └────────┘ └────────┘ └────────┘    │          │
│  └──────────────────────────────────────────────────┘          │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │  Oracle Database │
                    │  TS_INS_MASTER   │
                    │  TS_INS_WEEK     │
                    │  TS_INS_WEEK_SUB │
                    └──────────────────┘
```


## 3. 디렉토리 구조

```
inspig-etl/
├── run_etl.py              # 메인 실행 스크립트
├── config.ini              # 설정 파일 (git 제외)
├── config.ini.example      # 설정 파일 템플릿
├── run_weekly.sh           # Crontab 실행 스크립트
├── deploy-etl.sh           # 운영 서버 배포 스크립트
├── requirements.txt        # Python 의존성
│
├── docs/                   # 문서
│   ├── 01_ETL_OVERVIEW.md      # 전체 개요 (본 문서)
│   ├── 02_WEEKLY_REPORT.md     # 주간 리포트 상세
│   ├── 03_MONTHLY_REPORT.md    # 월간 리포트 상세 (예정)
│   ├── 04_QUARTERLY_REPORT.md  # 분기 리포트 상세 (예정)
│   └── 05_OPERATION_GUIDE.md   # 운영 가이드
│
├── src/
│   ├── common/             # 공통 모듈
│   │   ├── config.py           # 설정 관리
│   │   ├── database.py         # DB 연결
│   │   └── logger.py           # 로깅
│   │
│   ├── collectors/         # 외부 데이터 수집
│   │   ├── base.py             # 수집기 기본 클래스
│   │   ├── weather.py          # 기상청 API
│   │   └── productivity.py     # 생산성 API
│   │
│   └── weekly/             # 주간 리포트
│       ├── orchestrator.py     # 오케스트레이터
│       ├── farm_processor.py   # 농장별 처리
│       ├── async_processor.py  # 비동기 병렬 처리
│       ├── data_loader.py      # 데이터 로더
│       └── processors/         # 프로세서들
│
└── logs/                   # 로그 파일
```


## 4. 설정 (config.ini)

```ini
[database]
# Oracle RDS 접속 정보
user = pksu
password = YOUR_PASSWORD_HERE
dsn = pigclouddb.c8ks4denaq5l.ap-northeast-2.rds.amazonaws.com:1521/pigplan

[processing]
# 병렬 처리 스레드 수
parallel = 4
max_farm_workers = 4
max_processor_workers = 5
test_mode = N

[logging]
log_path = /data/etl/inspig/logs

[api]
productivity_base_url = http://10.4.35.10:11000
productivity_timeout = 60

[weather]
api_key = YOUR_WEATHER_API_KEY
base_url = https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0
```


## 5. 데이터베이스 테이블

### 5.1 주요 테이블

| 테이블 | 용도 |
|--------|------|
| TS_INS_SERVICE | 서비스 신청 농장 관리 (REG_TYPE: AUTO/MANUAL) |
| TS_INS_MASTER | 배치 실행 마스터 |
| TS_INS_WEEK | 농장별 리포트 헤더 |
| TS_INS_WEEK_SUB | 농장별 리포트 상세 |
| TS_INS_JOB_LOG | 작업 로그 |

### 5.2 REG_TYPE 구분

| 값 | 설명 |
|----|------|
| AUTO | 정기 스케줄 대상 |
| MANUAL | 수동 등록 (스케줄 제외) |

> REG_TYPE과 무관하게 `/batch/manual` API를 통해 수동 ETL 실행 가능


## 6. 빠른 시작

### 6.1 로컬 개발 환경

```bash
cd C:\Projects\inspig-etl

# 가상환경 생성
python -m venv venv
venv\Scripts\activate

# 의존성 설치
pip install oracledb requests python-dotenv

# 설정 파일 생성
cp config.ini.example config.ini
# config.ini 편집하여 DB 정보 입력

# 테스트 실행 (dry-run)
python run_etl.py --dry-run
```

### 6.2 기본 실행

```bash
# 전체 ETL 실행
python run_etl.py

# 주간 리포트만
python run_etl.py weekly

# 기상청 데이터만
python run_etl.py weather

# 특정 농장 수동 실행
python run_etl.py --manual --farm-no 12345
```


## 7. 운영 서버 정보

| 항목 | 값 |
|------|-----|
| 서버 | 10.4.35.10 |
| 사용자 | pigplan |
| Python | 3.8.5 (Anaconda) |
| 경로 | /data/etl/inspig |
| Conda 환경 | inspig-etl |


## 8. Crontab 스케줄

```bash
# 주간: 매주 월요일 02:00
0 2 * * 1 /data/etl/inspig/run_weekly.sh

# 월간: 매월 1일 03:00 (예정)
0 3 1 * * /data/etl/inspig/run_monthly.sh

# 분기: 1,4,7,10월 1일 04:00 (예정)
0 4 1 1,4,7,10 * /data/etl/inspig/run_quarterly.sh
```


## 9. 관련 문서

| 문서 | 설명 |
|------|------|
| [02_WEEKLY_REPORT.md](./02_WEEKLY_REPORT.md) | 주간 리포트 상세 (프로세서, 기술 구현) |
| [03_MONTHLY_REPORT.md](./03_MONTHLY_REPORT.md) | 월간 리포트 상세 (예정) |
| [04_QUARTERLY_REPORT.md](./04_QUARTERLY_REPORT.md) | 분기 리포트 상세 (예정) |
| [05_OPERATION_GUIDE.md](./05_OPERATION_GUIDE.md) | 운영 가이드 (실행, 모니터링, 트러블슈팅) |
