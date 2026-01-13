# InsightPig ETL

스마트 양돈 관리 시스템 InsightPig의 ETL 프로젝트.

## 개요

기존 Oracle DB Job/Procedure 기반 배치 작업을 Python ETL로 전환합니다.

### ETL 종류

| ETL | 스크립트 | 실행 주기 | 설명 |
|-----|---------|----------|------|
| **주간 리포트** | `main.py` | 매주 월요일 02:00, 12:00 | 주간 생산성 보고서 생성 |
| **날씨 수집** | `weather_etl.py` | 매 1시간 (정각) | 기상청 단기예보 수집 |
| 월간 리포트 | - | 매월 1일 03:00 | 예정 |
| 분기 리포트 | - | 분기 첫날 04:00 | 예정 |


## 빠른 시작

### 로컬 개발 환경

```bash
cd C:\Projects\inspig-etl

# Conda 환경 생성 (최초 1회)
conda create -n inspig-etl python=3.8
conda activate inspig-etl

# 의존성 설치
pip install oracledb requests python-dotenv

# 설정 파일 생성
cp config.ini.example config.ini
# config.ini 편집하여 DB 정보 입력

# 테스트 실행 (dry-run)
python run_etl.py --dry-run
```

### 운영 서버 접속

```bash
ssh -i "E:/ssh key/sshkey/aws/ProdPigplanKey.pem" pigplan@10.4.35.10
cd /data/etl/inspig
```

### 운영 서버 배포

```bash
./deploy-etl.sh
```


## 사용법

### 1. 주간 리포트 ETL (`main.py`)

주간 생산성 보고서를 생성합니다. TS_INS_MASTER, TS_INS_WEEK 등 테이블에 저장.

```bash
# 전체 주간 리포트 실행 (모든 대상 농장)
python main.py

# 테스트 모드 (금주 데이터)
python main.py --test
python main.py --date-from 2025-11-10 --date-to 2025-12-22 --test

# 특정 기준일
python main.py --base-date 2024-12-15

# 설정 확인 (실제 실행 안 함)
python main.py --dry-run

# 특정 농장 수동 실행
python main.py --manual --farm-no 12345
python main.py --manual --farm-no 12345 --dt-from 20251215 --dt-to 20251221
```

### 2. 날씨 ETL (`weather_etl.py`)

기상청 API를 호출하여 농장별 날씨 데이터를 수집합니다.
TM_WEATHER (일별), TM_WEATHER_HOURLY (시간별) 테이블에 저장.

#### 수집 데이터

| 데이터 | API | IS_FORECAST | 설명 |
|--------|-----|-------------|------|
| 단기예보 | getVilageFcst | Y | 오늘~+3일 예보 (격자 기반) |
| 초단기실황 | getUltraSrtNcst | N | 현재 시각 실측 (격자 기반) |
| ASOS 일자료 | AsosDalyInfoService | N | 최근 7일 실측 (관측소 기반) |

#### 관련 테이블

| 테이블 | 설명 |
|--------|------|
| TM_WEATHER | 일별 날씨 (예보/실측) |
| TM_WEATHER_HOURLY | 시간별 날씨 (예보/실측) |
| TM_WEATHER_ASOS | ASOS 관측소 메타정보 (97개 관측소) |
| TA_FARM.ASOS_STN_ID | 농장별 ASOS 관측소 매핑 (캐싱) |

```bash
# 기본 실행 (예보 + 실황)
python weather_etl.py

# ASOS 일자료도 함께 수집 (기본 7일)
python weather_etl.py --asos

# ASOS 14일치 수집
python weather_etl.py --asos --asos-days 14

# ASOS 특정 기간 수집
python weather_etl.py --asos --asos-start 20250101 --asos-end 20250107

# 농장 격자 좌표 업데이트 후 실행
python weather_etl.py --update-grid

# 농장 ASOS 관측소 매핑 업데이트 후 실행
python weather_etl.py --update-asos

# 격자 좌표만 업데이트 (날씨 수집 안함)
python weather_etl.py --grid-only

# 설정 확인 (실제 실행 안 함)
python weather_etl.py --dry-run
```

> **참고**:
> - 단기예보/초단기실황은 격자(NX, NY) 기반으로 조회
> - ASOS는 관측소 기반이므로 농장 위치에서 가장 가까운 관측소의 데이터를 사용
> - ASOS 관측소 정보는 TM_WEATHER_ASOS 테이블에서 관리 (DB 기반으로 유연한 관리)
> - 격자 중복 제거: 같은 (NX, NY) 격자는 API 1회만 호출 (불필요한 API 호출 방지)


## Crontab 설정

```bash
# 주간 리포트: 매주 월요일 02:00, 12:00
0 2,12 * * 1 cd /data/etl/inspig && ./venv/bin/python main.py >> logs/main_cron.log 2>&1

# 날씨 수집: 매 1시간 (예보 + 실황)
0 * * * * cd /data/etl/inspig && ./venv/bin/python weather_etl.py >> logs/weather_cron.log 2>&1

# 날씨 수집 (ASOS 포함): 매일 자정
0 0 * * * cd /data/etl/inspig && ./venv/bin/python weather_etl.py --asos >> logs/weather_cron.log 2>&1
```


## 운영 서버 정보

| 항목 | 값 |
|------|-----|
| 서버 | 10.4.35.10 |
| 사용자 | pigplan |
| Python | 3.8.5 (Anaconda) |
| 경로 | /data/etl/inspig |
| Conda 환경 | inspig-etl |


## 문서

| 문서 | 설명 |
|------|------|
| [01_ETL_OVERVIEW.md](docs/01_ETL_OVERVIEW.md) | 전체 개요 |
| [02_WEEKLY_REPORT.md](docs/02_WEEKLY_REPORT.md) | 주간 리포트 상세 |
| [03_MONTHLY_REPORT.md](docs/03_MONTHLY_REPORT.md) | 월간 리포트 상세 (예정) |
| [04_QUARTERLY_REPORT.md](docs/04_QUARTERLY_REPORT.md) | 분기 리포트 상세 (예정) |
| [05_OPERATION_GUIDE.md](docs/05_OPERATION_GUIDE.md) | 운영 가이드 |
| [공통 문서](docs/shared/README.md) | DB 테이블 정의, 뷰, SQL 가이드 등 공통 문서 |


## 프로젝트 구조

```
inspig-etl/
├── main.py                 # 주간 리포트 ETL 스크립트
├── weather_etl.py          # 날씨 수집 ETL 스크립트
├── config.ini.example      # 설정 파일 예시
├── deploy-etl.sh           # 배포 스크립트
├── docs/                   # 문서
├── src/
│   ├── common/             # 공통 모듈 (Config, Database, ApiKeyManager 등)
│   ├── collectors/         # 외부 데이터 수집
│   │   └── weather.py      # 기상청 API 수집기
│   └── weekly/             # 주간 리포트
│       ├── orchestrator.py
│       ├── farm_processor.py
│       └── processors/     # 10개 프로세서
└── logs/                   # 로그 디렉토리
```


## 관련 프로젝트

- [inspig](../inspig) - InsightPig 메인 프로젝트 (NestJS API, Vue.js Web)
