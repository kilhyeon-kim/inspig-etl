# InsightPig ETL

스마트 양돈 관리 시스템 InsightPig의 ETL 프로젝트.

## 개요

기존 Oracle DB Job/Procedure 기반 배치 작업을 Python ETL로 전환합니다.

### 리포트 종류

| 종류 | DAY_GB | 실행 주기 | 상태 |
|------|--------|----------|------|
| 주간 리포트 | WEEK | 매주 월요일 02:00 | 완료 |
| 월간 리포트 | MON | 매월 1일 03:00 | 예정 |
| 분기 리포트 | QT | 분기 첫날 04:00 | 예정 |


## 빠른 시작

### 로컬 개발 환경

ssh -i "E:/ssh key/sshkey/aws/ProdPigplanKey.pem" pigplan@10.4.35.10
cd /data/etl/inspig

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

### 운영 서버 배포

```bash
./deploy-etl.sh
```


## 사용법

```bash
# 전체 ETL 실행 (기상청 + 주간리포트)
python run_etl.py

# 주간 리포트만 실행
python run_etl.py weekly

# 기상청 데이터만 수집
python run_etl.py weather

# 테스트 모드 (금주 데이터)
python run_etl.py --test
python run_etl.py --date-from 2025-11-10 --date-to 2025-12-22 --test

# 특정 기준일
python run_etl.py --base-date 2024-12-15

# 설정 확인 (실제 실행 안 함)
python run_etl.py --dry-run

# 특정 농장 수동 실행
python run_etl.py --manual --farm-no 12345
python run_etl.py --manual --farm-no 12345 --dt-from 20251215 --dt-to 20251221


```


## Crontab 설정

```bash
# 주간: 매주 월요일 02:00
0 2 * * 1 /data/etl/inspig/run_weekly.sh
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


## 프로젝트 구조

```
inspig-etl/
├── run_etl.py              # 메인 실행 스크립트
├── config.ini.example      # 설정 파일 예시
├── run_weekly.sh           # Crontab 실행 스크립트
├── deploy-etl.sh           # 배포 스크립트
├── docs/                   # 문서
├── src/
│   ├── common/             # 공통 모듈
│   ├── collectors/         # 외부 데이터 수집
│   └── weekly/             # 주간 리포트
│       ├── orchestrator.py
│       ├── farm_processor.py
│       └── processors/     # 10개 프로세서
└── logs/                   # 로그 디렉토리
```


## 관련 프로젝트

- [inspig](../inspig) - InsightPig 메인 프로젝트 (NestJS API, Vue.js Web)
