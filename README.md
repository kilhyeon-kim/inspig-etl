# InsightPig ETL

스마트 양돈 관리 시스템 InsightPig의 주간 리포트 ETL 프로젝트.

기존 Oracle DB의 `JOB_INS_WEEKLY_REPORT` Job을 Python ETL로 대체합니다.

## 구조

```
inspig-etl/
├── weekly_report_etl.py   # 메인 ETL 스크립트
├── config.ini.example     # 설정 파일 예시
├── run_weekly.sh          # Crontab 실행 스크립트
├── deploy-etl.sh          # 운영 서버 배포 스크립트
├── requirements.txt       # Python 의존성
└── logs/                  # 로그 디렉토리
```

## 빠른 시작

### 1. 로컬 개발 환경

```bash
# 프로젝트 클론
git clone https://github.com/your-org/inspig-etl.git
cd inspig-etl

# 가상환경 생성
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 의존성 설치
pip install oracledb python-dotenv

# 설정 파일 생성
cp config.ini.example config.ini
# config.ini 편집하여 DB 정보 입력

# 테스트 실행 (dry-run)
python weekly_report_etl.py --test --dry-run
```

### 2. 운영 서버 배포

```bash
# 배포 스크립트 실행
./deploy-etl.sh

# 서버 접속 후 config.ini 설정
ssh -i "E:/ssh key/sshkey/aws/ProdPigplanKey.pem" pigplan@10.4.35.10
cd /data/etl/inspig-weekly
cp config.ini.example config.ini
vi config.ini
```

## 사용법

```bash
# 운영 모드 (config.ini 설정대로)
python weekly_report_etl.py

# 테스트 모드 (금주 데이터만)
python weekly_report_etl.py --test

# 특정 기준일
python weekly_report_etl.py --base-date 2024-12-15

# 설정 확인 (실제 실행 안 함)
python weekly_report_etl.py --dry-run
```

## Crontab 설정

```bash
# 매주 월요일 02:00 실행
0 2 * * 1 /data/etl/inspig-weekly/run_weekly.sh
```

## 운영 서버 정보

| 항목 | 값 |
|------|-----|
| 서버 | 10.4.35.10 |
| 사용자 | pigplan |
| Python | 3.8.5 (Anaconda) |
| 경로 | /data/etl/inspig-weekly |
| Conda 환경 | inspig-weekly-etl |

## 관련 문서

- [InsightPig 배포 가이드](../inspig/docs/web/04-deployment.md)
- [Python ETL 가이드](../inspig/docs/web/05-python-etl.md)
