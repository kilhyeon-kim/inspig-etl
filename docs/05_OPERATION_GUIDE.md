# InsightPig ETL 운영 가이드

---

## 목차

1. [서버 정보](#1-서버-정보)
2. [초기 설치](#2-초기-설치)
3. [API 서버 운영](#3-api-서버-운영)
4. [배치 ETL 운영](#4-배치-etl-운영)
5. [CLI 사용법](#5-cli-사용법)
6. [로그 관리](#6-로그-관리)
7. [테이블 구조](#7-테이블-구조)
8. [모니터링](#8-모니터링)
9. [트러블슈팅](#9-트러블슈팅)

---

## 1. 서버 정보

| 항목 | 값 |
|------|-----|
| 서버 IP | 10.4.35.10 |
| ETL API 포트 | 8001 |
| 설치 경로 | /data/etl/inspig |
| 계정 | pigplan |
| Python 가상환경 | /data/etl/inspig/venv |

---

## 2. 초기 설치

### 2.1 서버 접속

```bash
ssh -i "E:/ssh key/sshkey/aws/ProdPigplanKey.pem" pigplan@10.4.35.10
```

### 2.2 환경 설정

```bash
# 디렉토리 이동
cd /data/etl/inspig

# Python 가상환경 설정
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 설정 파일 생성
cp config.ini.example config.ini
vi config.ini  # DB 패스워드, API 키 입력
```

### 2.3 테스트

```bash
source venv/bin/activate
python run_etl.py --dry-run
```

---

## 3. API 서버 운영

### 3.1 서비스 등록 (최초 1회)

```bash
sudo cp /data/etl/inspig/inspig-etl-api.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable inspig-etl-api
sudo systemctl start inspig-etl-api
```

### 3.2 일상 운영 명령어

| 작업 | 명령어 |
|------|--------|
| 시작 | `sudo systemctl start inspig-etl-api` |
| 중지 | `sudo systemctl stop inspig-etl-api` |
| 재시작 | `sudo systemctl restart inspig-etl-api` |
| 상태확인 | `sudo systemctl status inspig-etl-api` |
| 로그확인 | `sudo journalctl -u inspig-etl-api -f` |
| 헬스체크 | `curl http://localhost:8001/health` |

### 3.3 API 엔드포인트

| 메서드 | URL | 설명 |
|--------|-----|------|
| GET | `/health` | 헬스체크 |
| POST | `/api/etl/run-farm` | 농장별 ETL 실행 |
| GET | `/api/etl/status/{farm_no}` | 리포트 상태 조회 |

---

## 4. 배치 ETL 운영

### 4.1 Crontab 설정

```bash
crontab -e
```

```
# 주간 배치: 매주 월요일 02:00
0 2 * * 1 /data/etl/inspig/run_weekly.sh

# 로그 정리: 매일 04:00
0 4 * * * /data/etl/inspig/cleanup_logs.sh
```

### 4.2 수동 실행

```bash
cd /data/etl/inspig
source venv/bin/activate

# 전체 배치 실행
python run_etl.py

# 특정 농장만 실행
python run_etl.py --farm 2807
```

---

## 5. CLI 사용법

### 5.1 기본 명령어

| 명령어 | 설명 |
|--------|------|
| `python run_etl.py` | 전체 ETL 실행 |
| `python run_etl.py weekly` | 주간 리포트만 실행 |
| `python run_etl.py weather` | 기상청 데이터만 수집 |
| `python run_etl.py --dry-run` | 테스트 (DB 변경 없음) |
| `python run_etl.py --test` | 금주 데이터로 테스트 |
| `python run_etl.py --farm 2807` | 특정 농장만 실행 |

### 5.2 안전도 등급

| 등급 | 명령어 | 설명 |
|------|--------|------|
| ✅ 안전 | `--dry-run` | DB 연결만 테스트, 데이터 변경 없음 |
| ⚠️ 주의 | `--farm 1387` | 해당 농장 데이터만 재생성 |
| ⚠️ 주의 | `--test` | 금주 데이터로 테스트 배치 |
| 🔴 위험 | 옵션 없이 실행 | 전체 배치 실행 |

### 5.3 안전한 테스트 순서

```bash
# 1단계: 연결 테스트
python run_etl.py --dry-run

# 2단계: 특정 농장 테스트
python run_etl.py --farm 1387

# 3단계: 전체 배치
python run_etl.py
```

### 5.4 INS_DT(기준일) 개념

| INS_DT 범위 | 지난주 (DT_FROM~DT_TO) | REPORT_WEEK |
|-------------|----------------------|-------------|
| 12/22(월)~12/28(일) | 12/15~12/21 | 51주 |
| 12/29(월)~12/31(수) | 12/22~12/28 | 52주 |

- **INS_DT**: ETL 실행 기준일
- **지난주**: INS_DT 기준 이전 주 (리포트 대상 기간)
- **REPORT_WEEK**: 지난주의 ISO Week 번호

---

## 6. 로그 관리

### 6.1 로그 파일 위치

```
/data/etl/inspig/logs/
├── run_etl_YYYYMMDD.log    # 메인 실행 로그
├── weekly_YYYYMMDD.log     # 주간 리포트 로그
├── weather_YYYYMMDD.log    # 기상청 수집 로그
└── cron.log                # Crontab 실행 로그
```

### 6.2 로그 보존 정책

| 로그 종류 | 보존 기간 |
|----------|----------|
| 주간 로그 | 30일 |
| 월간 로그 | 180일 |
| 분기 로그 | 365일 |

---

## 7. 테이블 구조

### 7.1 테이블 개요

| 테이블 | 역할 | 특성 |
|--------|------|------|
| **TS_INS_MASTER** | 배치 실행 마스터 | 배치 단위 실행 이력, 시작/종료 시간, 처리 건수 |
| **TS_INS_WEEK** | 주간 리포트 헤더 | 농장별 주간 리포트 메타정보, SHARE_TOKEN |
| **TS_INS_WEEK_SUB** | 주간 리포트 상세 | 주간 리포트 상세 데이터 (JSON 형태) |
| **TS_INS_MONTH** | 월간 리포트 헤더 | 농장별 월간 리포트 메타정보 |
| **TS_INS_QUARTER** | 분기 리포트 헤더 | 농장별 분기 리포트 메타정보 |
| **TS_INS_JOB_LOG** | 작업 로그 | 프로세서별 실행 로그, 오류 추적 |

### 7.2 TS_INS_MASTER (배치 마스터)

배치 실행 단위를 관리하는 마스터 테이블

| 주요 컬럼 | 설명 |
|----------|------|
| SEQ | 배치 실행 순번 (PK) |
| DAY_GB | 리포트 구분 (WEEK, MONTH, QUARTER) |
| INS_DT | 기준일 (YYYYMMDD) |
| STATUS_CD | 상태 (READY, RUNNING, COMPLETE, ERROR) |
| TARGET_CNT | 대상 농장 수 |
| COMPLETE_CNT | 완료 농장 수 |
| ERROR_CNT | 오류 농장 수 |
| START_DT / END_DT | 시작/종료 시간 |
| ELAPSED_SEC | 소요 시간(초) |

### 7.3 TS_INS_WEEK (주간 리포트)

농장별 주간 리포트 메타정보

| 주요 컬럼 | 설명 |
|----------|------|
| SEQ | 리포트 순번 (PK) |
| MASTER_SEQ | 배치 마스터 참조 (FK) |
| FARM_NO | 농장번호 |
| REPORT_YEAR | 리포트 년도 |
| REPORT_WEEK_NO | 리포트 주차 (ISO Week) |
| DT_FROM / DT_TO | 리포트 기간 (월~일) |
| SHARE_TOKEN | 공유용 토큰 (UUID) |
| STATUS_CD | 상태 (READY, COMPLETE, ERROR) |

### 7.4 TS_INS_WEEK_SUB (주간 리포트 상세)

주간 리포트 상세 데이터 (프로세서별 결과)

| 주요 컬럼 | 설명 |
|----------|------|
| SEQ | 상세 순번 (PK) |
| WEEK_SEQ | 주간 리포트 참조 (FK) |
| PROC_NM | 프로세서명 |
| JSON_DATA | 상세 데이터 (JSON) |

### 7.5 TS_INS_JOB_LOG (작업 로그)

프로세서별 실행 로그, 오류 추적용

| 주요 컬럼 | 설명 |
|----------|------|
| SEQ | 로그 순번 (PK) |
| MASTER_SEQ | 배치 마스터 참조 |
| PROC_NM | 프로세서명 |
| FARM_NO | 농장번호 |
| STATUS_CD | 상태 (SUCCESS, ERROR) |
| ELAPSED_MS | 소요 시간(밀리초) |
| ERROR_MSG | 오류 메시지 |

### 7.6 데이터 흐름

```
배치 실행
    │
    ▼
┌─────────────────┐
│ TS_INS_MASTER   │  ← 배치 단위 생성
└────────┬────────┘
         │
         ▼ (농장별 반복)
┌─────────────────┐
│ TS_INS_WEEK     │  ← 농장별 리포트 헤더 생성
└────────┬────────┘
         │
         ▼ (프로세서별 반복)
┌─────────────────┐
│ TS_INS_WEEK_SUB │  ← 상세 데이터 저장
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ TS_INS_JOB_LOG  │  ← 실행 로그 기록
└─────────────────┘
```

---

## 8. 모니터링

### 8.1 상태 코드

| STATUS_CD | 설명 |
|-----------|------|
| READY | 대기 중 |
| RUNNING | 실행 중 |
| COMPLETE | 완료 |
| ERROR | 오류 |

### 8.2 대시보드 활용

| 확인 항목 | 참조 테이블 |
|----------|------------|
| 배치 실행 현황 | TS_INS_MASTER |
| 농장별 처리 상태 | TS_INS_WEEK |
| 오류 상세 로그 | TS_INS_JOB_LOG |
| 프로세서별 소요시간 | TS_INS_JOB_LOG.ELAPSED_MS |

---

## 9. 트러블슈팅

### 9.1 API 서버 연결 안됨

```bash
# 1. 서비스 상태 확인
sudo systemctl status inspig-etl-api

# 2. 포트 확인
netstat -tlnp | grep 8001

# 3. 로그 확인
sudo journalctl -u inspig-etl-api -n 50
```

### 9.2 DB 연결 오류

```bash
# config.ini 확인
cat /data/etl/inspig/config.ini

# Oracle 환경변수 확인
echo $ORACLE_HOME
echo $LD_LIBRARY_PATH
```

### 9.3 ETL 실행 오류

```bash
# 특정 농장 재실행
python run_etl.py --farm 12345

# 의존성 재설치
pip install -r requirements.txt
```

### 9.4 메모리 부족

config.ini에서 병렬 워커 수 줄이기:

```ini
[processing]
max_farm_workers = 2    # 4 → 2로 줄임
```

---

## 관련 문서

- [01_ETL_OVERVIEW.md](./01_ETL_OVERVIEW.md) - ETL 개요
- [02_WEEKLY_REPORT.md](./02_WEEKLY_REPORT.md) - 주간 리포트 상세
- [server-operation-guide.md](./server-operation-guide.md) - 서버 운영 상세 가이드
