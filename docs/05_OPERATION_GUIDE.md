# InsightPig ETL 운영 가이드

---

## 목차

1. [서버 정보](#1-서버-정보)
2. [초기 설치](#2-초기-설치)
3. [ETL 실행 흐름도](#3-etl-실행-흐름도)
4. [CLI 사용법 (run_etl.py)](#4-cli-사용법-run_etlpy)
5. [API 서버 운영](#5-api-서버-운영)
6. [배치 ETL 운영](#6-배치-etl-운영)
7. [로그 관리](#7-로그-관리)
8. [테이블 구조](#8-테이블-구조)
9. [모니터링](#9-모니터링)
10. [트러블슈팅](#10-트러블슈팅)

---

## 1. 서버 정보

| 항목 | 값 |
|------|-----|
| 서버 IP | 10.4.35.10 |
| ETL API 포트 | 8001 |
| 설치 경로 | /data/etl/inspig |
| 계정 | pigplan |
| Conda 환경 | inspig-etl |

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

# Conda 환경 활성화
source /data/anaconda/anaconda3/etc/profile.d/conda.sh
conda activate inspig-etl
pip install -r requirements.txt

# 설정 파일 생성
cp config.ini.example config.ini
vi config.ini  # DB 패스워드, API 키 입력
```

### 2.3 테스트

```bash
source /data/anaconda/anaconda3/etc/profile.d/conda.sh
conda activate inspig-etl
python run_etl.py --dry-run
```

---

## 3. ETL 실행 흐름도

### 3.1 전체 구성도

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              run_etl.py (CLI)                                   │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐         │
│  │  --manual   │   │   --init    │   │ --date-from │   │   기본/     │         │
│  │ 수동 실행   │   │ 테스트 초기화│   │ --date-to   │   │  --test     │         │
│  └──────┬──────┘   └──────┬──────┘   │ 배치 범위   │   │   모드      │         │
│         │                 │          └──────┬──────┘   └──────┬──────┘         │
│         ▼                 ▼                 ▼                 ▼                 │
│  run_single_farm()  initialize_test_data() │          orchestrator.run()       │
│         │           run_test_batch()       │                 │                 │
│         │                 │                │                 │                 │
│         └─────────────────┴────────────────┴─────────────────┘                 │
│                                    │                                            │
│                                    ▼                                            │
│                      ┌─────────────────────────────┐                           │
│                      │  WeeklyReportOrchestrator   │                           │
│                      └─────────────────────────────┘                           │
│                                    │                                            │
│                    ┌───────────────┼───────────────┐                           │
│                    ▼               ▼               ▼                           │
│           ┌──────────────┐ ┌──────────────┐ ┌──────────────┐                   │
│           │ 데이터 삭제   │ │ 외부 데이터  │ │ 주간 리포트  │                   │
│           │ (init_all/   │ │ 수집         │ │ 생성         │                   │
│           │  init_week)  │ │              │ │              │                   │
│           └──────────────┘ └──────────────┘ └──────────────┘                   │
│                                │                   │                            │
│                    ┌───────────┴───────────┐       │                            │
│                    ▼                       ▼       ▼                            │
│            ┌──────────────┐       ┌──────────────┐ ┌──────────────┐            │
│            │ Productivity │       │   Weather    │ │ FarmProcessor│            │
│            │  Collector   │       │  Collector   │ │  (병렬처리)  │            │
│            └──────────────┘       └──────────────┘ └──────────────┘            │
│                    │                       │               │                    │
│                    ▼                       ▼               ▼                    │
│            ┌──────────────┐       ┌──────────────┐ ┌──────────────┐            │
│            │TS_PRODUCTIVITY│      │ TM_WEATHER   │ │TS_INS_WEEK   │            │
│            └──────────────┘       └──────────────┘ │TS_INS_WEEK_SUB│           │
│                                                    │TS_INS_JOB_LOG │           │
│                                                    └──────────────┘            │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 실행 조건별 동작 매트릭스

| 실행 모드 | 옵션 | 데이터 삭제 | 생산성 수집 | 대상 농장 | 비고 |
|-----------|------|-------------|-------------|-----------|------|
| **운영 모드** | (옵션 없음) | ❌ 삭제 안함 | ✅ 전체 농장 | 전체 서비스 농장 | 크론 등록용 |
| **운영 + 제외** | `--exclude 848` | ❌ 삭제 안함 | ✅ 제외 농장 빼고 | 전체 - 제외 농장 | 특정 농장 제외 |
| **테스트** | `--test` | ❌ 삭제 안함 | ✅ farm_list만 | farm_list 농장 | 기본: 1387,2807,... |
| **테스트 + 주차 삭제** | `--test --init-week` | ⚠️ 해당 주차만 | ✅ farm_list만 | farm_list 농장 | 재실행 시 사용 |
| **테스트 + 전체 삭제** | `--test --init-all` | 🔴 전체 삭제 | ✅ farm_list만 | farm_list 농장 | 초기화 후 실행 |
| **수동 실행** | `--manual --farm-no 1234` | ⚠️ 해당 농장/주차 | ✅ 해당 농장만 | 지정 농장 | 웹시스템 호출용 |

### 3.3 farm_list vs exclude_farms 우선순위

```
┌────────────────────────────────────────────────────────────────┐
│                     농장 필터링 로직                            │
├────────────────────────────────────────────────────────────────┤
│                                                                │
│  farm_list가 지정됨?                                           │
│       │                                                        │
│       ├── YES ──▶ farm_list에 있는 농장만 처리                 │
│       │          (exclude_farms 무시됨)                        │
│       │                                                        │
│       └── NO ───▶ exclude_farms가 지정됨?                      │
│                       │                                        │
│                       ├── YES ──▶ 전체 서비스 농장 - 제외 농장 │
│                       │                                        │
│                       └── NO ───▶ 전체 서비스 농장             │
│                                                                │
└────────────────────────────────────────────────────────────────┘
```

**핵심 원칙:**
- `--test` 모드: `--farm-list` 사용 (기본값: `1387,2807,848,4223,1013`)
- 운영 모드: `--farm-list` 무시, `--exclude` 사용 가능
- `farm_list`가 있으면 `exclude_farms`는 무시됨

### 3.4 데이터 삭제 정책 상세

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         데이터 삭제 정책 결정 흐름                           │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  test_mode = False?                                                         │
│       │                                                                     │
│       ├── YES ──▶ 삭제 안함 (운영 데이터 보호)                              │
│       │                                                                     │
│       └── NO ───▶ init_all = True?                                          │
│                       │                                                     │
│                       ├── YES ──▶ farm_list 있음?                           │
│                       │               │                                     │
│                       │               ├── YES ──▶ 해당 농장 전체 데이터 삭제│
│                       │               │                                     │
│                       │               └── NO ───▶ 전체 테이블 삭제          │
│                       │                          (TS_INS_*)                 │
│                       │                                                     │
│                       └── NO ───▶ init_week = True?                         │
│                                       │                                     │
│                                       ├── YES ──▶ farm_list 있음?           │
│                                       │               │                     │
│                                       │               ├── YES ──▶ 해당 농장,│
│                                       │               │          해당 주차만│
│                                       │               │          삭제       │
│                                       │               │                     │
│                                       │               └── NO ───▶ 해당 주차 │
│                                       │                          전체 삭제  │
│                                       │                                     │
│                                       └── NO ───▶ 삭제 안함                 │
│                                                  (--test만 사용)            │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. CLI 사용법 (run_etl.py)

### 4.1 기본 문법

```bash
python run_etl.py [command] [options]
```

### 4.2 Command 종류

| Command | 설명 |
|---------|------|
| `all` (기본값) | 전체 ETL 실행 (생산성 + 주간리포트) |
| `weekly` | 주간 리포트만 실행 |
| `weather` | 기상청 데이터만 수집 |
| `productivity` | 생산성 데이터만 수집 |

### 4.3 주요 옵션

| 옵션 | 설명 | 기본값 |
|------|------|--------|
| `--test` | 테스트 모드 (farm_list 농장만 처리) | - |
| `--base-date YYYY-MM-DD` | 기준일 지정 | 오늘 |
| `--dry-run` | 실제 실행 없이 설정만 확인 | - |
| `--farm-list "1387,2807"` | 테스트용 농장 목록 | `1387,2807,848,4223,1013` |
| `--exclude "848,1234"` | 제외할 농장 목록 | - |
| `--init-all` | 전체 테스트 데이터 삭제 (--test와 함께) | - |
| `--init-week` | 해당 주차 데이터만 삭제 (--test와 함께) | - |

### 4.4 실행 예시

#### 운영 모드

```bash
# 기본 운영 모드 (크론 등록용)
python run_etl.py

# 848 농장 제외하고 실행
python run_etl.py --exclude 848

# 여러 농장 제외
python run_etl.py --exclude "848,1234,5678"

# 특정 기준일로 실행
python run_etl.py --base-date 2025-01-06
```

#### 테스트 모드

```bash
# 테스트 모드 (기본 farm_list, 삭제 안함)
python run_etl.py --test

# 테스트 모드 + 해당 주차 삭제 후 실행
python run_etl.py --test --init-week

# 테스트 모드 + 전체 삭제 후 실행
python run_etl.py --test --init-all

# 특정 농장만 테스트
python run_etl.py --test --farm-list "1387,2807"

# 특정 농장 테스트 + 해당 주차 삭제
python run_etl.py --test --farm-list "1387" --init-week
```

#### 배치 날짜 범위 실행

```bash
# 여러 주차 순차 실행 (7일 간격)
python run_etl.py --date-from 2025-11-10 --date-to 2025-12-22 --test

# dry-run으로 실행 계획 확인
python run_etl.py --date-from 2025-11-10 --date-to 2025-12-22 --dry-run
```

#### 수동 실행 (웹시스템 호출용)

```bash
# 특정 농장 수동 실행
python run_etl.py --manual --farm-no 12345

# 기간 지정 수동 실행
python run_etl.py --manual --farm-no 12345 --dt-from 20251215 --dt-to 20251221
```

### 4.5 안전도 등급

| 등급 | 명령어 | 설명 |
|------|--------|------|
| ✅ 안전 | `--dry-run` | DB 연결만 테스트, 데이터 변경 없음 |
| ✅ 안전 | `--test` | 테스트 농장만, 삭제 안함 |
| ⚠️ 주의 | `--test --init-week` | 해당 주차 데이터 삭제 |
| ⚠️ 주의 | `--manual --farm-no 1234` | 해당 농장 데이터 재생성 |
| 🔴 위험 | `--test --init-all` | 전체 테스트 데이터 삭제 |
| 🔴 위험 | 옵션 없이 실행 | 전체 배치 실행 (운영) |

### 4.6 안전한 테스트 순서

```bash
# 1단계: 연결/설정 테스트
python run_etl.py --dry-run

# 2단계: 테스트 농장으로 실행 (삭제 안함)
python run_etl.py --test

# 3단계: 필요시 주차 데이터 삭제 후 재실행
python run_etl.py --test --init-week

# 4단계: 운영 실행 (특정 농장 제외)
python run_etl.py --exclude 848
```

---

## 5. API 서버 운영

### 5.1 서비스 등록 (최초 1회)

```bash
sudo cp /data/etl/inspig/inspig-etl-api.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable inspig-etl-api
sudo systemctl start inspig-etl-api
```

### 5.2 일상 운영 명령어

| 작업 | 명령어 |
|------|--------|
| 시작 | `sudo systemctl start inspig-etl-api` |
| 중지 | `sudo systemctl stop inspig-etl-api` |
| 재시작 | `sudo systemctl restart inspig-etl-api` |
| 상태확인 | `sudo systemctl status inspig-etl-api` |
| 로그확인 | `sudo journalctl -u inspig-etl-api -f` |
| 헬스체크 | `curl http://localhost:8001/health` |

### 5.3 API 엔드포인트

| 메서드 | URL | 설명 |
|--------|-----|------|
| GET | `/health` | 헬스체크 |
| POST | `/api/etl/run-farm` | 농장별 ETL 실행 |
| GET | `/api/etl/status/{farm_no}` | 리포트 상태 조회 |

---

## 6. 배치 ETL 운영

### 6.1 Crontab 설정

```bash
crontab -e
```

```
# 주간 배치: 매주 월요일 02:00
0 2 * * 1 /data/etl/inspig/run_weekly.sh

# 로그 정리: 매일 04:00
0 4 * * * /data/etl/inspig/cleanup_logs.sh
```

### 6.2 INS_DT(기준일) 개념

| INS_DT 범위 | 지난주 (DT_FROM~DT_TO) | REPORT_WEEK |
|-------------|----------------------|-------------|
| 12/22(월)~12/28(일) | 12/15~12/21 | 51주 |
| 12/29(월)~12/31(수) | 12/22~12/28 | 52주 |

- **INS_DT**: ETL 실행 기준일
- **지난주**: INS_DT 기준 이전 주 (리포트 대상 기간)
- **REPORT_WEEK**: 지난주의 ISO Week 번호

---

## 7. 로그 관리

### 7.1 로그 파일 위치

```
/data/etl/inspig/logs/
├── run_etl_YYYYMMDD.log    # 메인 실행 로그
├── weekly_YYYYMMDD.log     # 주간 리포트 로그
├── weather_YYYYMMDD.log    # 기상청 수집 로그
└── cron_*.log              # Crontab 실행 로그
```

### 7.2 로그 보존 정책

| 로그 종류 | 보존 기간 |
|----------|----------|
| 주간 로그 | 30일 |
| 월간 로그 | 180일 |
| 분기 로그 | 365일 |

---

## 8. 테이블 구조

### 8.1 테이블 개요

| 테이블 | 역할 | 특성 |
|--------|------|------|
| **TS_INS_MASTER** | 배치 실행 마스터 | 배치 단위 실행 이력, 시작/종료 시간, 처리 건수 |
| **TS_INS_WEEK** | 주간 리포트 헤더 | 농장별 주간 리포트 메타정보, SHARE_TOKEN |
| **TS_INS_WEEK_SUB** | 주간 리포트 상세 | 주간 리포트 상세 데이터 (JSON 형태) |
| **TS_INS_JOB_LOG** | 작업 로그 | 프로세서별 실행 로그, 오류 추적 |
| **TS_PRODUCTIVITY** | 생산성 데이터 | PSY, MSY 등 생산성 지표 |

### 8.2 데이터 흐름

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

### 8.3 주요 컬럼 설명

#### TS_INS_MASTER (배치 마스터)

| 컬럼 | 설명 |
|------|------|
| SEQ | 배치 실행 순번 (PK) |
| DAY_GB | 리포트 구분 (WEEK, MONTH, QUARTER) |
| REPORT_YEAR | 리포트 년도 |
| REPORT_WEEK_NO | 리포트 주차 (ISO Week) |
| DT_FROM / DT_TO | 리포트 기간 |
| STATUS_CD | 상태 (READY, RUNNING, COMPLETE, ERROR) |
| TARGET_CNT | 대상 농장 수 |
| COMPLETE_CNT | 완료 농장 수 |
| ERROR_CNT | 오류 농장 수 |

#### TS_INS_WEEK (주간 리포트)

| 컬럼 | 설명 |
|------|------|
| MASTER_SEQ | 배치 마스터 참조 (FK) |
| FARM_NO | 농장번호 |
| SHARE_TOKEN | 공유용 토큰 (UUID) |
| STATUS_CD | 상태 (READY, COMPLETE, ERROR) |
| MODON_SANGSI_CNT | 상시모돈수 (TS_PRODUCTIVITY에서 조회) |

---

## 9. 모니터링

### 9.1 상태 코드

| STATUS_CD | 설명 |
|-----------|------|
| READY | 대기 중 |
| RUNNING | 실행 중 |
| COMPLETE | 완료 |
| ERROR | 오류 |

### 9.2 모니터링 쿼리

```sql
-- 최근 배치 실행 현황
SELECT SEQ, DAY_GB, REPORT_YEAR, REPORT_WEEK_NO, STATUS_CD,
       TARGET_CNT, COMPLETE_CNT, ERROR_CNT, ELAPSED_SEC
FROM TS_INS_MASTER
ORDER BY SEQ DESC
FETCH FIRST 10 ROWS ONLY;

-- 오류 농장 조회
SELECT W.FARM_NO, W.FARM_NM, W.STATUS_CD, J.ERROR_MSG
FROM TS_INS_WEEK W
LEFT JOIN TS_INS_JOB_LOG J ON W.MASTER_SEQ = J.MASTER_SEQ
                          AND W.FARM_NO = J.FARM_NO
WHERE W.MASTER_SEQ = :master_seq
  AND W.STATUS_CD = 'ERROR';

-- 프로세서별 평균 소요시간
SELECT PROC_NM, AVG(ELAPSED_MS) AS AVG_MS, COUNT(*) AS CNT
FROM TS_INS_JOB_LOG
WHERE MASTER_SEQ = :master_seq
GROUP BY PROC_NM
ORDER BY AVG_MS DESC;
```

---

## 10. 트러블슈팅

### 10.1 API 서버 연결 안됨

```bash
# 1. 서비스 상태 확인
sudo systemctl status inspig-etl-api

# 2. 포트 확인
netstat -tlnp | grep 8001

# 3. 로그 확인
sudo journalctl -u inspig-etl-api -n 50
```

### 10.2 DB 연결 오류

```bash
# config.ini 확인
cat /data/etl/inspig/config.ini

# Oracle 환경변수 확인
echo $ORACLE_HOME
echo $LD_LIBRARY_PATH
```

### 10.3 ETL 실행 오류

```bash
# 특정 농장만 재실행 (수동)
python run_etl.py --manual --farm-no 12345

# 테스트 모드로 해당 주차 재실행
python run_etl.py --test --farm-list "12345" --init-week
```

### 10.4 메모리 부족

config.ini에서 병렬 워커 수 줄이기:

```ini
[processing]
max_farm_workers = 2    # 4 → 2로 줄임
```

### 10.5 특정 농장 데이터 문제

```bash
# 1. 해당 농장만 삭제 후 재실행 (테스트 모드)
python run_etl.py --test --farm-list "문제농장번호" --init-week

# 2. 수동 실행으로 재생성
python run_etl.py --manual --farm-no 문제농장번호
```

### 10.6 운영 중 특정 농장 제외

```bash
# 문제 농장 제외하고 전체 실행
python run_etl.py --exclude "848"

# 여러 농장 제외
python run_etl.py --exclude "848,1234,5678"
```

---

## 관련 문서

- [01_ETL_OVERVIEW.md](./01_ETL_OVERVIEW.md) - ETL 개요
- [02_WEEKLY_REPORT.md](./02_WEEKLY_REPORT.md) - 주간 리포트 상세
- [server-operation-guide.md](./server-operation-guide.md) - 서버 운영 상세 가이드
