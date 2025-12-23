# InsightPig ETL 운영 가이드

## 1. 환경 설정

### 1.1 로컬 개발 환경

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

### 1.2 운영 서버 정보

| 항목 | 값 |
|------|-----|
| 서버 | 10.4.35.10 |
| 사용자 | pigplan |
| Python | 3.8.5 (Anaconda) |
| 경로 | /data/etl/inspig |
| Conda 환경 | inspig-etl |

### 1.3 운영 서버 배포

```bash
# 배포 스크립트 실행
./deploy-etl.sh

# 또는 수동 배포
scp -r src run_etl.py run_weekly.sh pigplan@10.4.35.10:/data/etl/inspig/
```


## 2. 실행 방법

### 2.1 CLI 사용법

```bash
# 전체 ETL 실행 (기상청 + 주간리포트)
python run_etl.py

# 주간 리포트만 실행
python run_etl.py weekly

# 기상청 데이터만 수집
python run_etl.py weather

# 생산성 데이터만 수집
python run_etl.py productivity

# 테스트 모드 (금주 데이터)
python run_etl.py --test

# 특정 기준일
python run_etl.py --base-date 2024-12-15

# 설정 확인 (실제 실행 안 함)
python run_etl.py --dry-run

# 기상청 수집 스킵
python run_etl.py --skip-weather
```

### 2.2 명령어별 안전도

운영 서버에서 테스트 시 영향도를 미리 파악하세요.

| 명령어 | 안전도 | 설명 |
|--------|--------|------|
| `--dry-run` | ✅ **안전** | DB 연결만 테스트, 데이터 변경 없음 |
| `--manual --farm-no 1387` | ⚠️ **주의** | 해당 농장 데이터만 재생성 (기존 데이터 덮어쓰기) |
| `--test` | ⚠️ **주의** | 금주 데이터로 테스트 배치 실행 |
| `--init` | ⚠️ **주의** | 테스트 데이터 초기화 후 배치 실행 |
| 옵션 없이 실행 | 🔴 **위험** | 전체 배치 실행 (기존 데이터 덮어쓰기) |

#### 안전한 테스트 순서

```bash
# 1단계: 연결 테스트 (데이터 변경 없음)
python run_etl.py --dry-run

# 2단계: 특정 농장 테스트 (영향 범위 최소화)
python run_etl.py --manual --farm-no 1387 --dry-run   # 먼저 확인
python run_etl.py --manual --farm-no 1387             # 실제 실행

# 3단계: 테스트 배치 (금주 데이터)
python run_etl.py --test --dry-run
python run_etl.py --test

# 4단계: 전체 배치 (운영 실행)
python run_etl.py
```

> **참고**: 운영 서버에서 구동 중인 cron 작업과 충돌하지 않습니다.
> ETL은 배치 작업으로, 실행 시점에만 DB에 접근합니다.
> 단, 동일 농장에 대해 동시 실행 시 데이터 정합성 문제가 발생할 수 있으니
> cron 실행 시간(월요일 02:00)을 피해 테스트하세요.

### 2.3 테스트 모드

```bash
# 테스트 데이터 초기화 후 배치 실행
python run_etl.py --init

# 초기화 설정만 확인
python run_etl.py --init --dry-run

# 특정 농장만 테스트
python run_etl.py --init --farm-list "1387,2807"
```

### 2.4 수동 ETL 실행

특정 농장의 주간 리포트를 수동으로 생성:

```bash
# 기본 실행 (지난주 기준 자동 계산)
python run_etl.py --manual --farm-no 12345

# 특정 기간 지정
python run_etl.py --manual --farm-no 12345 --dt-from 20251215 --dt-to 20251221
```


## 3. 스케줄 설정

### 3.1 Crontab 설정

```bash
# 주간: 매주 월요일 02:00
0 2 * * 1 /data/etl/inspig/run_weekly.sh

# 로그 정리: 매일 04:00
0 4 * * * /data/etl/inspig/cleanup_logs.sh
```

### 3.2 run_weekly.sh

```bash
#!/bin/bash
cd /data/etl/inspig
source /home/pigplan/anaconda3/etc/profile.d/conda.sh
conda activate inspig-etl
python run_etl.py >> /data/etl/inspig/logs/cron.log 2>&1
```


## 4. 로그 관리

### 4.1 로그 파일 구조

```
/data/etl/inspig/logs/
├── run_etl_YYYYMMDD.log        # 메인 실행 로그
├── weekly_YYYYMMDD.log         # 주간 리포트 로그
├── monthly_YYYYMMDD.log        # 월간 리포트 로그 (예정)
├── quarterly_YYYYMMDD.log      # 분기 리포트 로그 (예정)
├── weather_YYYYMMDD.log        # 기상청 수집 로그
└── cron.log                    # Crontab 실행 로그
```

### 4.2 로그 보존 정책

| 리포트 종류 | 보존 기간 | 정리 주기 |
|------------|----------|----------|
| 주간 (WEEK) | 1개월 | 매일 |
| 월간 (MON) | 6개월 | 매일 |
| 분기 (QT) | 1년 | 매일 |

### 4.3 로그 정리 스크립트 (cleanup_logs.sh)

```bash
#!/bin/bash
# /data/etl/inspig/cleanup_logs.sh

LOG_DIR="/data/etl/inspig/logs"

echo "=== ETL 로그 정리 시작: $(date) ==="

# 주간 로그: 1개월(30일) 이전 삭제
find $LOG_DIR -name "weekly_*.log" -mtime +30 -delete
find $LOG_DIR -name "run_etl_*.log" -mtime +30 -delete
find $LOG_DIR -name "weather_*.log" -mtime +30 -delete
echo "주간 로그 정리 완료 (30일 이전 삭제)"

# 월간 로그: 6개월(180일) 이전 삭제
find $LOG_DIR -name "monthly_*.log" -mtime +180 -delete
echo "월간 로그 정리 완료 (180일 이전 삭제)"

# 분기 로그: 1년(365일) 이전 삭제
find $LOG_DIR -name "quarterly_*.log" -mtime +365 -delete
echo "분기 로그 정리 완료 (365일 이전 삭제)"

# cron.log는 최근 1000줄만 유지
if [ -f "$LOG_DIR/cron.log" ]; then
    tail -1000 $LOG_DIR/cron.log > $LOG_DIR/cron.log.tmp
    mv $LOG_DIR/cron.log.tmp $LOG_DIR/cron.log
    echo "cron.log 정리 완료 (최근 1000줄 유지)"
fi

echo "=== ETL 로그 정리 완료: $(date) ==="
```


## 5. REST API 연동

NestJS 웹시스템에서 ETL 수동 실행 API 제공

### 5.1 전체 배치 실행

```
POST /batch/run
Content-Type: application/json

{
    "dayGb": "WEEK"
}

Response:
{
    "success": true,
    "message": "배치 프로세스가 시작되었습니다. (WEEK)"
}
```

### 5.2 특정 농장 수동 ETL

```
POST /batch/manual
Content-Type: application/json

{
    "farmNo": 12345,
    "dtFrom": "20251215",  // 선택
    "dtTo": "20251221"     // 선택
}

Response:
{
    "success": true,
    "message": "ETL 작업이 시작되었습니다. 농장=12345",
    "taskId": "manual_12345_1734850000000"
}
```


## 6. 대시보드용 DB 로그

### 6.1 로그 테이블 구조

| 테이블 | 용도 | 대시보드 활용 |
|--------|------|--------------|
| TS_INS_MASTER | 배치 실행 마스터 | 배치 실행 현황, 성공/실패율 |
| TS_INS_WEEK | 농장별 리포트 상태 | 농장별 처리 현황 |
| TS_INS_JOB_LOG | 작업 로그 (오류/성공) | 상세 로그, 오류 추적 |

### 6.2 대시보드 조회 쿼리

```sql
-- 1. 배치 실행 현황 (최근 30일)
SELECT
    SEQ,
    DAY_GB,
    TO_DATE(INS_DT, 'YYYYMMDD') AS REPORT_DATE,
    STATUS_CD,
    TARGET_CNT,
    COMPLETE_CNT,
    ERROR_CNT,
    ROUND(COMPLETE_CNT / NULLIF(TARGET_CNT, 0) * 100, 1) AS SUCCESS_RATE,
    START_DT,
    END_DT,
    ELAPSED_SEC
FROM TS_INS_MASTER
WHERE INS_DT >= TO_CHAR(SYSDATE - 30, 'YYYYMMDD')
ORDER BY SEQ DESC;

-- 2. 일별 처리 통계
SELECT
    INS_DT AS REPORT_DATE,
    DAY_GB,
    COUNT(*) AS BATCH_CNT,
    SUM(TARGET_CNT) AS TOTAL_FARMS,
    SUM(COMPLETE_CNT) AS COMPLETE_FARMS,
    SUM(ERROR_CNT) AS ERROR_FARMS,
    ROUND(AVG(ELAPSED_SEC)) AS AVG_ELAPSED_SEC
FROM TS_INS_MASTER
WHERE INS_DT >= TO_CHAR(SYSDATE - 30, 'YYYYMMDD')
GROUP BY INS_DT, DAY_GB
ORDER BY INS_DT DESC;

-- 3. 농장별 처리 현황
SELECT
    W.FARM_NO,
    W.FARM_NM,
    W.STATUS_CD,
    W.DT_FROM,
    W.DT_TO,
    M.INS_DT,
    M.DAY_GB
FROM TS_INS_WEEK W
JOIN TS_INS_MASTER M ON W.MASTER_SEQ = M.SEQ
WHERE M.SEQ = (SELECT MAX(SEQ) FROM TS_INS_MASTER WHERE DAY_GB = 'WEEK')
ORDER BY W.FARM_NO;

-- 4. 오류 농장 목록 (최근 7일)
SELECT
    M.SEQ AS MASTER_SEQ,
    M.DAY_GB,
    M.INS_DT,
    W.FARM_NO,
    W.FARM_NM,
    J.PROC_NM,
    J.ERROR_MSG,
    J.LOG_INS_DT
FROM TS_INS_WEEK W
JOIN TS_INS_MASTER M ON W.MASTER_SEQ = M.SEQ
LEFT JOIN TS_INS_JOB_LOG J ON W.MASTER_SEQ = J.MASTER_SEQ AND W.FARM_NO = J.FARM_NO
WHERE W.STATUS_CD = 'ERROR'
  AND M.INS_DT >= TO_CHAR(SYSDATE - 7, 'YYYYMMDD')
ORDER BY M.SEQ DESC, W.FARM_NO;

-- 5. 프로세서별 오류 통계
SELECT
    PROC_NM,
    COUNT(*) AS ERROR_CNT,
    COUNT(DISTINCT FARM_NO) AS FARM_CNT
FROM TS_INS_JOB_LOG
WHERE STATUS_CD = 'ERROR'
  AND LOG_INS_DT >= SYSDATE - 30
GROUP BY PROC_NM
ORDER BY ERROR_CNT DESC;
```

### 6.3 현재 로그 기록 현황

| 항목 | 현재 상태 | 기록 위치 |
|------|----------|----------|
| 배치 시작/종료 시간 | ✅ 완료 | TS_INS_MASTER |
| 대상/완료/오류 건수 | ✅ 완료 | TS_INS_MASTER |
| 농장별 처리 상태 | ✅ 완료 | TS_INS_WEEK |
| 오류 상세 로그 | ✅ 완료 | TS_INS_JOB_LOG |
| 정상 처리 로그 | ✅ 완료 | TS_INS_JOB_LOG (STATUS_CD='SUCCESS') |
| 프로세서별 소요시간 | ✅ 완료 | TS_INS_JOB_LOG (ELAPSED_MS) |

### 6.4 TS_INS_JOB_LOG 상세

```sql
-- 테이블 구조
CREATE TABLE TS_INS_JOB_LOG (
    SEQ             NUMBER NOT NULL,
    MASTER_SEQ      NUMBER,
    JOB_NM          VARCHAR2(50) NOT NULL,    -- 'PYTHON_ETL'
    PROC_NM         VARCHAR2(50) NOT NULL,    -- 프로세서명 (ConfigProcessor, AlertProcessor, ...)
    FARM_NO         INTEGER,
    DAY_GB          VARCHAR2(10),             -- WEEK, MON, QT
    REPORT_YEAR     NUMBER(4),
    REPORT_WEEK_NO  NUMBER(2),
    STATUS_CD       VARCHAR2(10),             -- SUCCESS, ERROR
    START_DT        DATE NOT NULL,
    END_DT          DATE,
    ELAPSED_MS      INTEGER DEFAULT 0,        -- 소요시간 (밀리초)
    PROC_CNT        INTEGER DEFAULT 0,
    ERROR_MSG       VARCHAR2(4000),
    LOG_INS_DT      DATE DEFAULT SYSDATE,
    CONSTRAINT PK_TS_INS_JOB_LOG PRIMARY KEY (SEQ)
);
```

### 6.5 대시보드용 추가 쿼리

```sql
-- 프로세서별 평균 소요시간 (최근 7일)
SELECT
    PROC_NM,
    COUNT(*) AS EXEC_CNT,
    ROUND(AVG(ELAPSED_MS)) AS AVG_MS,
    MIN(ELAPSED_MS) AS MIN_MS,
    MAX(ELAPSED_MS) AS MAX_MS
FROM TS_INS_JOB_LOG
WHERE STATUS_CD = 'SUCCESS'
  AND LOG_INS_DT >= SYSDATE - 7
GROUP BY PROC_NM
ORDER BY AVG_MS DESC;

-- 농장별 전체 처리시간 (최근 7일)
SELECT
    FARM_NO,
    TO_CHAR(LOG_INS_DT, 'YYYY-MM-DD') AS PROC_DATE,
    SUM(ELAPSED_MS) AS TOTAL_MS,
    ROUND(SUM(ELAPSED_MS) / 1000, 1) AS TOTAL_SEC
FROM TS_INS_JOB_LOG
WHERE STATUS_CD = 'SUCCESS'
  AND LOG_INS_DT >= SYSDATE - 7
GROUP BY FARM_NO, TO_CHAR(LOG_INS_DT, 'YYYY-MM-DD')
ORDER BY PROC_DATE DESC, TOTAL_MS DESC;

-- 프로세서별 성공/실패 통계
SELECT
    PROC_NM,
    SUM(CASE WHEN STATUS_CD = 'SUCCESS' THEN 1 ELSE 0 END) AS SUCCESS_CNT,
    SUM(CASE WHEN STATUS_CD = 'ERROR' THEN 1 ELSE 0 END) AS ERROR_CNT,
    ROUND(SUM(CASE WHEN STATUS_CD = 'SUCCESS' THEN 1 ELSE 0 END) /
          NULLIF(COUNT(*), 0) * 100, 1) AS SUCCESS_RATE
FROM TS_INS_JOB_LOG
WHERE LOG_INS_DT >= SYSDATE - 30
GROUP BY PROC_NM
ORDER BY PROC_NM;
```


## 7. 모니터링

### 7.1 상태 코드

| STATUS_CD | 설명 |
|-----------|------|
| READY | 대기 중 |
| RUNNING | 실행 중 |
| COMPLETE | 완료 |
| ERROR | 오류 |

### 7.2 빠른 상태 확인

```sql
-- 최근 배치 실행 현황
SELECT SEQ, DAY_GB, INS_DT, STATUS_CD, TARGET_CNT, COMPLETE_CNT, ERROR_CNT,
       ELAPSED_SEC
FROM TS_INS_MASTER
ORDER BY SEQ DESC
FETCH FIRST 10 ROWS ONLY;

-- 오류 발생 농장 조회
SELECT M.SEQ, W.FARM_NO, W.FARM_NM, W.STATUS_CD, W.DT_FROM, W.DT_TO
FROM TS_INS_MASTER M
JOIN TS_INS_WEEK W ON M.SEQ = W.MASTER_SEQ
WHERE W.STATUS_CD = 'ERROR'
ORDER BY M.SEQ DESC, W.FARM_NO;
```


## 8. 트러블슈팅

### 8.1 DB 연결 오류

```
오류: ORA-12170: TNS:Connect timeout occurred

해결:
1. config.ini의 DSN 확인
2. 방화벽 설정 확인 (1521 포트)
3. Oracle 리스너 상태 확인
```

### 8.2 특정 농장 ETL 실패

```sql
-- 오류 상세 확인
SELECT * FROM TS_INS_JOB_LOG
WHERE FARM_NO = 12345
ORDER BY SEQ DESC;

-- 해당 농장 데이터 재생성
python run_etl.py --manual --farm-no 12345
```

### 8.3 메모리 부족

```
오류: MemoryError

해결:
1. config.ini의 parallel 값 줄이기 (4 → 2)
2. max_farm_workers 값 줄이기
3. 농장 단위 분할 실행
```

### 8.4 기상청 API 오류

```
오류: 기상청 API 호출 실패

해결:
1. config.ini의 api_key 확인
2. 기상청 API 서버 상태 확인
3. --skip-weather 옵션으로 스킵 후 별도 실행
```


## 9. 테스트 데이터 관리

### 9.1 테스트 데이터 초기화

> **주의**: 테스트 환경에서만 사용. 운영 데이터는 절대 삭제하지 않음.

```bash
python run_etl.py --init --dry-run  # 먼저 확인
python run_etl.py --init            # 실제 초기화
```

### 9.2 데이터 보존 정책

> **중요**: 운영 환경에서 생성된 ETL 데이터는 삭제하지 않습니다.
> - TS_INS_MASTER, TS_INS_WEEK, TS_INS_WEEK_SUB, TS_INS_JOB_LOG 데이터는 영구 보존
> - 이력 데이터는 리포트 조회 및 분석에 활용
> - 테스트 데이터 삭제가 필요한 경우 DBA에게 요청


## 10. 성능 튜닝

### 10.1 병렬 처리 설정

```ini
[processing]
# 농장별 병렬 워커 (서버 코어 수에 맞게 조정)
max_farm_workers = 4

# 프로세서별 병렬 워커
max_processor_workers = 5
```

### 10.2 예상 실행 시간

| 농장 수 | 병렬 워커 | 예상 시간 |
|---------|-----------|-----------|
| 100 | 4 | ~5분 |
| 500 | 4 | ~20분 |
| 1000 | 8 | ~25분 |

### 10.3 DB 인덱스 확인

```sql
-- 필수 인덱스 확인
SELECT INDEX_NAME, TABLE_NAME, COLUMN_NAME
FROM USER_IND_COLUMNS
WHERE TABLE_NAME IN ('TB_WORK_MODON', 'TB_BUN_MODON', 'TA_MODON')
ORDER BY TABLE_NAME, INDEX_NAME, COLUMN_POSITION;
```


## 11. 관련 문서

- [01_ETL_OVERVIEW.md](./01_ETL_OVERVIEW.md) - ETL 개요
- [02_WEEKLY_REPORT.md](./02_WEEKLY_REPORT.md) - 주간 리포트 상세
