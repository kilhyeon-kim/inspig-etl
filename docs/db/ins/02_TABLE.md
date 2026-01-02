# ETL 테이블 정의

> InsightPig ETL 주간/월간/분기 리포트용 테이블

---

## 테이블 구조도

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         TA_SYS_CONFIG (시스템 설정)                          │
│  - INS_SCHEDULE_YN: ETL 스케줄 실행 여부 (Y/N)                               │
│  - 전역 설정 (SEQ=1 단일 레코드)                                              │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                       TS_INS_SERVICE (서비스 신청)                           │
│  - 농장별 인사이트피그 서비스 신청 정보                                        │
│  - INSPIG_YN='Y' 농장만 리포트 생성 대상                                      │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │ FARM_NO (1:N)
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        TS_INS_MASTER (리포트 마스터)                          │
│  - 주/월별 리포트 생성 배치 정보                                               │
│  - 대상 농장수, 완료 농장수, 실행 상태                                         │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │ MASTER_SEQ (1:N)
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         TS_INS_WEEK (주간 리포트)                             │
│  - 농장별 리포트 요약 데이터                                                  │
│  - SHARE_TOKEN: 공유 URL 토큰 (64자 SHA256)                                  │
│  - 페이지 표시용 집계 수치                                                    │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │ MASTER_SEQ + FARM_NO (1:N)
                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                       TS_INS_WEEK_SUB (리포트 상세)                           │
│  - 각 팝업별 리스트 데이터                                                    │
│  - GUBUN: MODON/ALERT/GB/BM/EU/SG/DOPE/SHIP/SCHEDULE                         │
└─────────────────────────────────────────────────────────────────────────────┘

┌───────────────────────────┐     ┌───────────────────────────┐
│   TS_INS_JOB_LOG (로그)    │     │   TS_INS_ACCESS_LOG       │
│  - 프로시저 실행 로그       │     │  - 사용자 접속 로그         │
│  - MASTER_SEQ 연결         │     │  - 리포트 조회 이력         │
└───────────────────────────┘     └───────────────────────────┘

┌───────────────────────────┐     ┌───────────────────────────────────────────┐
│   TS_PSY_DELAY_HEATMAP    │     │           TM_WEATHER (일별)               │
│  - PSY/지연일 분포 히트맵   │     │  - 기상청 격자(NX, NY) 기준                 │
│  - MASTER_SEQ 연결         │     │  - 1주일 예보 + 당일 실측                   │
└───────────────────────────┘     │  - UK: NX + NY + WK_DATE                   │
                                  └──────────────────────┬────────────────────┘
                                                         │ NX + NY + WK_DATE (1:N)
┌───────────────────────────┐                            ▼
│        TA_FARM            │     ┌───────────────────────────────────────────┐
│  - WEATHER_NX, WEATHER_NY │────▶│       TM_WEATHER_HOURLY (시간별)           │
│  - 농장 좌표로 격자 변환    │     │  - 당일 시간별 상세 예보                     │
│  - N:1 관계 (다수 농장→1날씨)│     │  - UK: NX + NY + WK_DATE + WK_TIME        │
└───────────────────────────┘     └───────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                       TS_PRODUCTIVITY (생산성 데이터)                         │
│  - 외부 API(10.4.35.10:11000) 수집 데이터                                    │
│  - PCODE: 031(교배)/032(분만)/033(이유)/034(번식종합)/035(모돈현황)           │
│  - PERIOD: W(주간)/M(월간)/Q(분기)                                           │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 테이블 목록

| 테이블명 | 설명 | 용도 |
|----------|------|------|
| TA_SYS_CONFIG | 시스템 설정 | ETL 스케줄 실행 여부 설정 |
| TS_INS_SERVICE | 서비스 신청 | 농장별 서비스 신청 정보 |
| TS_INS_MASTER | 리포트 마스터 | 리포트 생성 배치 정보 |
| TS_INS_JOB_LOG | 스케줄러 로그 | 프로시저 실행 로그 |
| TS_INS_WEEK | 주간 리포트 | 농장별 주간 리포트 데이터 |
| TS_INS_WEEK_SUB | 리포트 상세 | 팝업/상세 데이터 |
| TS_INS_MGMT | 관리포인트 | 주요 포인트/추천 콘텐츠 |
| TS_INS_ACCESS_LOG | 접속 로그 | 사용자 접속 로그 |
| TM_WEATHER | 날씨 정보 (일별) | 읍/면/동 기준 일별 날씨 |
| TM_WEATHER_HOURLY | 날씨 정보 (시간별) | 읍/면/동 기준 시간별 날씨 |
| TS_PSY_DELAY_HEATMAP | PSY 히트맵 | 농장 분포 히트맵 |
| TS_PRODUCTIVITY | 생산성 데이터 | 외부 API 수집 (PCODE별 통합) |

---

## 시간 저장 원칙

- **저장**: UTC (SYSDATE) - 글로벌 표준시간
- **계산/비교**: `SF_GET_LOCALE_VW_DATE_2022(LOCALE, SYSDATE)`
  - `KOR`: 한국 +09:00
  - `VNM`: 베트남 +07:00

---

## 1. TA_SYS_CONFIG (시스템 설정)

```sql
CREATE TABLE TA_SYS_CONFIG (
    SEQ             NUMBER DEFAULT 1,           -- 일련번호 (항상 1)
    MODON_HIST_YN   VARCHAR2(1) DEFAULT 'N',    -- 모돈이력제 연계여부 (Y/N)
    EKAPE_YN        VARCHAR2(1) DEFAULT 'N',    -- 축평원 등급판정 연계여부 (Y/N)
    INS_SCHEDULE_YN VARCHAR2(1) DEFAULT 'Y',    -- 인사이트피그플랜 실행여부 (Y/N), 테스트(T)
    TEST_TEL        VARCHAR2(18),               -- 테스트 SMS수신번호
    SISAE_YN        CHAR(1) DEFAULT 'Y',        -- 축평원 도축시세 연계 여부 (Y/N)
    WEATHER_YN      CHAR(1) DEFAULT 'Y',        -- 기상청 API 연계 여부 (Y/N), 테스트(T)
    LOG_INS_DT      DATE DEFAULT SYSDATE,       -- 생성일
    LOG_UPT_DT      DATE DEFAULT SYSDATE,       -- 수정일
    CONSTRAINT PK_TA_SYS_CONFIG PRIMARY KEY (SEQ)
);

-- 초기 데이터
INSERT INTO TA_SYS_CONFIG (SEQ, INS_SCHEDULE_YN) VALUES (1, 'Y');
```

### INS_SCHEDULE_YN 값

| 값 | 설명 | ETL 배치 | 웹 API | 알림톡 발송 번호 |
|----|------|---------|--------|-----------------|
| Y | 운영 모드 | 정상 실행 | 정상 | TA_MEMBER.HP_NUM |
| T | 테스트 모드 | 정상 실행 | 정상 | TA_SYS_CONFIG.TEST_TEL |
| N | 비활성화 | 스킵 | 비활성화 | - |

**참고:** `T` 모드에서 ETL과 웹 API는 `Y`와 동일하게 동작하며, 알림톡만 `TEST_TEL`로 발송됩니다.

---

## 2. TS_INS_SERVICE (서비스 신청)

```sql
CREATE TABLE TS_INS_SERVICE (
    FARM_NO         INTEGER NOT NULL,           -- 농장번호 (PK, FK)
    INSPIG_YN       VARCHAR2(1) DEFAULT 'N',    -- 서비스 신청여부
    INSPIG_REG_DT   VARCHAR2(8),                -- 신청일 (YYYYMMDD)
    INSPIG_FROM_DT  VARCHAR2(8),                -- 시작일 (YYYYMMDD)
    INSPIG_TO_DT    VARCHAR2(8),                -- 종료일 (YYYYMMDD)
    INSPIG_STOP_DT  VARCHAR2(8),                -- 중단일 (YYYYMMDD)
    WEB_PAY_YN      VARCHAR2(1) DEFAULT 'N',    -- 웹결재 여부
    REG_TYPE        VARCHAR2(10) DEFAULT 'AUTO', -- AUTO/MANUAL
    USE_YN          VARCHAR2(1) DEFAULT 'Y',
    LOG_INS_DT      DATE DEFAULT SYSDATE,
    LOG_UPT_DT      DATE DEFAULT SYSDATE,
    CONSTRAINT PK_TS_INS_SERVICE PRIMARY KEY (FARM_NO)
);
```

### REG_TYPE 값

| 값 | 설명 |
|----|------|
| AUTO | 정기 스케줄 대상 (기본값) |
| MANUAL | 수동 등록 (테스트/개별 실행) |

---

## 3. TS_INS_MASTER (리포트 마스터)

```sql
CREATE TABLE TS_INS_MASTER (
    SEQ             NUMBER NOT NULL,            -- 일련번호 (PK)
    DAY_GB          VARCHAR2(10) NOT NULL,      -- WEEK/MON/QT
    INS_DT          CHAR(8) NOT NULL,           -- 생성기준일 (YYYYMMDD)

    -- 기간 정보
    REPORT_YEAR     NUMBER(4) NOT NULL,         -- 년도
    REPORT_WEEK_NO  NUMBER(2) NOT NULL,         -- 주차(1~53)/월(1~12)
    DT_FROM         VARCHAR2(8) NOT NULL,       -- 시작일 (YYYYMMDD)
    DT_TO           VARCHAR2(8) NOT NULL,       -- 종료일 (YYYYMMDD)

    -- 실행 현황
    TARGET_CNT      INTEGER DEFAULT 0,          -- 대상 농장수
    COMPLETE_CNT    INTEGER DEFAULT 0,          -- 완료 농장수
    ERROR_CNT       INTEGER DEFAULT 0,          -- 오류 농장수

    -- 상태
    STATUS_CD       VARCHAR2(10) DEFAULT 'READY', -- READY/RUNNING/COMPLETE/ERROR
    START_DT        DATE,                       -- 시작일시
    END_DT          DATE,                       -- 종료일시
    ELAPSED_SEC     INTEGER DEFAULT 0,          -- 소요시간(초)

    LOG_INS_DT      DATE DEFAULT SYSDATE,
    CONSTRAINT PK_TS_INS_MASTER PRIMARY KEY (SEQ)
);

-- 인덱스
CREATE UNIQUE INDEX UK_TS_INS_MASTER_01 ON TS_INS_MASTER(DAY_GB, REPORT_YEAR, REPORT_WEEK_NO);
CREATE INDEX IDX_TS_INS_MASTER_01 ON TS_INS_MASTER(DAY_GB, INS_DT);
CREATE INDEX IDX_TS_INS_MASTER_02 ON TS_INS_MASTER(STATUS_CD);
```

### DAY_GB 값

| 값 | 설명 |
|----|------|
| WEEK | 주간 리포트 |
| MON | 월간 리포트 |
| QT | 분기 리포트 |

---

## 4. TS_INS_JOB_LOG (스케줄러 로그)

```sql
CREATE TABLE TS_INS_JOB_LOG (
    SEQ             NUMBER NOT NULL,            -- 일련번호 (PK)
    MASTER_SEQ      NUMBER,                     -- FK (NULL 허용)
    JOB_NM          VARCHAR2(50) NOT NULL,      -- JOB 이름
    PROC_NM         VARCHAR2(50) NOT NULL,      -- 프로시저명
    FARM_NO         INTEGER,                    -- 농장번호 (NULL=전체)

    -- 리포트 정보
    DAY_GB          VARCHAR2(10),               -- WEEK/MON/QT
    REPORT_YEAR     NUMBER(4),
    REPORT_WEEK_NO  NUMBER(2),

    -- 실행 상태
    STATUS_CD       VARCHAR2(10) DEFAULT 'RUNNING', -- RUNNING/SUCCESS/ERROR
    START_DT        DATE NOT NULL,
    END_DT          DATE,
    ELAPSED_MS      INTEGER DEFAULT 0,          -- 소요시간(ms)

    -- 처리 결과
    PROC_CNT        INTEGER DEFAULT 0,          -- 처리 건수
    ERROR_CD        VARCHAR2(20),
    ERROR_MSG       VARCHAR2(4000),

    LOG_INS_DT      DATE DEFAULT SYSDATE,
    CONSTRAINT PK_TS_INS_JOB_LOG PRIMARY KEY (SEQ)
);

-- 인덱스
CREATE INDEX IDX_TS_INS_JOB_LOG_01 ON TS_INS_JOB_LOG(MASTER_SEQ);
CREATE INDEX IDX_TS_INS_JOB_LOG_02 ON TS_INS_JOB_LOG(JOB_NM, START_DT);
CREATE INDEX IDX_TS_INS_JOB_LOG_03 ON TS_INS_JOB_LOG(STATUS_CD, START_DT);
CREATE INDEX IDX_TS_INS_JOB_LOG_04 ON TS_INS_JOB_LOG(MASTER_SEQ, FARM_NO, STATUS_CD);
CREATE INDEX IDX_TS_INS_JOB_LOG_05 ON TS_INS_JOB_LOG(DAY_GB, REPORT_YEAR, REPORT_WEEK_NO);
```

### 보관 정책

- 6개월 보관, 이전 로그 자동 삭제

---

## 5. TS_INS_WEEK (주간 리포트)

```sql
CREATE TABLE TS_INS_WEEK (
    MASTER_SEQ      NUMBER NOT NULL,            -- FK → TS_INS_MASTER
    FARM_NO         INTEGER NOT NULL,           -- FK → TS_INS_SERVICE

    -- 기간 정보
    REPORT_YEAR     NUMBER(4),
    REPORT_WEEK_NO  NUMBER(2),
    DT_FROM         VARCHAR2(8),
    DT_TO           VARCHAR2(8),

    -- 헤더 정보
    FARM_NM         VARCHAR2(100),              -- 농장명
    OWNER_NM        VARCHAR2(50),               -- 대표자명
    SIGUNGU_CD      VARCHAR2(10),               -- 시군구코드 (날씨용)

    -- 모돈 현황
    MODON_REG_CNT   INTEGER DEFAULT 0,          -- 현재모돈수
    MODON_REG_CHG   INTEGER DEFAULT 0,          -- 증감
    MODON_SANGSI_CNT INTEGER DEFAULT 0,         -- 상시모돈수
    MODON_SANGSI_CHG INTEGER DEFAULT 0,         -- 증감

    -- 관리대상 모돈 (alertMd)
    ALERT_TOTAL     INTEGER DEFAULT 0,
    ALERT_HUBO      INTEGER DEFAULT 0,          -- 미교배 후보돈
    ALERT_EU_MI     INTEGER DEFAULT 0,          -- 이유후 미교배
    ALERT_SG_MI     INTEGER DEFAULT 0,          -- 사고후 미교배
    ALERT_BM_DELAY  INTEGER DEFAULT 0,          -- 분만지연
    ALERT_EU_DELAY  INTEGER DEFAULT 0,          -- 이유지연

    -- 지난주 교배 (lastWeek.mating)
    LAST_GB_CNT     INTEGER DEFAULT 0,          -- 교배 복수
    LAST_GB_SUM     INTEGER DEFAULT 0,          -- 누계

    -- 지난주 분만 (lastWeek.farrowing)
    LAST_BM_CNT     INTEGER DEFAULT 0,          -- 분만 복수
    LAST_BM_TOTAL   INTEGER DEFAULT 0,          -- 총산자수
    LAST_BM_LIVE    INTEGER DEFAULT 0,          -- 실산자수
    LAST_BM_DEAD    INTEGER DEFAULT 0,          -- 사산
    LAST_BM_MUMMY   INTEGER DEFAULT 0,          -- 미라
    LAST_BM_SUM_CNT INTEGER DEFAULT 0,          -- 누계 복수
    LAST_BM_SUM_TOTAL INTEGER DEFAULT 0,        -- 총산 누계
    LAST_BM_SUM_LIVE INTEGER DEFAULT 0,         -- 실산 누계
    LAST_BM_AVG_TOTAL NUMBER(5,1) DEFAULT 0,    -- 총산 평균
    LAST_BM_AVG_LIVE NUMBER(5,1) DEFAULT 0,     -- 실산 평균
    LAST_BM_SUM_AVG_TOTAL NUMBER(5,1) DEFAULT 0,
    LAST_BM_SUM_AVG_LIVE NUMBER(5,1) DEFAULT 0,

    -- 지난주 이유 (lastWeek.weaning)
    LAST_EU_CNT     INTEGER DEFAULT 0,          -- 이유 복수
    LAST_EU_JD_CNT  INTEGER DEFAULT 0,          -- 이유자돈수
    LAST_EU_AVG_JD  NUMBER(5,1) DEFAULT 0,      -- 평균 이유두수
    LAST_EU_AVG_KG  NUMBER(5,1) DEFAULT 0,      -- 평균체중
    LAST_EU_SUM_CNT INTEGER DEFAULT 0,
    LAST_EU_SUM_JD  INTEGER DEFAULT 0,
    LAST_EU_SUM_AVG_JD NUMBER(5,1) DEFAULT 0,

    -- 지난주 사고 (lastWeek.accident)
    LAST_SG_CNT     INTEGER DEFAULT 0,
    LAST_SG_AVG_GYUNGIL NUMBER(5,1) DEFAULT 0,
    LAST_SG_SUM     INTEGER DEFAULT 0,
    LAST_SG_SUM_AVG_GYUNGIL NUMBER(5,1) DEFAULT 0,

    -- 지난주 도폐 (lastWeek.culling)
    LAST_CL_CNT     INTEGER DEFAULT 0,
    LAST_CL_SUM     INTEGER DEFAULT 0,

    -- 지난주 출하 (lastWeek.shipment)
    LAST_SH_CNT     INTEGER DEFAULT 0,
    LAST_SH_AVG_KG  NUMBER(5,1) DEFAULT 0,
    LAST_SH_SUM     INTEGER DEFAULT 0,
    LAST_SH_AVG_SUM NUMBER(5,1) DEFAULT 0,

    -- 금주 예정 (thisWeek)
    THIS_GB_SUM     INTEGER DEFAULT 0,          -- 교배 예정
    THIS_IMSIN_SUM  INTEGER DEFAULT 0,          -- 임신확인 예정
    THIS_BM_SUM     INTEGER DEFAULT 0,          -- 분만 예정
    THIS_EU_SUM     INTEGER DEFAULT 0,          -- 이유 예정
    THIS_VACCINE_SUM INTEGER DEFAULT 0,         -- 백신 예정
    THIS_SHIP_SUM   INTEGER DEFAULT 0,          -- 출하 예정

    -- KPI
    KPI_PSY         NUMBER(5,1) DEFAULT 0,
    KPI_DELAY_DAY   INTEGER DEFAULT 0,          -- 입력지연일
    PSY_X           INTEGER DEFAULT 0,
    PSY_Y           INTEGER DEFAULT 0,
    PSY_ZONE        VARCHAR2(10),               -- 1A~4D

    -- 상태
    STATUS_CD       VARCHAR2(10) DEFAULT 'READY',

    -- 공유 토큰
    SHARE_TOKEN     VARCHAR2(64),               -- SHA256 (64자)
    TOKEN_EXPIRE_DT VARCHAR2(8),                -- 만료일

    LOG_INS_DT      DATE DEFAULT SYSDATE,
    CONSTRAINT PK_TS_INS_WEEK PRIMARY KEY (MASTER_SEQ, FARM_NO)
);

-- 인덱스
CREATE INDEX IDX_TS_INS_WEEK_01 ON TS_INS_WEEK(FARM_NO, MASTER_SEQ);
CREATE INDEX IDX_TS_INS_WEEK_02 ON TS_INS_WEEK(FARM_NO, REPORT_YEAR, REPORT_WEEK_NO);
CREATE UNIQUE INDEX UK_TS_INS_WEEK_TOKEN ON TS_INS_WEEK(SHARE_TOKEN);
```

---

## 6. TS_INS_WEEK_SUB (리포트 상세)

팝업/섹션별 상세 데이터 저장

```sql
CREATE TABLE TS_INS_WEEK_SUB (
    MASTER_SEQ      NUMBER NOT NULL,
    FARM_NO         INTEGER NOT NULL,
    GUBUN           VARCHAR2(20) NOT NULL,      -- 데이터 구분
    SUB_GUBUN       VARCHAR2(20) DEFAULT '-',   -- 세부 구분
    SORT_NO         INTEGER DEFAULT 0,          -- 정렬순서

    -- 코드
    CODE_1          VARCHAR2(30),               -- 1차 구분코드
    CODE_2          VARCHAR2(30),               -- 2차 구분코드

    -- 숫자형 (CNT_1 ~ CNT_15)
    CNT_1 ~ CNT_15  NUMBER(10,2) DEFAULT 0,

    -- 수치형 (VAL_1 ~ VAL_15)
    VAL_1 ~ VAL_15  NUMBER(10,2) DEFAULT 0,

    -- 문자형 (STR_1 ~ STR_15)
    STR_1 ~ STR_15  VARCHAR2(1000),

    LOG_INS_DT      DATE DEFAULT SYSDATE,
    CONSTRAINT PK_TS_INS_WEEK_SUB PRIMARY KEY (MASTER_SEQ, FARM_NO, GUBUN, SUB_GUBUN, SORT_NO)
);
```

### GUBUN 값

| 값 | 설명 | 내용 |
|----|------|------|
| MODON | 모돈현황 팝업 | 산차별 현황 |
| ALERT | 관리대상 팝업 | 미교배/지연 모돈 목록 |
| GB | 교배 팝업 | 교배 통계/목록 |
| BM | 분만 팝업 | 분만 통계/목록 |
| EU | 이유 팝업 | 이유 통계/목록 |
| SG | 임신사고 팝업 | 사고 통계/목록 |
| DOPE | 도폐 팝업 | 도폐 통계/목록 |
| SHIP | 출하 팝업 | 출하 통계/목록 |
| SCHEDULE | 예정 캘린더 | 일별 예정 작업 |

### SUB_GUBUN 값

| 값 | 설명 |
|----|------|
| STAT | 요약 통계 |
| LIST | 목록 데이터 |
| CHART | 차트 데이터 |
| ROW | 행 데이터 |

---

## 7. 부가 테이블

### TM_WEATHER (일별 날씨)

기상청 격자(NX, NY) 기준 일별 날씨 데이터

- **UK**: NX + NY + WK_DATE
- **관계**: TA_FARM N:1 TM_WEATHER (다수 농장 → 1개 날씨)
- **격자 변환**: TA_FARM.MAP_X/Y → WEATHER_NX/NY (5km 단위)

> DDL: [TM_WEATHER.sql](../ddl/TM_WEATHER.sql)

### TM_WEATHER_HOURLY (시간별 날씨)

기상청 격자(NX, NY) 기준 시간별 날씨 데이터 (당일 상세 예보)

- **UK**: NX + NY + WK_DATE + WK_TIME

> DDL: [TM_WEATHER.sql](../ddl/TM_WEATHER.sql)

### TA_FARM 확장

```sql
ALTER TABLE TA_FARM ADD WEATHER_NX INTEGER;
ALTER TABLE TA_FARM ADD WEATHER_NY INTEGER;

COMMENT ON COLUMN TA_FARM.MAP_X IS 'WGS84 경도 (longitude) - Kakao API로 조회된 좌표';
COMMENT ON COLUMN TA_FARM.MAP_Y IS 'WGS84 위도 (latitude) - Kakao API로 조회된 좌표';
COMMENT ON COLUMN TA_FARM.WEATHER_NX IS '기상청 격자 X좌표 (5km 단위, MAP_X/MAP_Y로부터 변환)';
COMMENT ON COLUMN TA_FARM.WEATHER_NY IS '기상청 격자 Y좌표 (5km 단위, MAP_X/MAP_Y로부터 변환)';

CREATE INDEX IDX_TA_FARM_WEATHER ON TA_FARM(WEATHER_NX, WEATHER_NY);
```

### 좌표 컬럼 비교

| 항목 | MAP_X, MAP_Y | WEATHER_NX, WEATHER_NY |
|------|--------------|------------------------|
| **좌표계** | WGS84 (위경도) | 기상청 Lambert 격자 |
| **단위** | 도(degree) | 격자 번호 (정수) |
| **예시** | 126.8979, 36.5767 | 63, 89 |
| **정밀도** | 미터 단위 | 5km 단위 |
| **출처** | Kakao API | MAP_X/Y 변환 결과 |

### 날씨 코드 정의

| WEATHER_CD | WEATHER_NM | SKY_CD | PTY_CD |
|------------|------------|--------|--------|
| sunny | 맑음 | 1 | 0 |
| cloudy | 구름많음 | 3 | 0 |
| overcast | 흐림 | 4 | 0 |
| rainy | 비 | - | 1 |
| rain_snow | 비/눈 | - | 2 |
| snow | 눈 | - | 3 |
| shower | 소나기 | - | 4 |

### 기상청 API 정보

| 항목 | 값 |
|------|-----|
| API | 기상청 단기예보 API (공공데이터포털) |
| 격자 | 5km x 5km (Lambert Conformal Conic) |
| 예보 범위 | 당일 ~ +7일 |
| 갱신 주기 | 3시간 (02, 05, 08, 11, 14, 17, 20, 23시) |
| API 키 관리 | TS_API_KEY_INFO 테이블 (REQ_CNT 기반 로드밸런싱) |

> 격자 변환 함수: `latlon_to_grid()` - [weather.py](../../src/collectors/weather.py)

### TS_PSY_DELAY_HEATMAP (히트맵)

```sql
CREATE TABLE TS_PSY_DELAY_HEATMAP (
    MASTER_SEQ      NUMBER NOT NULL,
    X_POS           INTEGER NOT NULL,           -- 0~3: 입력지연일 구간
    Y_POS           INTEGER NOT NULL,           -- 0~3: PSY 구간
    ZONE_CD         VARCHAR2(10),               -- 1A~4D
    FARM_CNT        INTEGER DEFAULT 0,
    LOG_INS_DT      DATE DEFAULT SYSDATE,
    CONSTRAINT PK_TS_PSY_DELAY_HEATMAP PRIMARY KEY (MASTER_SEQ, X_POS, Y_POS)
);
```

---

## 8. TS_PRODUCTIVITY (생산성 데이터)

외부 API(10.4.35.10:11000)에서 수집한 생산성 지표 저장

### 기간 구분

| PERIOD | 설명 | PERIOD_NO 범위 |
|--------|------|----------------|
| W | 주간 | 1~53 (ISO 주차) |
| M | 월간 | 1~12 (월) |
| Q | 분기 | 1~4 (분기) |

### PCODE 구분

| PCODE | 설명 | 항목 수 |
|-------|------|---------|
| 031 | 교배 | 28개 |
| 032 | 분만 | 24개 |
| 033 | 이유 | 18개 |
| 034 | 번식종합 | 30개 |
| 035 | 모돈현황 | 13개 |

### 테이블 구조

```sql
CREATE TABLE TS_PRODUCTIVITY (
    SEQ             NUMBER(12) NOT NULL,
    FARM_NO         NUMBER(10) NOT NULL,
    PCODE           VARCHAR2(3) NOT NULL,              -- 031:교배, 032:분만, 033:이유, 034:번식종합, 035:모돈현황
    STAT_YEAR       NUMBER(4) NOT NULL,                -- 통계년도 (YYYY)
    PERIOD          VARCHAR2(1) NOT NULL,              -- 기간구분 (W:주간, M:월간, Q:분기)
    PERIOD_NO       NUMBER(2) NOT NULL,                -- 기간차수 (W:1~53, M:1~12, Q:1~4)
    STAT_DATE       VARCHAR2(10) NOT NULL,             -- 통계기준일 (YYYY-MM-DD)
    -- 데이터 컬럼 (C001~C043) - PCODE별로 사용 컬럼 다름
    C001            NUMBER(15,4),
    C002            NUMBER(15,4),
    C003            NUMBER(15,4),
    C004            NUMBER(15,4),
    C005            NUMBER(15,4),
    C006            NUMBER(15,4),
    C007            NUMBER(15,4),
    C008            NUMBER(15,4),
    C009            NUMBER(15,4),
    C010            NUMBER(15,4),
    C011            NUMBER(15,4),
    C012            NUMBER(15,4),
    C013            NUMBER(15,4),
    C014            NUMBER(15,4),
    C015            NUMBER(15,4),
    C016            NUMBER(15,4),
    C017            NUMBER(15,4),
    C018            NUMBER(15,4),
    C019            NUMBER(15,4),
    C020            NUMBER(15,4),
    C021            NUMBER(15,4),
    C022            NUMBER(15,4),
    C023            NUMBER(15,4),
    C024            NUMBER(15,4),
    C025            NUMBER(15,4),
    C026            NUMBER(15,4),
    C027            NUMBER(15,4),
    C028            NUMBER(15,4),
    C029            NUMBER(15,4),
    C030            NUMBER(15,4),
    C031            NUMBER(15,4),
    C032            NUMBER(15,4),
    C033            NUMBER(15,4),
    C034            NUMBER(15,4),
    C035            NUMBER(15,4),
    C036            NUMBER(15,4),
    C037            NUMBER(15,4),
    C038            NUMBER(15,4),
    C039            NUMBER(15,4),
    C040            NUMBER(15,4),
    C041            NUMBER(15,4),
    C042            NUMBER(15,4),
    C043            NUMBER(15,4),
    INS_DT          DATE DEFAULT SYSDATE,
    UPD_DT          DATE,
    CONSTRAINT PK_TS_PRODUCTIVITY PRIMARY KEY (SEQ)
);

-- 유니크 인덱스: 농장 + PCODE + 년도 + 기간구분 + 차수
CREATE UNIQUE INDEX UK_TS_PRODUCTIVITY ON TS_PRODUCTIVITY (FARM_NO, PCODE, STAT_YEAR, PERIOD, PERIOD_NO);

-- 조회용 인덱스
CREATE INDEX IX_TS_PRODUCTIVITY_01 ON TS_PRODUCTIVITY (STAT_YEAR, PERIOD, PERIOD_NO);
CREATE INDEX IX_TS_PRODUCTIVITY_02 ON TS_PRODUCTIVITY (PCODE, STAT_DATE);

-- 시퀀스
CREATE SEQUENCE SEQ_TS_PRODUCTIVITY START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;
```

### 컬럼명 규칙

- `C + 뒤3자리` (예: 031001 → C001, 035024 → C024)
- TC_CODE_SYS 조인: `PCODE IN ('031','032','033','034','035'), CODE=PCODE||컬럼뒤3자리`
- CNAME = 통계명 (STAT_NM), HELP_MSG = {"tooltip":"설명"} (JSON 형식)

### PCODE별 주요 컬럼 (031: 교배)

| 컬럼 | 코드 | 설명 |
|------|------|------|
| C001 | 031001 | 교배복수 |
| C013 | 031013 | 정상교배 |
| C014 | 031014 | 1차재발교배 |
| C015 | 031015 | 2차재발교배 |
| C016 | 031016 | 기타사고후교배 |
| C017 | 031017 | 미경산돈교배복수 |
| C024 | 031024 | 초교배복수(모돈편입) |
| C025 | 031025 | 평균초교배일령 |
| C028 | 031028 | 평균재귀발정일령 |
| C037 | 031037 | 7일내재귀율 |
| C039 | 031039 | 재발교배비율 |

### 조회 예시

```sql
-- 2024년 52주차 주간 데이터 조회
SELECT FARM_NO, PCODE, C023 AS PSY, C029 AS MSY
FROM TS_PRODUCTIVITY
WHERE STAT_YEAR = 2024 AND PERIOD = 'W' AND PERIOD_NO = 52 AND PCODE = '034';

-- 2024년 12월 월간 데이터 조회
SELECT FARM_NO, PCODE, C023 AS PSY, C029 AS MSY
FROM TS_PRODUCTIVITY
WHERE STAT_YEAR = 2024 AND PERIOD = 'M' AND PERIOD_NO = 12 AND PCODE = '034';
```

> 전체 DDL: [TS_PRODUCTIVITY.sql](../ddl/TS_PRODUCTIVITY.sql)
> 상세 코드 목록: [api_stat_codes.csv](../../api_stat_codes.csv)

---

## 성능 최적화 인덱스 (운영 DB)

```sql
-- TM_LPD_DATA (약 1,000만 건) - 출하 조회용
CREATE INDEX IDX_TM_LPD_DATA_INS_01 ON TM_LPD_DATA (FARM_NO, DOCHUK_DT);

-- TB_EU (약 1,100만 건) - 이유 조회용
CREATE INDEX IDX_TB_EU_INS_01 ON TB_EU (FARM_NO, WK_DT);
```
