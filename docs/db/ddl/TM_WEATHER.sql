-- =====================================================
-- TM_WEATHER: 일별 날씨 데이터 테이블
-- 기상청 단기예보 API 수집 데이터 저장
--
-- 격자(NX, NY) 기준:
--   - 기상청 Lambert 격자 (5km x 5km)
--   - 다수 농장 → 1개 날씨 데이터 (N:1)
--
-- 연결:
--   - TA_FARM.WEATHER_NX_N, WEATHER_NY_N 조인
--   - 농장 주소(읍면동) 정보는 TA_FARM.ADDR1에서 조회
-- =====================================================

CREATE TABLE TM_WEATHER (
    SEQ             NUMBER NOT NULL,
    WK_DATE         VARCHAR2(8) NOT NULL,       -- YYYYMMDD (예보일)

    -- 기상청 격자 좌표 (UK 역할)
    NX              INTEGER NOT NULL,           -- 격자 X (5km)
    NY              INTEGER NOT NULL,           -- 격자 Y (5km)

    -- 날씨 정보 (일별 집계)
    WEATHER_CD      VARCHAR2(20),               -- 날씨코드 (sunny/cloudy/rainy/snow)
    WEATHER_NM      VARCHAR2(50),               -- 날씨명 (맑음/구름많음/비/눈)
    TEMP_AVG        NUMBER(4,1),                -- 평균기온
    TEMP_HIGH       NUMBER(4,1),                -- 최고기온
    TEMP_LOW        NUMBER(4,1),                -- 최저기온
    RAIN_PROB       INTEGER DEFAULT 0,          -- 강수확률 (%)
    RAIN_AMT        NUMBER(5,1) DEFAULT 0,      -- 강수량 (mm)
    HUMIDITY        INTEGER,                    -- 습도 (%)
    WIND_SPEED      NUMBER(4,1),                -- 풍속 (m/s)
    WIND_DIR        INTEGER,                    -- 풍향 (deg)
    SKY_CD          VARCHAR2(10),               -- 하늘상태코드 (1:맑음,3:구름많음,4:흐림)

    -- 예보 정보
    FCST_DT         DATE,                       -- 예보 발표시각
    IS_FORECAST     CHAR(1) DEFAULT 'Y',        -- 예보여부 (Y:예보, N:실측)

    LOG_INS_DT      DATE DEFAULT SYSDATE,
    LOG_UPT_DT      DATE DEFAULT SYSDATE,
    CONSTRAINT PK_TM_WEATHER PRIMARY KEY (SEQ)
);

-- 인덱스: 격자(NX,NY) + 날짜가 유일키
CREATE UNIQUE INDEX UK_TM_WEATHER_01 ON TM_WEATHER(NX, NY, WK_DATE);
CREATE INDEX IDX_TM_WEATHER_01 ON TM_WEATHER(WK_DATE);

-- 시퀀스
CREATE SEQUENCE SEQ_TM_WEATHER START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;

-- 테이블 COMMENT
COMMENT ON TABLE TM_WEATHER IS '일별 날씨 데이터 (기상청 단기예보, 격자 기준)';
COMMENT ON COLUMN TM_WEATHER.SEQ IS '일련번호';
COMMENT ON COLUMN TM_WEATHER.WK_DATE IS '예보일 (YYYYMMDD)';
COMMENT ON COLUMN TM_WEATHER.NX IS '기상청 격자 X좌표 (5km 단위)';
COMMENT ON COLUMN TM_WEATHER.NY IS '기상청 격자 Y좌표 (5km 단위)';
COMMENT ON COLUMN TM_WEATHER.WEATHER_CD IS '날씨코드 (sunny/cloudy/overcast/rainy/snow/shower)';
COMMENT ON COLUMN TM_WEATHER.WEATHER_NM IS '날씨명 (맑음/구름많음/흐림/비/눈/소나기)';
COMMENT ON COLUMN TM_WEATHER.TEMP_AVG IS '평균기온 (도)';
COMMENT ON COLUMN TM_WEATHER.TEMP_HIGH IS '최고기온 (도)';
COMMENT ON COLUMN TM_WEATHER.TEMP_LOW IS '최저기온 (도)';
COMMENT ON COLUMN TM_WEATHER.RAIN_PROB IS '강수확률 (%)';
COMMENT ON COLUMN TM_WEATHER.RAIN_AMT IS '강수량 (mm)';
COMMENT ON COLUMN TM_WEATHER.HUMIDITY IS '습도 (%)';
COMMENT ON COLUMN TM_WEATHER.WIND_SPEED IS '풍속 (m/s)';
COMMENT ON COLUMN TM_WEATHER.WIND_DIR IS '풍향 (도)';
COMMENT ON COLUMN TM_WEATHER.SKY_CD IS '하늘상태코드 (1:맑음,3:구름많음,4:흐림)';
COMMENT ON COLUMN TM_WEATHER.FCST_DT IS '예보 발표시각';
COMMENT ON COLUMN TM_WEATHER.IS_FORECAST IS '예보여부 (Y:예보, N:실측)';


-- =====================================================
-- TM_WEATHER_HOURLY: 시간별 날씨 데이터 테이블
-- 기상청 단기예보 API 시간별 데이터 저장
-- =====================================================

CREATE TABLE TM_WEATHER_HOURLY (
    SEQ             NUMBER NOT NULL,
    WK_DATE         VARCHAR2(8) NOT NULL,       -- YYYYMMDD
    WK_TIME         VARCHAR2(4) NOT NULL,       -- HHMM (0000~2300)

    -- 기상청 격자 좌표
    NX              INTEGER NOT NULL,           -- 격자 X
    NY              INTEGER NOT NULL,           -- 격자 Y

    -- 시간별 날씨 정보
    WEATHER_CD      VARCHAR2(20),               -- 날씨코드
    WEATHER_NM      VARCHAR2(50),               -- 날씨명
    TEMP            NUMBER(4,1),                -- 기온
    RAIN_PROB       INTEGER DEFAULT 0,          -- 강수확률 (%)
    RAIN_AMT        NUMBER(5,1) DEFAULT 0,      -- 1시간 강수량 (mm)
    HUMIDITY        INTEGER,                    -- 습도 (%)
    WIND_SPEED      NUMBER(4,1),                -- 풍속 (m/s)
    WIND_DIR        INTEGER,                    -- 풍향 (deg)
    SKY_CD          VARCHAR2(10),               -- 하늘상태코드
    PTY_CD          VARCHAR2(10),               -- 강수형태코드 (0:없음,1:비,2:비/눈,3:눈,4:소나기)

    -- 예보 정보
    FCST_DT         DATE,                       -- 예보 발표시각
    BASE_DATE       VARCHAR2(8),                -- 예보 기준일
    BASE_TIME       VARCHAR2(4),                -- 예보 기준시간

    LOG_INS_DT      DATE DEFAULT SYSDATE,
    CONSTRAINT PK_TM_WEATHER_HOURLY PRIMARY KEY (SEQ)
);

-- 인덱스: 격자(NX,NY) + 날짜 + 시간이 유일키
CREATE UNIQUE INDEX UK_TM_WEATHER_HOURLY_01 ON TM_WEATHER_HOURLY(NX, NY, WK_DATE, WK_TIME);
CREATE INDEX IDX_TM_WEATHER_HOURLY_01 ON TM_WEATHER_HOURLY(WK_DATE, WK_TIME);

-- 시퀀스
CREATE SEQUENCE SEQ_TM_WEATHER_HOURLY START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;

-- 테이블 COMMENT
COMMENT ON TABLE TM_WEATHER_HOURLY IS '시간별 날씨 데이터 (기상청 단기예보, 격자 기준)';
COMMENT ON COLUMN TM_WEATHER_HOURLY.SEQ IS '일련번호';
COMMENT ON COLUMN TM_WEATHER_HOURLY.WK_DATE IS '예보일 (YYYYMMDD)';
COMMENT ON COLUMN TM_WEATHER_HOURLY.WK_TIME IS '예보시간 (HHMM)';
COMMENT ON COLUMN TM_WEATHER_HOURLY.NX IS '기상청 격자 X좌표';
COMMENT ON COLUMN TM_WEATHER_HOURLY.NY IS '기상청 격자 Y좌표';
COMMENT ON COLUMN TM_WEATHER_HOURLY.WEATHER_CD IS '날씨코드';
COMMENT ON COLUMN TM_WEATHER_HOURLY.WEATHER_NM IS '날씨명';
COMMENT ON COLUMN TM_WEATHER_HOURLY.TEMP IS '기온 (도)';
COMMENT ON COLUMN TM_WEATHER_HOURLY.RAIN_PROB IS '강수확률 (%)';
COMMENT ON COLUMN TM_WEATHER_HOURLY.RAIN_AMT IS '1시간 강수량 (mm)';
COMMENT ON COLUMN TM_WEATHER_HOURLY.HUMIDITY IS '습도 (%)';
COMMENT ON COLUMN TM_WEATHER_HOURLY.WIND_SPEED IS '풍속 (m/s)';
COMMENT ON COLUMN TM_WEATHER_HOURLY.WIND_DIR IS '풍향 (도)';
COMMENT ON COLUMN TM_WEATHER_HOURLY.SKY_CD IS '하늘상태코드 (1:맑음,3:구름많음,4:흐림)';
COMMENT ON COLUMN TM_WEATHER_HOURLY.PTY_CD IS '강수형태코드 (0:없음,1:비,2:비/눈,3:눈,4:소나기)';
COMMENT ON COLUMN TM_WEATHER_HOURLY.FCST_DT IS '예보 발표시각';
COMMENT ON COLUMN TM_WEATHER_HOURLY.BASE_DATE IS '예보 기준일';
COMMENT ON COLUMN TM_WEATHER_HOURLY.BASE_TIME IS '예보 기준시간';


-- =====================================================
-- TA_FARM 좌표 컬럼 (참고용)
-- 이미 pig3.1 스키마에 존재하는 컬럼
-- =====================================================
/*
- MAP_X, MAP_Y              : 상세 좌표 (건물 단위)
- MAP_X_N, MAP_Y_N          : 읍면동 대표 좌표
- WEATHER_NX_N, WEATHER_NY_N: 기상청 격자 좌표 (MAP_X_N/Y_N에서 Lambert 변환)

-- 날씨 조회용 인덱스
CREATE INDEX IDX_TA_FARM_WEATHER ON TA_FARM(WEATHER_NX_N, WEATHER_NY_N);
*/


-- =====================================================
-- 조회 예시
-- =====================================================

-- 1. 특정 농장의 일주일 날씨 조회 (읍면동 주소 포함)
/*
SELECT F.FARM_NO, F.FARM_NM, F.ADDR1 AS 농장주소,
       W.WK_DATE, W.WEATHER_NM, W.TEMP_HIGH, W.TEMP_LOW, W.RAIN_PROB
FROM TA_FARM F
JOIN TM_WEATHER W ON W.NX = F.WEATHER_NX_N AND W.NY = F.WEATHER_NY_N
WHERE F.FARM_NO = :P_FARM_NO
  AND W.WK_DATE BETWEEN TO_CHAR(SYSDATE, 'YYYYMMDD')
                    AND TO_CHAR(SYSDATE + 6, 'YYYYMMDD')
ORDER BY W.WK_DATE;
*/

-- 2. 오늘 시간별 날씨 조회
/*
SELECT WK_TIME, TEMP, RAIN_PROB, WEATHER_NM
FROM TM_WEATHER_HOURLY H
JOIN TA_FARM F ON H.NX = F.WEATHER_NX_N AND H.NY = F.WEATHER_NY_N
WHERE F.FARM_NO = :P_FARM_NO
  AND H.WK_DATE = TO_CHAR(SYSDATE, 'YYYYMMDD')
ORDER BY H.WK_TIME;
*/

-- 3. 동일 격자 내 농장 수 확인
/*
SELECT WEATHER_NX_N, WEATHER_NY_N, COUNT(*) AS FARM_CNT
FROM TA_FARM
WHERE WEATHER_NX_N IS NOT NULL
GROUP BY WEATHER_NX_N, WEATHER_NY_N
HAVING COUNT(*) > 1
ORDER BY FARM_CNT DESC;
*/


-- =====================================================
-- 기존 테이블 마이그레이션 (불필요 컬럼 제거)
-- =====================================================
/*
-- 1. TM_WEATHER에서 불필요 컬럼 삭제
ALTER TABLE TM_WEATHER DROP COLUMN SIDO_CD;
ALTER TABLE TM_WEATHER DROP COLUMN SIDO_NM;
ALTER TABLE TM_WEATHER DROP COLUMN SIGUN_CD;
ALTER TABLE TM_WEATHER DROP COLUMN SIGUN_NM;
ALTER TABLE TM_WEATHER DROP COLUMN MAP_X;
ALTER TABLE TM_WEATHER DROP COLUMN MAP_Y;

-- 2. 불필요한 인덱스 삭제
DROP INDEX IDX_TM_WEATHER_01;  -- (SIDO_CD, SIGUN_CD, WK_DATE) 더 이상 불필요

-- 3. TM_WEATHER_HOURLY에서 불필요 컬럼 삭제
ALTER TABLE TM_WEATHER_HOURLY DROP COLUMN WEATHER_SEQ;

-- 4. 불필요한 인덱스 삭제
DROP INDEX IDX_TM_WEATHER_HOURLY_02;  -- (WEATHER_SEQ) 더 이상 불필요
*/
