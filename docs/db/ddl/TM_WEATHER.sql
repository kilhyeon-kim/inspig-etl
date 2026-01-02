-- =====================================================
-- TM_WEATHER: 일별 날씨 데이터 테이블
-- 기상청 단기예보 API 수집 데이터 저장
--
-- 격자(NX, NY) 기준:
--   - 기상청 Lambert 격자 (5km x 5km)
--   - 다수 농장 → 1개 날씨 데이터 (N:1)
--
-- 연결:
--   - TA_FARM.WEATHER_NX, WEATHER_NY 조인
-- =====================================================

CREATE TABLE TM_WEATHER (
    SEQ             NUMBER NOT NULL,
    WK_DATE         VARCHAR2(8) NOT NULL,       -- YYYYMMDD (예보일)

    -- 기상청 격자 좌표 (UK 역할)
    NX              INTEGER NOT NULL,           -- 격자 X (5km)
    NY              INTEGER NOT NULL,           -- 격자 Y (5km)

    -- 지역 정보 (대표값, 참고용)
    SIDO_CD         VARCHAR2(6),                -- 시도코드
    SIDO_NM         VARCHAR2(50),               -- 시도명
    SIGUN_CD        VARCHAR2(6),                -- 시군구코드
    SIGUN_NM        VARCHAR2(100),              -- 시군구명

    -- WGS84 좌표 (격자 중심점)
    MAP_X           VARCHAR2(20),               -- 경도 (longitude)
    MAP_Y           VARCHAR2(20),               -- 위도 (latitude)

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
CREATE INDEX IDX_TM_WEATHER_01 ON TM_WEATHER(SIDO_CD, SIGUN_CD, WK_DATE);
CREATE INDEX IDX_TM_WEATHER_02 ON TM_WEATHER(WK_DATE);

-- 시퀀스
CREATE SEQUENCE SEQ_TM_WEATHER START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;

-- 테이블 COMMENT
COMMENT ON TABLE TM_WEATHER IS '일별 날씨 데이터 (기상청 단기예보)';
COMMENT ON COLUMN TM_WEATHER.SEQ IS '일련번호';
COMMENT ON COLUMN TM_WEATHER.WK_DATE IS '예보일 (YYYYMMDD)';
COMMENT ON COLUMN TM_WEATHER.NX IS '기상청 격자 X좌표 (5km 단위)';
COMMENT ON COLUMN TM_WEATHER.NY IS '기상청 격자 Y좌표 (5km 단위)';
COMMENT ON COLUMN TM_WEATHER.SIDO_CD IS '시도코드';
COMMENT ON COLUMN TM_WEATHER.SIDO_NM IS '시도명';
COMMENT ON COLUMN TM_WEATHER.SIGUN_CD IS '시군구코드';
COMMENT ON COLUMN TM_WEATHER.SIGUN_NM IS '시군구명';
COMMENT ON COLUMN TM_WEATHER.MAP_X IS '경도 (longitude)';
COMMENT ON COLUMN TM_WEATHER.MAP_Y IS '위도 (latitude)';
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
    WEATHER_SEQ     NUMBER,                     -- FK → TM_WEATHER.SEQ (optional)
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
CREATE INDEX IDX_TM_WEATHER_HOURLY_02 ON TM_WEATHER_HOURLY(WEATHER_SEQ);

-- 시퀀스
CREATE SEQUENCE SEQ_TM_WEATHER_HOURLY START WITH 1 INCREMENT BY 1 NOCACHE NOCYCLE;

-- 테이블 COMMENT
COMMENT ON TABLE TM_WEATHER_HOURLY IS '시간별 날씨 데이터 (기상청 단기예보)';
COMMENT ON COLUMN TM_WEATHER_HOURLY.SEQ IS '일련번호';
COMMENT ON COLUMN TM_WEATHER_HOURLY.WEATHER_SEQ IS 'TM_WEATHER.SEQ (FK, optional)';
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
-- TA_FARM 확장: 기상청 격자 좌표 컬럼 추가
-- =====================================================

ALTER TABLE TA_FARM ADD WEATHER_NX INTEGER;
ALTER TABLE TA_FARM ADD WEATHER_NY INTEGER;

COMMENT ON COLUMN TA_FARM.MAP_X IS 'WGS84 경도 (longitude) - Kakao API로 조회된 좌표';
COMMENT ON COLUMN TA_FARM.MAP_Y IS 'WGS84 위도 (latitude) - Kakao API로 조회된 좌표';
COMMENT ON COLUMN TA_FARM.WEATHER_NX IS '기상청 격자 X좌표 (5km 단위, MAP_X/MAP_Y로부터 변환)';
COMMENT ON COLUMN TA_FARM.WEATHER_NY IS '기상청 격자 Y좌표 (5km 단위, MAP_X/MAP_Y로부터 변환)';

-- 날씨 조회용 인덱스
CREATE INDEX IDX_TA_FARM_WEATHER ON TA_FARM(WEATHER_NX, WEATHER_NY);


-- =====================================================
-- 조회 예시
-- =====================================================

-- 1. 특정 농장의 일주일 날씨 조회
/*
SELECT F.FARM_NO, F.FARM_NM,
       W.WK_DATE, W.WEATHER_NM, W.TEMP_HIGH, W.TEMP_LOW, W.RAIN_PROB
FROM TA_FARM F
JOIN TM_WEATHER W ON W.NX = F.WEATHER_NX AND W.NY = F.WEATHER_NY
WHERE F.FARM_NO = :P_FARM_NO
  AND W.WK_DATE BETWEEN TO_CHAR(SYSDATE, 'YYYYMMDD')
                    AND TO_CHAR(SYSDATE + 6, 'YYYYMMDD')
ORDER BY W.WK_DATE;
*/

-- 2. 오늘 시간별 날씨 조회
/*
SELECT WK_TIME, TEMP, RAIN_PROB, WEATHER_NM
FROM TM_WEATHER_HOURLY
WHERE NX = :P_NX AND NY = :P_NY
  AND WK_DATE = TO_CHAR(SYSDATE, 'YYYYMMDD')
ORDER BY WK_TIME;
*/

-- 3. 동일 격자 내 농장 수 확인
/*
SELECT WEATHER_NX, WEATHER_NY, COUNT(*) AS FARM_CNT
FROM TA_FARM
WHERE WEATHER_NX IS NOT NULL
GROUP BY WEATHER_NX, WEATHER_NY
HAVING COUNT(*) > 1
ORDER BY FARM_CNT DESC;
*/
