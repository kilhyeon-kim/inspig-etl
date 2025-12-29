-- =====================================================
-- TS_PRODUCTIVITY: 생산성 데이터 저장 테이블
-- 생산성 API에서 수집한 데이터를 PCODE별로 저장
--
-- 기간 구분:
--   - W (주간): 주간보고서용 데이터, PERIOD_NO = 주차 (1~53)
--   - M (월간): 월간보고서용 데이터, PERIOD_NO = 월 (1~12)
--   - Q (분기): 분기보고서용 데이터, PERIOD_NO = 분기 (1~4)
--
-- 테이블 구조:
--   - 농장당 PCODE별 1행씩 저장 (5개 PCODE x 기간종류)
--   - 컬럼명: C + 뒷자리 3자리 (예: 031001 -> C001)
--
-- TC_CODE_SYS 참조 (웹 시스템 구동시 캐시에 로드):
--   조인조건: PCODE IN ('031','032','033','034','035'), CODE=PCODE||컬럼뒷3자리
--   CNAME    = 통계명 (STAT_NM)
--   HELP_MSG = {"tooltip":"설명"} (JSON 형식)
-- =====================================================

CREATE TABLE TS_PRODUCTIVITY (
    SEQ             NUMBER(12) NOT NULL,
    FARM_NO         NUMBER(10) NOT NULL,
    PCODE           VARCHAR2(3) NOT NULL,              -- 031:교배, 032:분만, 033:이유, 034:번식종합, 035:모돈현황
    STAT_YEAR       NUMBER(4) NOT NULL,                -- 통계년도 (YYYY)
    PERIOD          VARCHAR2(1) NOT NULL,              -- 기간구분 (W:주간, M:월간, Q:분기)
    PERIOD_NO       NUMBER(2) NOT NULL,                -- 기간차수 (W:1~53, M:1~12, Q:1~4)
    STAT_DATE       VARCHAR2(10) NOT NULL,             -- 통계기준일 (YYYY-MM-DD)
    -- 031: 교배 (28개)
    C001            NUMBER(15,4),                      -- 031001: 교배복수
    C013            NUMBER(15,4),                      -- 031013: 정상교배
    C014            NUMBER(15,4),                      -- 031014: 1차재발교배
    C015            NUMBER(15,4),                      -- 031015: 2차재발교배
    C016            NUMBER(15,4),                      -- 031016: 기타사고후교배
    C017            NUMBER(15,4),                      -- 031017: 미경산돈교배복수
    C018            NUMBER(15,4),                      -- 031018: 미경산정상교배
    C019            NUMBER(15,4),                      -- 031019: 미경산재발교배
    C020            NUMBER(15,4),                      -- 031020: 미경산기타사고후교배
    C021            NUMBER(15,4),                      -- 031021: 경산돈정상교배
    C022            NUMBER(15,4),                      -- 031022: 경산돈재발교배
    C023            NUMBER(15,4),                      -- 031023: 경산돈기타사고후교배
    C024            NUMBER(15,4),                      -- 031024: 초교배복수(모돈편입)
    C025            NUMBER(15,4),                      -- 031025: 평균초교배일령
    C026            NUMBER(15,4),                      -- 031026: 재귀발정계산교배모돈수
    C027            NUMBER(15,4),                      -- 031027: 총재귀일수
    C028            NUMBER(15,4),                      -- 031028: 평균재귀발정일령
    C029            NUMBER(15,4),                      -- 031029: 3일내재귀복수
    C030            NUMBER(15,4),                      -- 031030: 4일재귀복수
    C031            NUMBER(15,4),                      -- 031031: 5일재귀복수
    C032            NUMBER(15,4),                      -- 031032: 6일재귀복수
    C033            NUMBER(15,4),                      -- 031033: 7일재귀복수
    C034            NUMBER(15,4),                      -- 031034: 8일재귀복수
    C035            NUMBER(15,4),                      -- 031035: 9일재귀복수
    C036            NUMBER(15,4),                      -- 031036: 10일이상재귀복수
    C037            NUMBER(15,4),                      -- 031037: 7일내재귀율
    C038            NUMBER(15,4),                      -- 031038: 4~6일재귀율
    C039            NUMBER(15,4),                      -- 031039: 재발교배비율
    -- 032: 분만 (24개) - 동일 컬럼명 사용
    -- C001: 분만예정복수, C010~C043
    -- 033: 이유 (18개) - 동일 컬럼명 사용
    -- C001~C022
    -- 034: 번식종합 (30개) - 동일 컬럼명 사용
    -- C001~C032
    -- 035: 모돈현황 (13개) - 동일 컬럼명 사용
    -- C001~C024
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

COMMENT ON TABLE TS_PRODUCTIVITY IS '생산성 데이터 (외부 API 수집)';
COMMENT ON COLUMN TS_PRODUCTIVITY.PCODE IS 'PCODE (031:교배, 032:분만, 033:이유, 034:번식종합, 035:모돈현황)';
COMMENT ON COLUMN TS_PRODUCTIVITY.STAT_YEAR IS '통계년도 (YYYY)';
COMMENT ON COLUMN TS_PRODUCTIVITY.PERIOD IS '기간구분 (W:주간, M:월간, Q:분기)';
COMMENT ON COLUMN TS_PRODUCTIVITY.PERIOD_NO IS '기간차수 (W:1~53주, M:1~12월, Q:1~4분기)';
COMMENT ON COLUMN TS_PRODUCTIVITY.STAT_DATE IS '통계기준일 (YYYY-MM-DD)';
