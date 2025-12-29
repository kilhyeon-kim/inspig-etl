-- ================================================================
-- TS_INS_MGMT: 주간리포트 관리포인트 테이블
-- 용도: 퀴즈, 중점사항, 추천학습자료 등 관리 콘텐츠 저장
-- ================================================================

-- DROP TABLE TS_INS_MGMT;

CREATE TABLE TS_INS_MGMT (
    MASTER_SEQ    NUMBER(10)      NOT NULL,     -- 마스터 SEQ (FK: TS_INS_MASTER)
    MGMT_TYPE     VARCHAR2(20)    NOT NULL,     -- 유형 (QUIZ: 퀴즈, HIGHLIGHT: 중점사항, RECOMMEND: 추천학습자료)
    SORT_NO       NUMBER(3)       NOT NULL,     -- 정렬순서
    TITLE         VARCHAR2(200)   NOT NULL,     -- 카드에 표시될 제목 (한줄)
    CONTENT       VARCHAR2(4000),               -- 상세 내용 (팝업에 표시)
    LINK_URL      VARCHAR2(500),                -- 링크 URL
    LINK_TARGET   VARCHAR2(10)    DEFAULT 'POPUP',  -- 링크 열기 방식 (POPUP: 팝업으로 열기, DIRECT: 바로 이동)
    POST_FROM     VARCHAR2(8),                  -- 게시 시작일 (YYYYMMDD)
    POST_TO       VARCHAR2(8),                  -- 게시 종료일 (YYYYMMDD)
    USE_YN        CHAR(1)         DEFAULT 'Y',  -- 사용여부
    REG_DT        DATE            DEFAULT SYSDATE,  -- 등록일시
    UPD_DT        DATE,                         -- 수정일시
    CONSTRAINT PK_TS_INS_MGMT PRIMARY KEY (MASTER_SEQ, MGMT_TYPE, SORT_NO)
);

COMMENT ON TABLE TS_INS_MGMT IS '주간리포트 관리포인트';
COMMENT ON COLUMN TS_INS_MGMT.MASTER_SEQ IS '마스터 SEQ (FK: TS_INS_MASTER)';
COMMENT ON COLUMN TS_INS_MGMT.MGMT_TYPE IS '유형 (QUIZ: 퀴즈, CHANNEL: 안박사 채널/정보, PORK-NEWS: 한돈업계소식)';
COMMENT ON COLUMN TS_INS_MGMT.SORT_NO IS '정렬순서';
COMMENT ON COLUMN TS_INS_MGMT.TITLE IS '카드에 표시될 제목 (한줄)';
COMMENT ON COLUMN TS_INS_MGMT.CONTENT IS '상세 내용 (팝업에 표시)';
COMMENT ON COLUMN TS_INS_MGMT.LINK_URL IS '링크 URL';
COMMENT ON COLUMN TS_INS_MGMT.LINK_TARGET IS '링크 열기 방식 (POPUP: 팝업으로 열기, DIRECT: 바로 이동)';
COMMENT ON COLUMN TS_INS_MGMT.POST_FROM IS '게시 시작일 (YYYYMMDD)';
COMMENT ON COLUMN TS_INS_MGMT.POST_TO IS '게시 종료일 (YYYYMMDD)';
COMMENT ON COLUMN TS_INS_MGMT.USE_YN IS '사용여부 (Y/N)';
COMMENT ON COLUMN TS_INS_MGMT.REG_DT IS '등록일시';
COMMENT ON COLUMN TS_INS_MGMT.UPD_DT IS '수정일시';

-- 인덱스 (마스터 SEQ로 조회)
CREATE INDEX IX_TS_INS_MGMT_01 ON TS_INS_MGMT (MASTER_SEQ);

-- ================================================================
-- ALTER 문 (기존 테이블에 컬럼 추가시)
-- ================================================================
-- ALTER TABLE TS_INS_MGMT ADD TITLE VARCHAR2(200);
-- ALTER TABLE TS_INS_MGMT ADD LINK_TARGET VARCHAR2(10) DEFAULT 'POPUP';
-- ALTER TABLE TS_INS_MGMT ADD POST_FROM VARCHAR2(8);
-- ALTER TABLE TS_INS_MGMT ADD POST_TO VARCHAR2(8);
-- ALTER TABLE TS_INS_MGMT ADD USE_YN CHAR(1) DEFAULT 'Y';
-- UPDATE TS_INS_MGMT SET TITLE = CONTENT WHERE TITLE IS NULL;
-- ALTER TABLE TS_INS_MGMT MODIFY TITLE NOT NULL;

-- ================================================================
-- 테스트 데이터
-- ================================================================
-- QUIZ (퀴즈)
INSERT INTO TS_INS_MGMT (MASTER_SEQ, MGMT_TYPE, SORT_NO, TITLE, CONTENT, LINK_URL, LINK_TARGET, POST_FROM, POST_TO)
VALUES (1, 'QUIZ', 1, '겨울철 돼지 관리 퀴즈', '겨울철 돼지 관리에 대한 퀴즈입니다. 아래 링크에서 참여해주세요.', 'https://example.com/quiz/winter', 'POPUP', '20251201', '20251231');

INSERT INTO TS_INS_MGMT (MASTER_SEQ, MGMT_TYPE, SORT_NO, TITLE, CONTENT, LINK_URL, LINK_TARGET, POST_FROM, POST_TO)
VALUES (1, 'QUIZ', 2, '번식성적 향상 퀴즈', '번식성적 향상을 위한 퀴즈에 참여해보세요.', 'https://example.com/quiz/breeding', 'POPUP', '20251201', '20251231');

-- HIGHLIGHT (중점사항)
INSERT INTO TS_INS_MGMT (MASTER_SEQ, MGMT_TYPE, SORT_NO, TITLE, CONTENT, LINK_URL, LINK_TARGET, POST_FROM, POST_TO)
VALUES (1, 'HIGHLIGHT', 1, '돈사 환기관리 강화', '겨울철 환기 부족으로 인한 호흡기 질병 예방을 위해 최소 환기량을 유지해주세요. 특히 새벽 시간대 암모니아 농도 관리가 중요합니다.', NULL, NULL, '20251201', '20251231');

INSERT INTO TS_INS_MGMT (MASTER_SEQ, MGMT_TYPE, SORT_NO, TITLE, CONTENT, LINK_URL, LINK_TARGET, POST_FROM, POST_TO)
VALUES (1, 'HIGHLIGHT', 2, '급수기 동파 예방', '영하의 날씨에는 급수기 동파 위험이 있습니다. 보온재 설치 및 야간 점등을 통해 동파를 예방해주세요.', NULL, NULL, '20251201', '20251231');

INSERT INTO TS_INS_MGMT (MASTER_SEQ, MGMT_TYPE, SORT_NO, TITLE, CONTENT, LINK_URL, LINK_TARGET, POST_FROM, POST_TO)
VALUES (1, 'HIGHLIGHT', 3, '백신 접종 일정 확인', '12월 백신 접종 일정을 확인하고, 누락되지 않도록 주의해주세요.', 'https://example.com/vaccine-schedule', 'DIRECT', '20251201', '20251231');

-- RECOMMEND (추천학습자료)
INSERT INTO TS_INS_MGMT (MASTER_SEQ, MGMT_TYPE, SORT_NO, TITLE, CONTENT, LINK_URL, LINK_TARGET, POST_FROM, POST_TO)
VALUES (1, 'RECOMMEND', 1, '겨울철 양돈 관리 가이드', '겨울철 양돈 관리의 핵심 포인트를 정리한 가이드입니다.', 'https://example.com/guide/winter', 'POPUP', '20251201', '20251231');

INSERT INTO TS_INS_MGMT (MASTER_SEQ, MGMT_TYPE, SORT_NO, TITLE, CONTENT, LINK_URL, LINK_TARGET, POST_FROM, POST_TO)
VALUES (1, 'RECOMMEND', 2, 'PSY 향상 전략 웨비나', 'PSY 향상을 위한 실전 전략 웨비나 영상입니다.', 'https://example.com/webinar/psy', 'POPUP', '20251201', '20251231');

COMMIT;
