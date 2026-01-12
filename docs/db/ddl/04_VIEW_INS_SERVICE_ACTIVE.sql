-- ============================================================================
-- VW_INS_SERVICE_ACTIVE: 현재 유효한 인사이트피그 서비스 농장 View
-- ============================================================================
-- 용도: TS_INS_SERVICE에서 현재 유효한 최신 구독만 조회
-- 공통 사용처:
--   - inspig (NestJS): 로그인 권한 확인, 배치 대상 농장 조회
--   - inspig-etl (Python): ETL 대상 농장 조회
--   - pig3.1 (Java): 알림톡 발송 대상 조회
--
-- PK: FARM_NO + INSPIG_REG_DT (이력 관리)
-- 유효 조건:
--   1. INSPIG_YN = 'Y' (서비스 사용)
--   2. USE_YN = 'Y' (레코드 유효)
--   3. 현재일 >= INSPIG_FROM_DT (시작일 이후)
--   4. 현재일 <= LEAST(INSPIG_TO_DT, NVL(INSPIG_STOP_DT, '99991231')) (종료일/중지일 이전)
--   5. 같은 농장 중 유효한 최신 건만 (INSPIG_REG_DT 기준)
--
-- 주의: DB가 UTC이므로 SYSDATE + 9/24로 KST 변환
--
-- 사용 예시:
--   1. 전체 유효 농장 조회 (로그인, 알림톡 등)
--      SELECT * FROM VW_INS_SERVICE_ACTIVE;
--
--   2. 정기 배치 대상만 (REG_TYPE='AUTO')
--      SELECT * FROM VW_INS_SERVICE_ACTIVE WHERE NVL(REG_TYPE, 'AUTO') = 'AUTO';
--
--   3. 특정 농장 유효성 확인
--      SELECT * FROM VW_INS_SERVICE_ACTIVE WHERE FARM_NO = :farmNo;
--
-- 생성일: 2026-01-12
-- ============================================================================

CREATE OR REPLACE VIEW VW_INS_SERVICE_ACTIVE AS
SELECT
    S1.FARM_NO,
    S1.INSPIG_YN,
    S1.INSPIG_REG_DT,
    S1.INSPIG_FROM_DT,
    S1.INSPIG_TO_DT,
    S1.INSPIG_STOP_DT,
    S1.WEB_PAY_YN,
    S1.REG_TYPE,
    S1.USE_YN,
    S1.BIGO,
    S1.LOG_INS_DT,
    S1.LOG_UPT_DT
FROM TS_INS_SERVICE S1
WHERE S1.INSPIG_YN = 'Y'
  AND S1.USE_YN = 'Y'
  AND S1.INSPIG_FROM_DT IS NOT NULL
  AND S1.INSPIG_TO_DT IS NOT NULL
  AND TO_CHAR(SYSDATE + 9/24, 'YYYYMMDD') >= S1.INSPIG_FROM_DT
  AND TO_CHAR(SYSDATE + 9/24, 'YYYYMMDD') <= LEAST(
      S1.INSPIG_TO_DT,
      NVL(S1.INSPIG_STOP_DT, '99991231')
  )
  AND S1.INSPIG_REG_DT = (
      SELECT MAX(S2.INSPIG_REG_DT)
      FROM TS_INS_SERVICE S2
      WHERE S2.FARM_NO = S1.FARM_NO
        AND S2.INSPIG_YN = 'Y'
        AND S2.USE_YN = 'Y'
        AND S2.INSPIG_FROM_DT IS NOT NULL
        AND S2.INSPIG_TO_DT IS NOT NULL
        AND TO_CHAR(SYSDATE + 9/24, 'YYYYMMDD') >= S2.INSPIG_FROM_DT
        AND TO_CHAR(SYSDATE + 9/24, 'YYYYMMDD') <= LEAST(
            S2.INSPIG_TO_DT,
            NVL(S2.INSPIG_STOP_DT, '99991231')
        )
  );

-- 권한 부여 (필요시)
-- GRANT SELECT ON VW_INS_SERVICE_ACTIVE TO PUBLIC;

COMMENT ON TABLE VW_INS_SERVICE_ACTIVE IS '현재 유효한 인사이트피그 서비스 농장 View';
