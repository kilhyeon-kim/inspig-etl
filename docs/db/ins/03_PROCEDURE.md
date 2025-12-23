# ETL 공통 프로시저

> InsightPig ETL 로그 기록용 공통 프로시저

---

## 프로시저 목록

| 프로시저명 | 용도 |
|------------|------|
| SP_INS_COM_LOG_START | 로그 시작 기록 |
| SP_INS_COM_LOG_END | 로그 종료 (성공) |
| SP_INS_COM_LOG_ERROR | 로그 종료 (오류) |
| SP_INS_COM_LOG_CLEAR | 6개월 이전 로그 삭제 |

---

## 시간 저장 원칙

- **저장**: SYSDATE (서버시간/UTC)
- **비교**: `SF_GET_LOCALE_VW_DATE_2022(LOCALE, SYSDATE)`
  - `KOR`: 한국 +09:00
  - `VNM`: 베트남 +07:00
- **조회**: 애플리케이션에서 로케일 변환

---

## 1. SP_INS_COM_LOG_START

프로시저 시작 시 RUNNING 상태로 로그 기록

```sql
CREATE OR REPLACE PROCEDURE SP_INS_COM_LOG_START (
    P_MASTER_SEQ    IN  NUMBER,         -- 마스터 시퀀스
    P_JOB_NM        IN  VARCHAR2,       -- JOB명
    P_PROC_NM       IN  VARCHAR2,       -- 프로시저명
    P_FARM_NO       IN  INTEGER DEFAULT NULL,
    P_LOG_SEQ       OUT NUMBER,         -- 반환값
    P_DAY_GB        IN  VARCHAR2 DEFAULT NULL,
    P_REPORT_YEAR   IN  NUMBER DEFAULT NULL,
    P_REPORT_WEEK_NO IN NUMBER DEFAULT NULL
) AS
BEGIN
    SELECT SEQ_TS_INS_JOB_LOG.NEXTVAL INTO P_LOG_SEQ FROM DUAL;

    INSERT INTO TS_INS_JOB_LOG (
        SEQ, MASTER_SEQ, JOB_NM, PROC_NM, FARM_NO,
        DAY_GB, REPORT_YEAR, REPORT_WEEK_NO,
        STATUS_CD, START_DT
    ) VALUES (
        P_LOG_SEQ, P_MASTER_SEQ, P_JOB_NM, P_PROC_NM, P_FARM_NO,
        P_DAY_GB, P_REPORT_YEAR, P_REPORT_WEEK_NO,
        'RUNNING', SYSDATE
    );
    COMMIT;
END;
```

---

## 2. SP_INS_COM_LOG_END

정상 완료 시 SUCCESS 상태로 업데이트

```sql
CREATE OR REPLACE PROCEDURE SP_INS_COM_LOG_END (
    P_LOG_SEQ       IN  NUMBER,
    P_PROC_CNT      IN  INTEGER DEFAULT 0
) AS
    V_START_DT DATE;
BEGIN
    SELECT START_DT INTO V_START_DT
    FROM TS_INS_JOB_LOG WHERE SEQ = P_LOG_SEQ;

    UPDATE TS_INS_JOB_LOG
    SET STATUS_CD = 'SUCCESS',
        END_DT = SYSDATE,
        ELAPSED_MS = ROUND((SYSDATE - V_START_DT) * 24 * 60 * 60 * 1000),
        PROC_CNT = P_PROC_CNT
    WHERE SEQ = P_LOG_SEQ;
    COMMIT;
END;
```

---

## 3. SP_INS_COM_LOG_ERROR

오류 발생 시 ERROR 상태로 업데이트

```sql
CREATE OR REPLACE PROCEDURE SP_INS_COM_LOG_ERROR (
    P_LOG_SEQ       IN  NUMBER,
    P_ERROR_CD      IN  VARCHAR2,       -- SQLCODE
    P_ERROR_MSG     IN  VARCHAR2        -- SQLERRM
) AS
    V_START_DT DATE;
BEGIN
    SELECT START_DT INTO V_START_DT
    FROM TS_INS_JOB_LOG WHERE SEQ = P_LOG_SEQ;

    UPDATE TS_INS_JOB_LOG
    SET STATUS_CD = 'ERROR',
        END_DT = SYSDATE,
        ELAPSED_MS = ROUND((SYSDATE - V_START_DT) * 24 * 60 * 60 * 1000),
        ERROR_CD = P_ERROR_CD,
        ERROR_MSG = SUBSTR(P_ERROR_MSG, 1, 4000)
    WHERE SEQ = P_LOG_SEQ;
    COMMIT;
END;
```

---

## 4. SP_INS_COM_LOG_CLEAR

6개월 이전 로그 삭제

```sql
CREATE OR REPLACE PROCEDURE SP_INS_COM_LOG_CLEAR AS
    V_DEL_CNT INTEGER;
BEGIN
    DELETE FROM TS_INS_JOB_LOG
    WHERE START_DT < ADD_MONTHS(TRUNC(SYSDATE), -6);

    V_DEL_CNT := SQL%ROWCOUNT;
    COMMIT;

    IF V_DEL_CNT > 0 THEN
        DBMS_OUTPUT.PUT_LINE('TS_INS_JOB_LOG 삭제: ' || V_DEL_CNT || '건');
    END IF;
END;
```

---

## Python ETL 대응 코드

Python ETL에서는 이 프로시저를 직접 호출하거나, 동일한 로직을 Python으로 구현

### BaseProcessor에서 로그 기록

```python
class BaseProcessor:
    def log_start(self) -> int:
        """로그 시작 기록"""
        sql = """
        INSERT INTO TS_INS_JOB_LOG (
            SEQ, MASTER_SEQ, JOB_NM, PROC_NM, FARM_NO,
            DAY_GB, REPORT_YEAR, REPORT_WEEK_NO,
            STATUS_CD, START_DT
        ) VALUES (
            SEQ_TS_INS_JOB_LOG.NEXTVAL, :master_seq, :job_nm, :proc_nm, :farm_no,
            :day_gb, :report_year, :report_week_no,
            'RUNNING', SYSDATE
        ) RETURNING SEQ INTO :log_seq
        """
        # ... 실행 로직

    def log_end(self, log_seq: int, proc_cnt: int = 0):
        """로그 성공 기록"""
        sql = """
        UPDATE TS_INS_JOB_LOG
        SET STATUS_CD = 'SUCCESS',
            END_DT = SYSDATE,
            ELAPSED_MS = ROUND((SYSDATE - START_DT) * 86400000),
            PROC_CNT = :proc_cnt
        WHERE SEQ = :log_seq
        """

    def log_error(self, log_seq: int, error_cd: str, error_msg: str):
        """로그 오류 기록"""
        sql = """
        UPDATE TS_INS_JOB_LOG
        SET STATUS_CD = 'ERROR',
            END_DT = SYSDATE,
            ELAPSED_MS = ROUND((SYSDATE - START_DT) * 86400000),
            ERROR_CD = :error_cd,
            ERROR_MSG = SUBSTR(:error_msg, 1, 4000)
        WHERE SEQ = :log_seq
        """
```

---

## 주간 프로시저 목록 (week 폴더)

Oracle 원본 프로시저 참조용:

| 파일명 | 프로시저명 | Python 대응 |
|--------|------------|-------------|
| 01_SP_INS_WEEK_MAIN.sql | SP_INS_WEEK_MAIN | WeeklyRunner |
| 02_SP_INS_WEEK_CONFIG.sql | SP_INS_WEEK_CONFIG | ConfigProcessor |
| 11_SP_INS_WEEK_MODON_POPUP.sql | SP_INS_WEEK_MODON_POPUP | SowStatusProcessor |
| 12_SP_INS_WEEK_ALERT_POPUP.sql | SP_INS_WEEK_ALERT_POPUP | AlertProcessor |
| 21_SP_INS_WEEK_GB_POPUP.sql | SP_INS_WEEK_GB_POPUP | MatingProcessor |
| 22_SP_INS_WEEK_BM_POPUP.sql | SP_INS_WEEK_BM_POPUP | FarrowingProcessor |
| 23_SP_INS_WEEK_EU_POPUP.sql | SP_INS_WEEK_EU_POPUP | WeaningProcessor |
| 31_SP_INS_WEEK_SG_POPUP.sql | SP_INS_WEEK_SG_POPUP | AccidentProcessor |
| 32_SP_INS_WEEK_DOPE_POPUP.sql | SP_INS_WEEK_DOPE_POPUP | CullingProcessor |
| 41_SP_INS_WEEK_SHIP_POPUP.sql | SP_INS_WEEK_SHIP_POPUP | ShipmentProcessor |
| 51_SP_INS_WEEK_SCHEDULE_POPUP.sql | SP_INS_WEEK_SCHEDULE_POPUP | ScheduleProcessor |
| 99_JOB_INS_WEEKLY.sql | JOB_INS_WEEKLY | Python scheduler |
