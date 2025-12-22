# 주간 리포트 (Weekly Report)

DAY_GB: `WEEK`

## 1. 개요

주간 리포트는 매주 월요일 새벽 2시에 실행되어 지난주(월~일)의 농장 생산 데이터를 집계합니다.

### 1.1 실행 주기
- **스케줄**: 매주 월요일 02:00
- **대상 기간**: 지난주 월요일 ~ 일요일
- **대상 농장**: TS_INS_SERVICE.REG_TYPE = 'AUTO'인 농장

### 1.2 날짜 계산 예시 (2025-12-22 월요일 실행 시)
```
지난주 (리포트 대상): 2025-12-15(월) ~ 2025-12-21(일)
금주 (예정 작업):      2025-12-22(월) ~ 2025-12-28(일)
```


## 2. 실행 흐름

```
run_etl.py weekly
       │
       ▼
WeeklyReportOrchestrator.run()
       │
       ├──▶ Step 1: 생산성 데이터 수집 (ProductivityCollector) - 현재 스킵
       │
       ├──▶ Step 2: 기상청 데이터 수집 (WeatherCollector)
       │
       └──▶ Step 3: 주간 리포트 생성
              │
              ├── 전국 탕박 평균 단가 계산
              ├── TS_INS_MASTER 생성
              ├── 대상 농장 조회
              │
              └── 농장별 병렬 처리 (ThreadPoolExecutor)
                     │
                     └──▶ FarmProcessor.process()
                            │
                            ├── FarmDataLoader.load() (데이터 1회 로드)
                            │
                            └── 프로세서 순차 실행 (10개)
                                 ├── 1. ConfigProcessor    (설정값)
                                 ├── 2. AlertProcessor     (관리대상)
                                 ├── 3. ModonProcessor     (모돈현황)
                                 ├── 4. MatingProcessor    (교배)
                                 ├── 5. FarrowingProcessor (분만)
                                 ├── 6. WeaningProcessor   (이유)
                                 ├── 7. AccidentProcessor  (임신사고)
                                 ├── 8. CullingProcessor   (도태폐사)
                                 ├── 9. ShipmentProcessor  (출하)
                                 └── 10. ScheduleProcessor (금주예정)
```


## 3. 프로세서 상세

### 3.1 프로세서 목록

| # | 프로세서 | GUBUN | 설명 | Oracle 원본 |
|---|----------|-------|------|-------------|
| 1 | ConfigProcessor | CONFIG | 농장 설정값 | SP_INS_WEEK_CONFIG |
| 2 | AlertProcessor | MANAGE | 관리대상 모돈 | SP_INS_WEEK_MANAGE_SOW |
| 3 | ModonProcessor | MODON | 모돈현황 통계 | SP_INS_WEEK_MODON |
| 4 | MatingProcessor | MATING | 교배 현황 | SP_INS_WEEK_MATING |
| 5 | FarrowingProcessor | BUN | 분만 현황 | SP_INS_WEEK_BUN |
| 6 | WeaningProcessor | EU | 이유 현황 | SP_INS_WEEK_EU |
| 7 | AccidentProcessor | SAGO | 임신사고 현황 | SP_INS_WEEK_SAGO |
| 8 | CullingProcessor | DOPE | 도태/폐사 현황 | SP_INS_WEEK_DOPE |
| 9 | ShipmentProcessor | SHIP | 출하 현황 | SP_INS_WEEK_SHIP |
| 10 | ScheduleProcessor | SCHEDULE | 금주 예정 작업 | SP_INS_WEEK_SCHEDULE |

### 3.2 GUBUN/SUB_GUBUN 구조

| GUBUN | SUB_GUBUN | 설명 |
|-------|-----------|------|
| CONFIG | CONFIG | 농장 설정값 |
| MANAGE | LIMIT_LIST | 관리대상 모돈 목록 |
| MANAGE | ETC_LIST | 관리대상 기타 목록 |
| MODON | MODON_STAT | 모돈현황 통계 |
| MATING | GB_LIST | 교배 목록 |
| MATING | GB_STAT | 교배 통계 |
| BUN | BM_LIST | 분만 목록 |
| BUN | BM_STAT | 분만 통계 |
| EU | EU_LIST | 이유 목록 |
| EU | EU_STAT | 이유 통계 |
| SAGO | SAGO_LIST | 임신사고 목록 |
| SAGO | SAGO_STAT | 임신사고 통계 |
| DOPE | DOPE_LIST | 도태폐사 목록 |
| DOPE | DOPE_STAT | 도태폐사 통계 |
| SHIP | SHIP_LIST | 출하 목록 |
| SHIP | SHIP_STAT | 출하 통계 |
| SCHEDULE | GB | 분만예정 팝업 |
| SCHEDULE | BM | 발정재귀 팝업 |
| SCHEDULE | EU | 이유예정 팝업 |
| SCHEDULE | VACCINE | 백신예정 팝업 |
| SCHEDULE | HELP | 도움말 정보 |


## 4. 기술 구현

### 4.1 데이터 흐름

```
┌─────────────────────────────────────────────────────────────────┐
│                      FarmProcessor                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. FarmDataLoader.load()                                       │
│     └── Oracle DB에서 모든 원시 데이터 1회 로드                   │
│         ├── 모돈 정보 (TA_MODON)                                 │
│         ├── 작업 이력 (TB_WORK_MODON)                            │
│         ├── 분만 정보 (TB_BUN_MODON)                             │
│         ├── 폐사/사고 정보 (TB_DEAD_MODON)                       │
│         └── 기타 참조 테이블                                     │
│                                                                 │
│  2. 프로세서 순차 실행                                           │
│     └── 각 프로세서는 로드된 데이터를 Python으로 가공             │
│         ├── filter_by_period()                                  │
│         ├── group_by()                                          │
│         ├── sum_field(), count()                                │
│         └── pivot_data()                                        │
│                                                                 │
│  3. 결과 저장                                                    │
│     └── TS_INS_WEEK, TS_INS_WEEK_SUB INSERT/UPDATE              │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 4.2 병렬 처리 구조

```
Level 1: 농장별 병렬 (ThreadPoolExecutor)
         max_farm_workers = 4
         │
         ├── Farm A ──┬── Processor 1~10
         │
         ├── Farm B ──┬── Processor 1~10
         │
         └── Farm C ──┬── ...
```

### 4.3 BaseProcessor 주요 메서드

#### 데이터 조회/저장

| 메서드 | 설명 |
|--------|------|
| `fetch_all(sql, params)` | SELECT 결과를 튜플 리스트로 반환 |
| `fetch_dict(sql, params)` | SELECT 결과를 딕셔너리 리스트로 반환 |
| `execute(sql, params)` | INSERT/UPDATE/DELETE 실행 |
| `save_sub(sub_type, data)` | TS_INS_WEEK_SUB 저장 |
| `update_week(updates)` | TS_INS_WEEK 업데이트 |

#### Python 데이터 가공

| 메서드 | 설명 |
|--------|------|
| `filter_by_period(data, date_field, dt_from, dt_to)` | 기간 필터링 |
| `filter_by_code(data, code_field, code_value)` | 코드값 필터링 |
| `group_by(data, key_field)` | 단일 필드 그룹핑 |
| `count(data)` / `sum_field(data, field)` | 집계 |
| `pivot_data(data, row_key, col_key, value_field, agg)` | 피벗 변환 |


## 5. 프로세서별 특이사항

### 5.1 WeaningProcessor (이유)

#### DAERI_YN 분기 처리
대리모돈 자돈 증감 계산 시 **ETL 수행일(SYSDATE)이 아닌 dt_to(지난주 일요일)** 기준 사용

```sql
AND JT.WK_DT <= CASE
    WHEN NW.NEXT_WK_GUBUN = 'G' THEN NW.NEXT_WK_DT
    WHEN NW.NEXT_WK_DT IS NULL AND A.DAERI_YN = 'N' THEN :dt_to  -- 여기!
    ELSE TO_CHAR(TO_DATE(A.WK_DT, 'YYYYMMDD') - 1, 'YYYYMMDD')
END
```

### 5.2 ScheduleProcessor (금주 예정)

#### 팝업 종류별 처리
```python
# GB, BM, EU는 공통 메서드
popup_configs = [
    ('GB', '150005'),   # 분만예정
    ('BM', '150002'),   # 발정재귀
    ('EU', '150003'),   # 이유예정
]

for sub_gubun, job_gubun_cd in popup_configs:
    self._insert_popup_by_job(sub_gubun, job_gubun_cd, ...)

# VACCINE은 ARTICLE_NM(백신명) 포함으로 별도 처리
self._insert_vaccine_popup(...)
```

### 5.3 CullingProcessor (도태/폐사)

#### 원인별 피벗 구조
DOPE_GUBUN_CD별 CNT를 피벗하여 저장
- 결과: CNT_1(050011), CNT_2(050012), ... CNT_10(050020)


## 6. Oracle Function 연동

### FN_MD_SCHEDULE_BSE_2020 호출
```python
sql = """
SELECT WK_NM, PIG_NO, MODON_STATUS_CD, PASS_DAY, PASS_DT
FROM TABLE(FN_MD_SCHEDULE_BSE_2020(
    :farm_no, 'JOB-DAJANG', '150004', NULL,
    :v_sdt, :v_edt, NULL, 'ko', 'yyyy-MM-dd', '-1', NULL
))
"""
result = self.fetch_dict(sql, {...})
```


## 7. 에러 처리

### 농장별 에러 격리
```python
class FarmProcessor:
    def process(self, ...):
        try:
            # 처리 로직
            self._update_status('COMPLETE')
        except Exception as e:
            # 해당 농장만 ERROR 상태로 기록
            self._update_status('ERROR')
            self._log_error(str(e))
            return {'status': 'error', 'error': str(e)}
```

### 에러 로그 테이블 (TS_INS_JOB_LOG)
```sql
INSERT INTO TS_INS_JOB_LOG (
    SEQ, MASTER_SEQ, FARM_NO, JOB_NM, PROC_NM,
    STATUS_CD, ERROR_MSG, LOG_INS_DT
) VALUES (
    SEQ_TS_INS_JOB_LOG.NEXTVAL, :master_seq, :farm_no,
    'PYTHON_ETL', 'FarmProcessor',
    'ERROR', :error_msg, SYSDATE
)
```


## 8. Oracle → Python 전환 매핑

| Oracle Procedure | Python Class | 상태 |
|------------------|--------------|------|
| SP_INS_WEEK_MAIN | WeeklyReportOrchestrator | 완료 |
| SP_INS_WEEK_FARM_PROCESS | FarmProcessor | 완료 |
| SP_INS_WEEK_CONFIG | ConfigProcessor | 완료 |
| SP_INS_WEEK_MANAGE_SOW | AlertProcessor | 완료 |
| SP_INS_WEEK_MODON | ModonProcessor | 완료 |
| SP_INS_WEEK_MATING | MatingProcessor | 완료 |
| SP_INS_WEEK_BUN | FarrowingProcessor | 완료 |
| SP_INS_WEEK_EU | WeaningProcessor | 완료 |
| SP_INS_WEEK_SAGO | AccidentProcessor | 완료 |
| SP_INS_WEEK_DOPE | CullingProcessor | 완료 |
| SP_INS_WEEK_SHIP | ShipmentProcessor | 완료 |
| SP_INS_WEEK_SCHEDULE | ScheduleProcessor | 완료 |
