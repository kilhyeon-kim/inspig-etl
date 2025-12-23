"""
농장별 데이터 로더
- 주간 리포트 생성에 필요한 모든 원시 데이터를 1회 조회
- 조회된 데이터는 Python 객체로 반환
- 각 프로세서는 이 데이터를 받아서 가공만 수행

목적:
- DB 조회 횟수 최소화 (농장당 1회)
- Oracle 함수 활용으로 정확한 상태코드 계산

v3 아키텍처:
- SF_GET_MODONGB_STATUS Oracle 함수 직접 호출
- VW_MODON_2020_MAX_WK_02 뷰 로직 Python 구현
- MAX(SEQ) 기반 마지막 작업 정보 계산
- 기준일(base_date) 기반 시점 데이터 계산
"""
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ============================================================================
# 상태코드 및 작업구분 상수 정의
# ============================================================================

# 모돈 상태코드 (STATUS_CD)
STATUS_HUBO = '010001'      # 후보돈
STATUS_IMSIN = '010002'     # 임신돈 (G:교배 후)
STATUS_POYU = '010003'      # 포유돈 (B:분만 후)
STATUS_DAERI = '010004'     # 대리모
STATUS_EUMO = '010005'      # 이유모 (E:이유 후)
STATUS_JAEBAL = '010006'    # 재발 (F:사고-재발)
STATUS_YUSAN = '010007'     # 유산 (F:사고-유산)
STATUS_DOPESA = '010008'    # 도폐사

# 작업구분 (WK_GUBUN)
WK_GYOBAE = 'G'     # 교배
WK_BUNMAN = 'B'     # 분만
WK_EU = 'E'         # 이유
WK_SAGO = 'F'       # 사고 (재발/유산)

# 사고구분 코드
SAGO_JAEBAL = '020001'  # 재발
SAGO_YUSAN = '020002'   # 유산


class FarmDataLoader:
    """농장별 원시 데이터 로더 (v3 - Oracle 함수 직접 호출)

    주간 리포트 생성에 필요한 모든 테이블 데이터를 1회 조회하여 반환
    상태코드 계산은 Oracle SF_GET_MODONGB_STATUS 함수 직접 호출

    조회 대상 테이블:
    - TB_MODON: 모돈 기본 정보
    - TB_MODON_WK: 모돈 작업 이력
    - TB_BUNMAN: 분만 정보
    - TB_EU: 이유 정보
    - TB_SAGO: 사고 정보
    - TB_MODON_JADON_TRANS: 자돈 이동 정보
    - TB_MODON_GB: 교배 정보
    - TB_MODON_GB_DETAIL: 교배 상세 정보
    - TM_LPD_DATA: LPD 측정 데이터

    Oracle 함수 활용:
    - SF_GET_MODONGB_STATUS: 상태코드 계산 (SQL에서 직접 호출)

    Python 가공 기능:
    - VW_MODON_2020_MAX_WK_02: MAX(SEQ) 기반 마지막 작업 정보
    - 경과일 계산 (기준일 대비)
    """

    def __init__(self, conn, farm_no: int, dt_from: str, dt_to: str,
                 locale: str = 'KOR', base_date: str = None):
        """
        Args:
            conn: Oracle DB 연결 객체
            farm_no: 농장 번호
            dt_from: 시작일 (YYYYMMDD)
            dt_to: 종료일 (YYYYMMDD)
            locale: 로케일 (KOR, VNM 등)
            base_date: 기준일 (YYYYMMDD) - None이면 dt_to 사용
        """
        self.conn = conn
        self.farm_no = farm_no
        self.dt_from = dt_from
        self.dt_to = dt_to
        self.locale = locale
        self.base_date = base_date or dt_to  # 기준일 (기본: 종료일)
        self.logger = logging.getLogger(f"{__name__}.Farm{farm_no}")

        # 캐시된 데이터
        self._data: Dict[str, Any] = {}
        self._loaded = False

        # 가공된 데이터 캐시
        self._modon_last_wk: Dict[str, Dict] = {}  # MAX(SEQ) 기준 마지막 작업
        self._modon_calc_status: Dict[str, str] = {}  # 계산된 상태코드
        self._modon_last_gb_dt: Dict[str, str] = {}  # 마지막 교배일

    def load(self) -> Dict[str, Any]:
        """모든 원시 데이터 로드 및 Python 가공

        Returns:
            로드된 데이터 딕셔너리:
            {
                'modon': [...],           # 모돈 기본 정보 (상태코드 계산 포함)
                'modon_wk': [...],         # 모돈 작업 이력
                'bunman': [...],           # 분만 정보
                'eu': [...],               # 이유 정보
                'sago': [...],             # 사고 정보
                'jadon_trans': [...],      # 자돈 이동 정보
                'gb': [...],               # 교배 정보
                'gb_detail': [...],        # 교배 상세
                'lpd': [...],              # LPD 측정 데이터
                'meta': {...},             # 메타 정보 (기간, 농장 등)

                # Python 가공 데이터
                'modon_last_wk': {...},    # 모돈별 마지막 작업 (MAX(SEQ))
                'modon_calc_status': {...}, # 모돈별 계산된 상태코드
                'modon_last_gb_dt': {...}, # 모돈별 마지막 교배일
            }
        """
        if self._loaded:
            return self._data

        self.logger.info(f"데이터 로드 시작: 농장={self.farm_no}, 기간={self.dt_from}~{self.dt_to}, 기준일={self.base_date}")

        # 날짜 형식 변환 (YYYYMMDD → YYYY-MM-DD)
        sdt = f"{self.dt_from[:4]}-{self.dt_from[4:6]}-{self.dt_from[6:8]}"
        edt = f"{self.dt_to[:4]}-{self.dt_to[4:6]}-{self.dt_to[6:8]}"

        # 기간 확장 (1개월 전, 1년치 등 프로세서별 필요 범위 고려)
        dt_from_obj = datetime.strptime(self.dt_from, '%Y%m%d')
        dt_to_obj = datetime.strptime(self.dt_to, '%Y%m%d')

        # 연초
        year_start = f"{dt_to_obj.year}0101"

        # 1개월 전
        month_ago = (dt_from_obj - timedelta(days=30)).strftime('%Y%m%d')

        # 메타 정보 저장
        self._data['meta'] = {
            'farm_no': self.farm_no,
            'dt_from': self.dt_from,
            'dt_to': self.dt_to,
            'base_date': self.base_date,
            'sdt': sdt,
            'edt': edt,
            'year_start': year_start,
            'month_ago': month_ago,
            'locale': self.locale,
        }

        # ========================================
        # 1단계: 원시 데이터 조회 (SQL - 1회만)
        # ========================================
        self._load_modon_raw()       # 모돈 기본 정보 (Oracle 함수 호출 없이)
        self._load_modon_wk()        # 모돈 작업 이력 (전체)
        self._load_bunman()
        self._load_eu()
        self._load_sago()
        self._load_jadon_trans()
        self._load_gb()
        self._load_lpd()
        self._load_etc_trade()       # TM_ETC_TRADE (내농장 단가 계산용)
        self._load_farm_config()

        # ========================================
        # 2단계: Python 가공 (Oracle 함수 결과 캐싱)
        # ========================================
        self._calculate_last_wk()           # MAX(SEQ) 기반 마지막 작업
        self._calculate_modon_status()      # Oracle 함수 결과 캐시 저장
        self._calculate_last_gb_dt()        # 마지막 교배일 계산
        self._calculate_schedule_python()   # 예정 정보 계산

        # 가공된 데이터 저장
        self._data['modon_last_wk'] = self._modon_last_wk
        self._data['modon_calc_status'] = self._modon_calc_status
        self._data['modon_last_gb_dt'] = self._modon_last_gb_dt

        self._loaded = True
        self.logger.info(f"데이터 로드 완료: 농장={self.farm_no}")

        return self._data

    def get_data(self) -> Dict[str, Any]:
        """로드된 데이터 반환 (로드 안됐으면 자동 로드)"""
        if not self._loaded:
            self.load()
        return self._data

    def _fetch_all(self, sql: str, params: Optional[Dict] = None) -> List[Dict]:
        """SELECT 쿼리 실행 후 딕셔너리 리스트로 반환"""
        cursor = self.conn.cursor()
        try:
            cursor.execute(sql, params or {})
            columns = [col[0] for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            cursor.close()

    # ========================================================================
    # 원시 데이터 로드 (SQL - 1회만 조회)
    # ========================================================================

    def _load_modon_raw(self) -> None:
        """TB_MODON + TB_MODON_WK 조인하여 모돈 정보 로드

        문서 참조: C:\Projects\inspig\docs\db\ref\01.table.md (섹션 7)
        - TB_MODON: 기본 정보 (PIG_NO, FARM_PIG_NO, IN_DT, OUT_DT 등)
        - TB_MODON_WK: 작업이력 (SANCHA, GYOBAE_CNT, DAERI_YN 등)
        - ROW_NUMBER로 마지막 작업 정보 조인
        - SF_GET_MODONGB_STATUS Oracle 함수로 상태코드 직접 계산
        """
        sql = """
        SELECT M.PIG_NO AS MODON_NO, M.FARM_PIG_NO AS MODON_NM, M.FARM_NO,
               NVL(W.SANCHA, M.IN_SANCHA) AS SANCHA, M.IN_SANCHA,
               M.STATUS_CD, TO_CHAR(M.IN_DT, 'YYYYMMDD') AS IN_DT,
               TO_CHAR(M.OUT_DT, 'YYYYMMDD') AS OUT_DT, M.OUT_GUBUN_CD,
               TO_CHAR(M.BIRTH_DT, 'YYYYMMDD') AS BIRTH_DT,
               NVL(W.GYOBAE_CNT, M.IN_GYOBAE_CNT) AS GB_SANCHA,
               NULL AS LAST_GB_DT, NULL AS LAST_BUN_DT,
               W.LOC_CD AS DONBANG_CD, NULL AS NOW_DONGHO, NULL AS NOW_BANGHO,
               M.IN_GYOBAE_CNT, NVL(W.DAERI_YN, 'N') AS DAERI_YN, M.USE_YN,
               W.WK_GUBUN, W.SAGO_GUBUN_CD,
               -- SF_GET_MODONGB_STATUS Oracle 함수 직접 호출
               -- 도폐사 전 마지막 상태: OUT_DT = '99991231' 전달
               SF_GET_MODONGB_STATUS(
                   'CD',
                   W.WK_GUBUN,
                   W.SAGO_GUBUN_CD,
                   TO_DATE('99991231', 'YYYYMMDD'),
                   M.STATUS_CD,
                   W.DAERI_YN,
                   ''
               ) AS CALC_STATUS_CD
        FROM TB_MODON M
        LEFT JOIN (
            SELECT FARM_NO, PIG_NO, WK_GUBUN, SANCHA, GYOBAE_CNT, LOC_CD, DAERI_YN, SAGO_GUBUN_CD,
                   ROW_NUMBER() OVER (
                       PARTITION BY FARM_NO, PIG_NO
                       ORDER BY WK_DATE DESC, SEQ DESC
                   ) RN
            FROM TB_MODON_WK
            WHERE USE_YN = 'Y'
              AND WK_DATE <= TO_DATE(:base_date, 'YYYYMMDD')
        ) W ON M.FARM_NO = W.FARM_NO AND M.PIG_NO = W.PIG_NO AND W.RN = 1
        WHERE M.FARM_NO = :farm_no
          AND M.USE_YN = 'Y'
          AND M.IN_DT <= TO_DATE(:base_date, 'YYYYMMDD')
          AND (M.OUT_DT = TO_DATE('99991231', 'YYYYMMDD') OR M.OUT_DT > TO_DATE(:base_date, 'YYYYMMDD'))
        """
        self._data['modon'] = self._fetch_all(sql, {
            'farm_no': self.farm_no,
            'base_date': self.base_date,
        })
        self.logger.debug(f"모돈 로드: {len(self._data['modon'])}건")

    def _load_modon_wk(self) -> None:
        """TB_MODON_WK 모돈 작업 이력 로드

        문서 참조: C:\Projects\inspig\docs\db\ref\01.table.md
        컬럼: FARM_NO, PIG_NO, WK_DT, WK_GUBUN, WK_DATE, SANCHA, GYOBAE_CNT,
              LOC_CD, SAGO_GUBUN_CD, DAERI_YN, SEQ, USE_YN
        """
        sql = """
        SELECT W.SEQ, W.PIG_NO AS MODON_NO, W.PIG_NO, W.FARM_NO, W.WK_DT,
               W.WK_GUBUN, W.SANCHA, W.GYOBAE_CNT,
               W.LOC_CD, W.SAGO_GUBUN_CD, W.DAERI_YN, W.USE_YN
        FROM TB_MODON_WK W
        WHERE W.FARM_NO = :farm_no
          AND W.USE_YN = 'Y'
        ORDER BY W.PIG_NO, W.SEQ
        """
        self._data['modon_wk'] = self._fetch_all(sql, {'farm_no': self.farm_no})
        self.logger.debug(f"모돈 작업 이력 로드: {len(self._data['modon_wk'])}건")

    def _load_bunman(self) -> None:
        """TB_BUNMAN 분만 정보 로드

        컬럼: FARM_NO, PIG_NO, WK_DT, WK_GUBUN, SILSAN, MILA, SASAN,
              BUNMAN_GUBUN_CD, SAENGSI_KG, SILSAN_AM, SILSAN_SU, BIGO, USE_YN
        """
        sql = """
        SELECT B.PIG_NO AS MODON_NO, B.FARM_NO, B.WK_DT AS BUN_DT,
               B.SILSAN, B.SASAN, B.MILA AS MUMMY,
               (B.SILSAN + B.SASAN + NVL(B.MILA, 0)) AS TOTAL_CNT,
               B.SAENGSI_KG AS SUM_WT,
               CASE WHEN B.SILSAN > 0 THEN ROUND(B.SAENGSI_KG / B.SILSAN, 2) ELSE 0 END AS AVG_WT,
               B.USE_YN
        FROM TB_BUNMAN B
        WHERE B.FARM_NO = :farm_no
          AND B.USE_YN = 'Y'
        ORDER BY B.PIG_NO, B.WK_DT
        """
        self._data['bunman'] = self._fetch_all(sql, {'farm_no': self.farm_no})
        self.logger.debug(f"분만 로드: {len(self._data['bunman'])}건")

    def _load_eu(self) -> None:
        """TB_EU 이유 정보 로드

        컬럼: FARM_NO, PIG_NO, WK_DT, WK_GUBUN, DUSU, DUSU_SU, ILRYUNG,
              TOTAL_KG, DAERI_YN, BIGO, USE_YN
        """
        sql = """
        SELECT E.PIG_NO AS MODON_NO, E.FARM_NO, E.WK_DT AS EU_DT,
               (NVL(E.DUSU, 0) + NVL(E.DUSU_SU, 0)) AS EU_CNT,
               E.TOTAL_KG AS EU_WT,
               CASE WHEN (NVL(E.DUSU, 0) + NVL(E.DUSU_SU, 0)) > 0
                    THEN ROUND(E.TOTAL_KG / (NVL(E.DUSU, 0) + NVL(E.DUSU_SU, 0)), 2)
                    ELSE 0 END AS EU_AVG_WT,
               E.ILRYUNG AS POYU_DAYS,
               E.DAERI_YN, E.USE_YN
        FROM TB_EU E
        WHERE E.FARM_NO = :farm_no
          AND E.USE_YN = 'Y'
        ORDER BY E.PIG_NO, E.WK_DT
        """
        self._data['eu'] = self._fetch_all(sql, {'farm_no': self.farm_no})
        self.logger.debug(f"이유 로드: {len(self._data['eu'])}건")

    def _load_sago(self) -> None:
        """TB_SAGO 사고 정보 로드

        컬럼: FARM_NO, PIG_NO, WK_DT, WK_GUBUN, SAGO_GUBUN_CD, BIGO, USE_YN
        """
        sql = """
        SELECT S.PIG_NO AS MODON_NO, S.FARM_NO, S.WK_DT AS SAGO_DT,
               S.SAGO_GUBUN_CD, S.BIGO AS MEMO, S.USE_YN
        FROM TB_SAGO S
        WHERE S.FARM_NO = :farm_no
          AND S.USE_YN = 'Y'
        ORDER BY S.PIG_NO, S.WK_DT
        """
        self._data['sago'] = self._fetch_all(sql, {'farm_no': self.farm_no})
        self.logger.debug(f"사고 로드: {len(self._data['sago'])}건")

    def _load_jadon_trans(self) -> None:
        """TB_MODON_JADON_TRANS 자돈 이동 정보 로드

        컬럼: FARM_NO, PIG_NO, SEQ, SANCHA, GUBUN_CD, SUB_GUBUN_CD, WK_DT,
              DUSU, DUSU_SU, ILRYUNG, TOTAL_KG, BUN_DT, EU_DT, IO_PIG_NO,
              LOC_CD, FW_NO, BIGO, USE_YN
        """
        sql = """
        SELECT T.SEQ, T.PIG_NO AS MODON_NO, T.FARM_NO, TO_CHAR(T.WK_DT, 'YYYYMMDD') AS TRANS_DT,
               T.SANCHA, TO_CHAR(T.BUN_DT, 'YYYYMMDD') AS BUN_DT, T.GUBUN_CD AS TRANS_GUBUN_CD,
               (NVL(T.DUSU, 0) + NVL(T.DUSU_SU, 0)) AS TRANS_CNT,
               T.USE_YN
        FROM TB_MODON_JADON_TRANS T
        WHERE T.FARM_NO = :farm_no
          AND T.USE_YN = 'Y'
        ORDER BY T.PIG_NO, T.WK_DT, T.SEQ
        """
        self._data['jadon_trans'] = self._fetch_all(sql, {'farm_no': self.farm_no})
        self.logger.debug(f"자돈 이동 로드: {len(self._data['jadon_trans'])}건")

    def _load_gb(self) -> None:
        """TB_GYOBAE 교배 정보 로드

        컬럼: FARM_NO, PIG_NO, WK_DT, WK_GUBUN, METHOD_1~3, UNGDON_PIG_NO_1~3,
              UFARM_PIG_NO_1~3, WK_PERSON_CD, BIGO, USE_YN
        """
        sql = """
        SELECT G.PIG_NO AS MODON_NO, G.FARM_NO, G.WK_DT AS GB_DT,
               G.METHOD_1, G.UNGDON_PIG_NO_1, G.UNGDON_PIG_NO_2, G.UNGDON_PIG_NO_3,
               G.USE_YN
        FROM TB_GYOBAE G
        WHERE G.FARM_NO = :farm_no
          AND G.USE_YN = 'Y'
        ORDER BY G.PIG_NO, G.WK_DT
        """
        self._data['gb'] = self._fetch_all(sql, {'farm_no': self.farm_no})
        self.logger.debug(f"교배 로드: {len(self._data['gb'])}건")

        # TB_GYOBAE는 상세 테이블이 없으므로 빈 리스트
        self._data['gb_detail'] = []
        self.logger.debug(f"교배 상세: TB_GYOBAE에 통합됨")

    def _load_lpd(self) -> None:
        """TM_LPD_DATA 출하 데이터 로드

        컬럼: FARM_NO, DOCHUK_DT, DOCHUK_NO, FACTORY_CD, SEX_GUBUN, NET_KG,
              BACK_DEPTH, MEAT_QUALITY, AU_PRICE, USE_YN
        """
        sql = """
        SELECT L.SEQ, L.FARM_NO, L.DOCHUK_DT, L.DOCHUK_NO,
               L.NET_KG, L.BACK_DEPTH, L.MEAT_QUALITY,
               L.AU_PRICE, L.SEX_GUBUN, L.USE_YN
        FROM TM_LPD_DATA L
        WHERE L.FARM_NO = :farm_no
          AND L.USE_YN = 'Y'
        ORDER BY L.DOCHUK_DT
        """
        self._data['lpd'] = self._fetch_all(sql, {'farm_no': self.farm_no})
        self.logger.debug(f"LPD 로드: {len(self._data['lpd'])}건")

    def _load_etc_trade(self) -> None:
        """TM_ETC_TRADE 매출 데이터 로드

        Oracle 프로시저 SP_INS_WEEK_SHIP_POPUP에서 내농장 단가 계산에 사용:
        - 계정코드 511% (비육돈매출)
        - TOTAL_KG > 0 조건
        - 내농장단가 = SUM(TOTAL_PRICE) / SUM(TOTAL_KG)

        컬럼: FARM_NO, WK_DT, ACCOUNT_CD, TOTAL_PRICE, TOTAL_KG, USE_YN
        """
        sql = """
        SELECT T.SEQ, T.FARM_NO, TO_CHAR(T.WK_DT, 'YYYYMMDD') AS WK_DT,
               T.ACCOUNT_CD, T.TOTAL_PRICE, T.TOTAL_KG, T.USE_YN
        FROM TM_ETC_TRADE T
        WHERE T.FARM_NO = :farm_no
          AND T.USE_YN = 'Y'
          AND SUBSTR(T.ACCOUNT_CD, 1, 3) = '511'
          AND T.TOTAL_KG > 0
        ORDER BY T.WK_DT
        """
        self._data['etc_trade'] = self._fetch_all(sql, {'farm_no': self.farm_no})
        self.logger.debug(f"ETC_TRADE 로드: {len(self._data['etc_trade'])}건")

    def _load_farm_config(self) -> None:
        """농장 정보 로드"""
        sql = """
        SELECT F.FARM_NO, F.FARM_NM, F.PRINCIPAL_NM, F.SIGUN_CD,
               NVL(F.COUNTRY_CODE, 'KOR') AS LOCALE,
               F.USE_YN
        FROM TA_FARM F
        WHERE F.FARM_NO = :farm_no
        """
        farms = self._fetch_all(sql, {'farm_no': self.farm_no})
        self._data['farm_config'] = farms[0] if farms else {}

        # 농장 기본 설정값 로드 (TC_FARM_CONFIG)
        sql = """
        SELECT C.CODE, C.CVALUE
        FROM TC_FARM_CONFIG C
        WHERE C.FARM_NO = :farm_no
          AND C.USE_YN = 'Y'
        """
        settings = self._fetch_all(sql, {'farm_no': self.farm_no})
        self._data['farm_settings'] = {s['CODE']: s['CVALUE'] for s in settings}

        self.logger.debug(f"농장 설정 로드: {len(self._data['farm_settings'])}건")

    # ========================================================================
    # Python 가공 함수 (Oracle View/Function 로직 대체)
    # ========================================================================

    def _calculate_last_wk(self) -> None:
        """MAX(SEQ) 기반 모돈별 마지막 작업 계산

        VW_MODON_2020_MAX_WK_02 뷰 로직 Python 구현:
        - 기준일 이전 작업 중 MAX(SEQ) 기준 마지막 작업
        - WK_DT <= base_date 조건
        """
        modon_wk = self._data.get('modon_wk', [])

        for wk in modon_wk:
            modon_no = str(wk.get('MODON_NO', ''))
            wk_dt = str(wk.get('WK_DT', ''))
            seq = wk.get('SEQ', 0)

            # 기준일 이전 작업만
            if wk_dt > self.base_date:
                continue

            # MAX(SEQ) 갱신
            if modon_no not in self._modon_last_wk:
                self._modon_last_wk[modon_no] = wk
            elif seq > self._modon_last_wk[modon_no].get('SEQ', 0):
                self._modon_last_wk[modon_no] = wk

        self.logger.debug(f"마지막 작업 계산: {len(self._modon_last_wk)}건")

    def _calculate_modon_status(self) -> None:
        """Oracle SF_GET_MODONGB_STATUS 함수 결과를 캐시에 저장

        v3 아키텍처:
        - SQL에서 SF_GET_MODONGB_STATUS Oracle 함수를 직접 호출하여 CALC_STATUS_CD 계산
        - Python에서는 해당 결과를 캐시(_modon_calc_status)에 저장만 수행
        - Oracle 함수가 NULL 반환 시 STATUS_HUBO('010001') 기본값 사용
        """
        modon_list = self._data.get('modon', [])

        for modon in modon_list:
            modon_no = str(modon.get('MODON_NO', ''))
            # Oracle 함수 결과 사용 (NULL이면 후보돈)
            status = modon.get('CALC_STATUS_CD') or STATUS_HUBO
            self._modon_calc_status[modon_no] = status

        self.logger.debug(f"상태코드 캐시 저장: {len(self._modon_calc_status)}건")

    def _calculate_last_gb_dt(self) -> None:
        """모돈별 마지막 교배일 계산

        SP_INS_WEEK_SG_POPUP의 WITH LAST_GB 로직 구현:
        - 각 작업일 기준 그 이전의 마지막 교배일(WK_GUBUN='G')
        - 경과일 계산에 사용
        """
        modon_wk = self._data.get('modon_wk', [])

        # 모돈별 교배 작업 수집
        modon_gb_list: Dict[str, List[Tuple[str, str]]] = {}  # {modon_no: [(wk_dt, seq), ...]}

        for wk in modon_wk:
            if wk.get('WK_GUBUN') == WK_GYOBAE:
                modon_no = str(wk.get('MODON_NO', ''))
                wk_dt = str(wk.get('WK_DT', ''))
                if modon_no and wk_dt and wk_dt <= self.base_date:
                    if modon_no not in modon_gb_list:
                        modon_gb_list[modon_no] = []
                    modon_gb_list[modon_no].append((wk_dt, wk.get('SEQ', 0)))

        # 모돈별 마지막 교배일 (기준일 이전)
        for modon_no, gb_list in modon_gb_list.items():
            if gb_list:
                # 날짜순 정렬 후 마지막
                gb_list.sort(key=lambda x: (x[0], x[1]))
                self._modon_last_gb_dt[modon_no] = gb_list[-1][0]

        # modon 데이터에 마지막 교배일 추가
        for modon in self._data.get('modon', []):
            modon_no = str(modon.get('MODON_NO', ''))
            modon['CALC_LAST_GB_DT'] = self._modon_last_gb_dt.get(modon_no, '')

        self.logger.debug(f"마지막 교배일 계산: {len(self._modon_last_gb_dt)}건")

    def _calculate_schedule_python(self) -> None:
        """예정 정보 Python 계산 (SQL 제거)

        모든 예정 계산을 Python에서 수행:
        - 교배 예정: 이유 후 4-7일 경과한 이유모돈
        - 임신검사 예정: 교배 후 21-28일 경과한 임신돈
        - 분만 예정: 교배 후 110-118일 경과한 임신돈
        - 이유 예정: 분만 후 21-28일 경과한 포유돈
        """
        # 금주 기간 계산 (리포트 기간 다음 주 월~일)
        dt_to_obj = datetime.strptime(self.dt_to, '%Y%m%d')
        this_monday = dt_to_obj + timedelta(days=1)
        this_sunday = this_monday + timedelta(days=6)

        this_sdt = this_monday.strftime('%Y%m%d')
        this_edt = this_sunday.strftime('%Y%m%d')

        modon_list = self._data.get('modon', [])
        eu_list = self._data.get('eu', [])

        # 모돈별 최근 이유일 조회
        modon_eu_dict: Dict[str, Tuple[str, Dict]] = {}  # {modon_no: (eu_dt, eu_record)}
        for eu in eu_list:
            modon_no = str(eu.get('MODON_NO', ''))
            eu_dt = str(eu.get('EU_DT', ''))
            if modon_no and eu_dt:
                if modon_no not in modon_eu_dict or eu_dt > modon_eu_dict[modon_no][0]:
                    modon_eu_dict[modon_no] = (eu_dt, eu)

        mating_schedule = []
        farrowing_schedule = []
        weaning_schedule = []
        check_schedule = []

        for modon in modon_list:
            modon_no = str(modon.get('MODON_NO', ''))
            calc_status = modon.get('CALC_STATUS_CD', '')
            last_gb_dt = modon.get('CALC_LAST_GB_DT', '') or str(modon.get('LAST_GB_DT', '') or '')
            last_bun_dt = str(modon.get('LAST_BUN_DT', '') or '')

            # 교배 예정: 이유모돈(010005), 이유 후 4-7일
            if calc_status == STATUS_EUMO:
                eu_info = modon_eu_dict.get(modon_no)
                if eu_info:
                    eu_dt = eu_info[0]
                    expect_dt = self._add_days_to_date(eu_dt, 5)  # 중간값 5일
                    if this_sdt <= expect_dt <= this_edt:
                        mating_schedule.append({
                            **modon,
                            'EU_DT': eu_dt,
                            'EXPECT_DT': expect_dt,
                            'SCHEDULE_CD': '150005',
                        })

            # 임신돈(010002)
            elif calc_status == STATUS_IMSIN and last_gb_dt:
                # 임신검사 예정: 교배 후 21-28일
                expect_check_dt = self._add_days_to_date(last_gb_dt, 25)
                if this_sdt <= expect_check_dt <= this_edt:
                    check_schedule.append({
                        **modon,
                        'EXPECT_DT': expect_check_dt,
                        'SCHEDULE_CD': '150004',
                    })

                # 분만 예정: 교배 후 110-118일
                expect_bun_dt = self._add_days_to_date(last_gb_dt, 114)
                if this_sdt <= expect_bun_dt <= this_edt:
                    farrowing_schedule.append({
                        **modon,
                        'EXPECT_DT': expect_bun_dt,
                        'SCHEDULE_CD': '150001',
                    })

            # 이유 예정: 포유돈(010003), 분만 후 21-28일
            elif calc_status == STATUS_POYU and last_bun_dt:
                expect_eu_dt = self._add_days_to_date(last_bun_dt, 25)
                if this_sdt <= expect_eu_dt <= this_edt:
                    weaning_schedule.append({
                        **modon,
                        'EXPECT_DT': expect_eu_dt,
                        'SCHEDULE_CD': '150002',
                    })

        self._data['schedule'] = {
            'this_sdt': this_monday.strftime('%Y-%m-%d'),
            'this_edt': this_sunday.strftime('%Y-%m-%d'),
            'mating': mating_schedule,
            'farrowing': farrowing_schedule,
            'weaning': weaning_schedule,
            'check': check_schedule,
        }

        total = len(mating_schedule) + len(farrowing_schedule) + len(weaning_schedule) + len(check_schedule)
        self.logger.debug(f"예정 정보 계산: {total}건")

    def _add_days_to_date(self, dt_str: str, days: int) -> str:
        """날짜에 일수 추가"""
        if not dt_str or len(dt_str) < 8:
            return ''
        try:
            dt = datetime.strptime(dt_str[:8], '%Y%m%d')
            result = dt + timedelta(days=days)
            return result.strftime('%Y%m%d')
        except ValueError:
            return ''

    # ========================================================================
    # 데이터 조회 헬퍼 함수
    # ========================================================================

    def get_modon_dict(self) -> Dict[str, Dict]:
        """모돈 정보를 MODON_NO 키로 딕셔너리 변환"""
        if 'modon' not in self._data:
            self.load()
        return {str(m['MODON_NO']): m for m in self._data['modon']}

    def get_modon_by_status(self, status_cd: str) -> List[Dict]:
        """계산된 상태코드로 모돈 필터링

        Args:
            status_cd: 상태코드 (예: '010002')

        Returns:
            해당 상태의 모돈 리스트
        """
        if not self._loaded:
            self.load()
        return [m for m in self._data['modon'] if m.get('CALC_STATUS_CD') == status_cd]

    def get_last_wk(self, modon_no: str) -> Optional[Dict]:
        """모돈의 마지막 작업 정보 조회

        Args:
            modon_no: 모돈 번호

        Returns:
            마지막 작업 딕셔너리 또는 None
        """
        if not self._loaded:
            self.load()
        return self._modon_last_wk.get(str(modon_no))

    def get_last_gb_dt(self, modon_no: str) -> str:
        """모돈의 마지막 교배일 조회

        Args:
            modon_no: 모돈 번호

        Returns:
            마지막 교배일 (YYYYMMDD) 또는 빈 문자열
        """
        if not self._loaded:
            self.load()
        return self._modon_last_gb_dt.get(str(modon_no), '')

    def calculate_days_elapsed(self, modon_no: str, from_field: str = 'LAST_GB') -> int:
        """경과일 계산

        Args:
            modon_no: 모돈 번호
            from_field: 기준 필드 ('LAST_GB', 'LAST_BUN', 'EU')

        Returns:
            경과일 (기준일 - 해당 날짜)
        """
        if not self._loaded:
            self.load()

        modon_no = str(modon_no)
        base_dt = datetime.strptime(self.base_date, '%Y%m%d')

        if from_field == 'LAST_GB':
            from_dt_str = self._modon_last_gb_dt.get(modon_no, '')
        else:
            modon = self.get_modon_dict().get(modon_no, {})
            if from_field == 'LAST_BUN':
                from_dt_str = str(modon.get('LAST_BUN_DT', '') or '')
            else:  # EU
                # 최근 이유일 조회
                eu_list = [e for e in self._data.get('eu', [])
                          if str(e.get('MODON_NO', '')) == modon_no]
                if eu_list:
                    eu_list.sort(key=lambda x: str(x.get('EU_DT', '')), reverse=True)
                    from_dt_str = str(eu_list[0].get('EU_DT', ''))
                else:
                    from_dt_str = ''

        if not from_dt_str or len(from_dt_str) < 8:
            return 0

        try:
            from_dt = datetime.strptime(from_dt_str[:8], '%Y%m%d')
            return (base_dt - from_dt).days
        except ValueError:
            return 0

    def filter_by_period(self, data: List[Dict], date_field: str,
                         dt_from: str = None, dt_to: str = None) -> List[Dict]:
        """기간으로 데이터 필터링 (Python에서 수행)

        Args:
            data: 필터링할 데이터 리스트
            date_field: 날짜 필드명 (예: 'WK_DT', 'BUN_DT')
            dt_from: 시작일 (YYYYMMDD), None이면 로드 시작일
            dt_to: 종료일 (YYYYMMDD), None이면 로드 종료일

        Returns:
            필터링된 데이터 리스트
        """
        dt_from = dt_from or self.dt_from
        dt_to = dt_to or self.dt_to

        return [
            row for row in data
            if row.get(date_field) and dt_from <= str(row[date_field])[:8] <= dt_to
        ]

    def filter_by_wk_gubun(self, wk_gubun: str, dt_from: str = None,
                            dt_to: str = None) -> List[Dict]:
        """작업구분으로 modon_wk 필터링

        Args:
            wk_gubun: 작업 구분 (예: 'G', 'B', 'E', 'F')
            dt_from: 시작일
            dt_to: 종료일

        Returns:
            필터링된 작업 리스트
        """
        if 'modon_wk' not in self._data:
            self.load()

        data = self.filter_by_period(self._data['modon_wk'], 'WK_DT', dt_from, dt_to)
        return [row for row in data if row.get('WK_GUBUN') == wk_gubun]

    def filter_by_wk_cd(self, wk_cd: str, dt_from: str = None, dt_to: str = None) -> List[Dict]:
        """작업코드로 modon_wk 필터링

        Args:
            wk_cd: 작업 코드 (예: '050001')
            dt_from: 시작일
            dt_to: 종료일

        Returns:
            필터링된 작업 리스트
        """
        if 'modon_wk' not in self._data:
            self.load()

        data = self.filter_by_period(self._data['modon_wk'], 'WK_DT', dt_from, dt_to)
        return [row for row in data if row.get('WK_CD') == wk_cd]

    def group_by(self, data: List[Dict], key_field: str) -> Dict[str, List[Dict]]:
        """데이터를 특정 필드로 그룹핑

        Args:
            data: 그룹핑할 데이터 리스트
            key_field: 그룹핑 키 필드명

        Returns:
            그룹핑된 딕셔너리
        """
        result: Dict[str, List[Dict]] = {}
        for row in data:
            key = str(row.get(key_field, ''))
            if key not in result:
                result[key] = []
            result[key].append(row)
        return result

    def get_farm_price(self, dt_from: str = None, dt_to: str = None) -> int:
        """내농장 단가 계산 (TM_ETC_TRADE)

        Oracle 프로시저 SP_INS_WEEK_SHIP_POPUP 로직:
        - 계정코드 511% (비육돈매출), TOTAL_KG > 0
        - 내농장단가 = ROUND(SUM(TOTAL_PRICE) / SUM(TOTAL_KG))

        Args:
            dt_from: 시작일 (YYYYMMDD), None이면 로드 시작일
            dt_to: 종료일 (YYYYMMDD), None이면 로드 종료일

        Returns:
            내농장 단가 (원/kg), 데이터 없으면 0
        """
        if 'etc_trade' not in self._data:
            self.load()

        dt_from = dt_from or self.dt_from
        dt_to = dt_to or self.dt_to

        # 기간 필터링
        filtered = [
            row for row in self._data.get('etc_trade', [])
            if row.get('WK_DT') and dt_from <= str(row['WK_DT'])[:8] <= dt_to
        ]

        if not filtered:
            return 0

        # SUM(TOTAL_PRICE) / SUM(TOTAL_KG)
        total_price = sum(row.get('TOTAL_PRICE', 0) or 0 for row in filtered)
        total_kg = sum(row.get('TOTAL_KG', 0) or 0 for row in filtered)

        if total_kg <= 0:
            return 0

        return round(total_price / total_kg)

    def aggregate(self, data: List[Dict], value_field: str,
                  agg_type: str = 'sum') -> float:
        """데이터 집계

        Args:
            data: 집계할 데이터 리스트
            value_field: 집계할 필드명
            agg_type: 집계 유형 ('sum', 'avg', 'count', 'min', 'max')

        Returns:
            집계 결과
        """
        values = [row.get(value_field, 0) or 0 for row in data]

        if not values:
            return 0

        if agg_type == 'sum':
            return sum(values)
        elif agg_type == 'avg':
            return sum(values) / len(values) if values else 0
        elif agg_type == 'count':
            return len(values)
        elif agg_type == 'min':
            return min(values)
        elif agg_type == 'max':
            return max(values)
        else:
            raise ValueError(f"Unknown agg_type: {agg_type}")

    def get_wk_by_modon(self, modon_no: str, wk_gubun: str = None,
                         dt_from: str = None, dt_to: str = None) -> List[Dict]:
        """모돈별 작업이력 조회

        Args:
            modon_no: 모돈 번호
            wk_gubun: 작업 구분 (None이면 전체)
            dt_from: 시작일
            dt_to: 종료일

        Returns:
            작업 이력 리스트
        """
        if 'modon_wk' not in self._data:
            self.load()

        data = [wk for wk in self._data['modon_wk']
                if str(wk.get('MODON_NO', '')) == str(modon_no)]

        if wk_gubun:
            data = [wk for wk in data if wk.get('WK_GUBUN') == wk_gubun]

        if dt_from or dt_to:
            data = self.filter_by_period(data, 'WK_DT', dt_from, dt_to)

        return data
