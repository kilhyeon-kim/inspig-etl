"""
InsightPig ETL REST API Server

pig3.1에서 호출하여 특정 농장의 주간/월간/분기 리포트를 생성합니다.

실행:
    python -m src.api.server
    또는
    python run_api.py

API:
    POST /api/etl/run-farm
    {
        "farmNo": 2807,
        "dayGb": "WEEK",       // WEEK, MONTH, QUARTER (default: WEEK)
        "insDate": "20251229"  // optional, 기준일(INS_DT), 미입력시 오늘
    }

    insDate 기준으로 지난주/지난달/지난분기 리포트를 생성합니다.
    - insDate가 2025-12-29(월)이면 → 지난주: 12/22~12/28 (52주)
    - insDate가 2025-12-28(일)이면 → 지난주: 12/15~12/21 (51주)

    Response:
    {
        "status": "success",
        "farmNo": 2807,
        "dayGb": "WEEK",
        "shareToken": "abc123...",
        "year": 2025,
        "weekNo": 52,          // WEEK인 경우
        "monthNo": null,       // MONTH인 경우
        "quarterNo": null,     // QUARTER인 경우
        "insDate": "20251229",
        "dtFrom": "20251222",
        "dtTo": "20251228"
    }
"""

import logging
from datetime import datetime, timedelta
from typing import Optional, Literal
from enum import Enum
from pydantic import BaseModel, Field

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.common import Config, setup_logger, now_kst
from src.weekly import WeeklyReportOrchestrator


class DayGbEnum(str, Enum):
    """리포트 구분"""
    WEEK = "WEEK"
    MONTH = "MONTH"
    QUARTER = "QUARTER"

# 로거 설정
logger = logging.getLogger(__name__)

# FastAPI 앱 생성
app = FastAPI(
    title="InsightPig ETL API",
    description="주간/월간/분기 리포트 ETL 실행 API",
    version="1.1.0",
)

# CORS 설정 (pig3.1 서버에서 호출 허용)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 운영 환경에서는 특정 도메인으로 제한
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Request/Response 모델
class RunFarmRequest(BaseModel):
    """농장별 ETL 실행 요청"""
    farmNo: int = Field(..., description="농장번호", ge=1)
    dayGb: DayGbEnum = Field(DayGbEnum.WEEK, description="리포트 구분 (WEEK, MONTH, QUARTER)")
    insDate: Optional[str] = Field(None, description="기준일 INS_DT (YYYYMMDD), 미입력시 오늘", pattern=r"^\d{8}$")


class RunFarmResponse(BaseModel):
    """농장별 ETL 실행 응답"""
    status: str
    farmNo: int
    dayGb: str = "WEEK"
    shareToken: Optional[str] = None
    year: Optional[int] = None
    weekNo: Optional[int] = None      # WEEK인 경우
    monthNo: Optional[int] = None     # MONTH인 경우
    quarterNo: Optional[int] = None   # QUARTER인 경우
    insDate: Optional[str] = None     # 기준일 (INS_DT)
    dtFrom: Optional[str] = None      # 리포트 시작일
    dtTo: Optional[str] = None        # 리포트 종료일
    message: Optional[str] = None
    error: Optional[str] = None


class HealthResponse(BaseModel):
    """헬스체크 응답"""
    status: str
    timestamp: str
    version: str


# 전역 설정 (서버 시작 시 로드)
_config = None
_orchestrator = None


def get_orchestrator():
    """WeeklyReportOrchestrator 싱글톤"""
    global _config, _orchestrator
    if _orchestrator is None:
        _config = Config()
        _orchestrator = WeeklyReportOrchestrator(_config)
    return _orchestrator


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """헬스체크 API"""
    return HealthResponse(
        status="ok",
        timestamp=now_kst().isoformat(),
        version="1.0.0"
    )


@app.post("/api/etl/run-farm", response_model=RunFarmResponse)
async def run_farm_etl(request: RunFarmRequest):
    """
    특정 농장의 주간/월간/분기 리포트 ETL 실행

    - farmNo: 농장번호 (필수)
    - dayGb: 리포트 구분 (WEEK, MONTH, QUARTER, 기본: WEEK)
    - insDate: 기준일 INS_DT (선택, 미입력시 오늘)

    insDate 기준으로 지난주/지난달/지난분기 리포트를 생성합니다.

    Returns:
        - status: success/error
        - shareToken: 생성된 공유 토큰
        - year, weekNo/monthNo/quarterNo: 기간 정보
        - insDate, dtFrom, dtTo: 날짜 정보
    """
    try:
        day_gb = request.dayGb.value
        ins_date = request.insDate or now_kst().strftime('%Y%m%d')

        logger.info(f"ETL 요청 수신: farmNo={request.farmNo}, dayGb={day_gb}, insDate={ins_date}")

        # 현재는 WEEK만 구현, MONTH/QUARTER는 추후 구현
        if day_gb != "WEEK":
            return RunFarmResponse(
                status="error",
                farmNo=request.farmNo,
                dayGb=day_gb,
                error=f"{day_gb} 리포트는 아직 구현되지 않았습니다.",
                message="현재 WEEK 리포트만 지원됩니다."
            )

        orchestrator = get_orchestrator()

        # ETL 실행 (insDate 기준으로 지난주 계산은 run_single_farm 내부에서 처리)
        result = orchestrator.run_single_farm(
            farm_no=request.farmNo,
            ins_date=ins_date,
        )

        if result.get('status') == 'success':
            return RunFarmResponse(
                status="success",
                farmNo=request.farmNo,
                dayGb=day_gb,
                shareToken=result.get('share_token'),
                year=result.get('year'),
                weekNo=result.get('week_no'),
                insDate=result.get('ins_date'),
                dtFrom=result.get('dt_from'),
                dtTo=result.get('dt_to'),
                message="ETL 완료"
            )
        else:
            return RunFarmResponse(
                status="error",
                farmNo=request.farmNo,
                dayGb=day_gb,
                error=result.get('error', 'Unknown error'),
                message=result.get('message')
            )

    except Exception as e:
        logger.error(f"ETL 실행 오류: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/etl/status/{farm_no}")
async def get_etl_status(farm_no: int, day_gb: str = "WEEK"):
    """
    농장의 최신 리포트 상태 조회

    - farm_no: 농장번호
    - day_gb: 리포트 구분 (WEEK, MONTH, QUARTER, 기본: WEEK)

    Returns:
        - exists: 리포트 존재 여부
        - shareToken: 공유 토큰 (있는 경우)
        - year, weekNo/monthNo/quarterNo: 기간 정보
    """
    try:
        day_gb = day_gb.upper()
        if day_gb not in ["WEEK", "MONTH", "QUARTER"]:
            raise HTTPException(status_code=400, detail=f"잘못된 day_gb: {day_gb}")

        orchestrator = get_orchestrator()

        # 테이블/컬럼 매핑
        table_map = {
            "WEEK": ("TS_INS_WEEK", "REPORT_WEEK_NO", "weekNo"),
            "MONTH": ("TS_INS_MONTH", "REPORT_MONTH_NO", "monthNo"),
            "QUARTER": ("TS_INS_QUARTER", "REPORT_QUARTER_NO", "quarterNo"),
        }
        table_name, period_col, period_key = table_map[day_gb]

        # DB에서 최신 리포트 조회
        with orchestrator.db.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(f"""
                    SELECT W.SHARE_TOKEN, W.REPORT_YEAR, W.{period_col},
                           W.DT_FROM, W.DT_TO, W.STATUS_CD
                    FROM {table_name} W
                    INNER JOIN TS_INS_MASTER M ON W.MASTER_SEQ = M.SEQ
                    WHERE W.FARM_NO = :farm_no
                      AND M.DAY_GB = :day_gb
                      AND M.STATUS_CD = 'COMPLETE'
                      AND W.STATUS_CD = 'COMPLETE'
                    ORDER BY W.REPORT_YEAR DESC, W.{period_col} DESC
                    FETCH FIRST 1 ROWS ONLY
                """, {'farm_no': farm_no, 'day_gb': day_gb})
                row = cursor.fetchone()

                if row:
                    result = {
                        "exists": True,
                        "farmNo": farm_no,
                        "dayGb": day_gb,
                        "shareToken": row[0],
                        "year": row[1],
                        "dtFrom": row[3],
                        "dtTo": row[4],
                        "statusCd": row[5]
                    }
                    result[period_key] = row[2]
                    return result
                else:
                    return {
                        "exists": False,
                        "farmNo": farm_no,
                        "dayGb": day_gb,
                        "message": f"{day_gb} 리포트가 없습니다."
                    }
            finally:
                cursor.close()

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"상태 조회 오류: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


def run_server(host: str = "0.0.0.0", port: int = 8000):
    """API 서버 실행"""
    import uvicorn

    # 로거 초기화
    config = Config()
    setup_logger("etl_api", config.logging.get('log_path'))

    logger.info(f"ETL API 서버 시작: http://{host}:{port}")
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    run_server()
