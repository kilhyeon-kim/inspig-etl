"""
InsightPig ETL REST API Server

pig3.1에서 호출하여 특정 농장의 주간 리포트를 생성합니다.

실행:
    python -m src.api.server
    또는
    python run_api.py

API:
    POST /api/etl/run-farm
    {
        "farmNo": 2807,
        "dtFrom": "20251223",  // optional
        "dtTo": "20251229"     // optional
    }

    Response:
    {
        "status": "success",
        "farmNo": 2807,
        "shareToken": "abc123...",
        "year": 2025,
        "weekNo": 52,
        "dtFrom": "20251223",
        "dtTo": "20251229"
    }
"""

import logging
from datetime import datetime, timedelta
from typing import Optional
from pydantic import BaseModel, Field

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.common import Config, setup_logger, now_kst
from src.weekly import WeeklyReportOrchestrator

# 로거 설정
logger = logging.getLogger(__name__)

# FastAPI 앱 생성
app = FastAPI(
    title="InsightPig ETL API",
    description="주간 리포트 ETL 실행 API",
    version="1.0.0",
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
    dtFrom: Optional[str] = Field(None, description="리포트 시작일 (YYYYMMDD)", pattern=r"^\d{8}$")
    dtTo: Optional[str] = Field(None, description="리포트 종료일 (YYYYMMDD)", pattern=r"^\d{8}$")


class RunFarmResponse(BaseModel):
    """농장별 ETL 실행 응답"""
    status: str
    farmNo: int
    shareToken: Optional[str] = None
    year: Optional[int] = None
    weekNo: Optional[int] = None
    dtFrom: Optional[str] = None
    dtTo: Optional[str] = None
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
    특정 농장의 주간 리포트 ETL 실행

    - farmNo: 농장번호 (필수)
    - dtFrom, dtTo: 리포트 기간 (선택, 미입력시 지난주 자동 계산)

    Returns:
        - status: success/error
        - shareToken: 생성된 공유 토큰
        - year, weekNo: 주차 정보
    """
    try:
        logger.info(f"ETL 요청 수신: farmNo={request.farmNo}, dtFrom={request.dtFrom}, dtTo={request.dtTo}")

        orchestrator = get_orchestrator()

        # ETL 실행
        result = orchestrator.run_single_farm(
            farm_no=request.farmNo,
            dt_from=request.dtFrom,
            dt_to=request.dtTo,
        )

        if result.get('status') == 'success':
            return RunFarmResponse(
                status="success",
                farmNo=request.farmNo,
                shareToken=result.get('share_token'),
                year=result.get('year'),
                weekNo=result.get('week_no'),
                dtFrom=result.get('dt_from'),
                dtTo=result.get('dt_to'),
                message="ETL 완료"
            )
        else:
            return RunFarmResponse(
                status="error",
                farmNo=request.farmNo,
                error=result.get('error', 'Unknown error'),
                message=result.get('message')
            )

    except Exception as e:
        logger.error(f"ETL 실행 오류: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/etl/status/{farm_no}")
async def get_etl_status(farm_no: int):
    """
    농장의 최신 주간 리포트 상태 조회

    Returns:
        - exists: 리포트 존재 여부
        - shareToken: 공유 토큰 (있는 경우)
        - year, weekNo: 주차 정보
    """
    try:
        orchestrator = get_orchestrator()

        # DB에서 최신 리포트 조회
        with orchestrator.db.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    SELECT W.SHARE_TOKEN, W.REPORT_YEAR, W.REPORT_WEEK_NO,
                           W.DT_FROM, W.DT_TO, W.STATUS_CD
                    FROM TS_INS_WEEK W
                    INNER JOIN TS_INS_MASTER M ON W.MASTER_SEQ = M.SEQ
                    WHERE W.FARM_NO = :farm_no
                      AND M.DAY_GB = 'WEEK'
                      AND M.STATUS_CD = 'COMPLETE'
                      AND W.STATUS_CD = 'COMPLETE'
                    ORDER BY W.REPORT_YEAR DESC, W.REPORT_WEEK_NO DESC
                    FETCH FIRST 1 ROWS ONLY
                """, {'farm_no': farm_no})
                row = cursor.fetchone()

                if row:
                    return {
                        "exists": True,
                        "farmNo": farm_no,
                        "shareToken": row[0],
                        "year": row[1],
                        "weekNo": row[2],
                        "dtFrom": row[3],
                        "dtTo": row[4],
                        "statusCd": row[5]
                    }
                else:
                    return {
                        "exists": False,
                        "farmNo": farm_no,
                        "message": "주간 리포트가 없습니다."
                    }
            finally:
                cursor.close()

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
