#!/bin/bash
# InsightPig 주간 리포트 ETL 실행 스크립트
# Crontab에서 호출: 0 2 * * 1 /data/etl/inspig-weekly/run_weekly.sh

# 스크립트 경로
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$SCRIPT_DIR/logs"

# 로그 디렉토리 생성
mkdir -p "$LOG_DIR"

# Conda 환경 활성화
source /data/anaconda/anaconda3/etc/profile.d/conda.sh
conda activate inspig-weekly-etl

# 작업 디렉토리 이동
cd "$SCRIPT_DIR"

# 시작 로그
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Weekly ETL 시작" >> "$LOG_DIR/cron.log"

# ETL 실행
python weekly_report_etl.py

# 종료 코드 확인
EXIT_CODE=$?
if [ $EXIT_CODE -eq 0 ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Weekly ETL 완료 (성공)" >> "$LOG_DIR/cron.log"
else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Weekly ETL 실패 (exit: $EXIT_CODE)" >> "$LOG_DIR/cron.log"
fi

exit $EXIT_CODE
