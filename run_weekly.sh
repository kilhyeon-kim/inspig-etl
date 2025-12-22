#!/bin/bash
# InsightPig Weekly ETL 실행 스크립트
# Crontab: 0 2 * * 1 /data/etl/inspig/run_weekly.sh

# 스크립트 디렉토리로 이동
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# 로그 디렉토리 생성
LOG_DIR="$SCRIPT_DIR/logs"
mkdir -p "$LOG_DIR"

# 타임스탬프
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$LOG_DIR/cron_$TIMESTAMP.log"

echo "========================================" >> "$LOG_FILE"
echo "InsightPig Weekly ETL 시작: $(date)" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

# Conda 환경 활성화 (운영 서버)
if [ -f "/home/pigplan/anaconda3/etc/profile.d/conda.sh" ]; then
    source /home/pigplan/anaconda3/etc/profile.d/conda.sh
    conda activate inspig-etl
    echo "Conda 환경: inspig-etl" >> "$LOG_FILE"
elif [ -f "/data/anaconda/anaconda3/etc/profile.d/conda.sh" ]; then
    source /data/anaconda/anaconda3/etc/profile.d/conda.sh
    conda activate inspig-etl
    echo "Conda 환경: inspig-etl (data)" >> "$LOG_FILE"
fi

# Python 버전 확인
python --version >> "$LOG_FILE" 2>&1

# ETL 실행
python run_etl.py weekly >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

echo "========================================" >> "$LOG_FILE"
echo "종료 코드: $EXIT_CODE" >> "$LOG_FILE"
echo "종료 시각: $(date)" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

# 오래된 로그 정리 (30일 이상)
find "$LOG_DIR" -name "cron_*.log" -mtime +30 -delete

exit $EXIT_CODE
