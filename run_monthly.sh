#!/bin/bash
# InsightPig Monthly ETL 실행 스크립트
# 매월 1일 04:00 KST 실행
#
# Crontab 설정:
# 매일 UTC 19:00에 실행하되, 스크립트 내부에서 KST 기준 1일인지 체크
# 0 19 * * * /data/etl/inspig/run_monthly.sh

# 스크립트 디렉토리로 이동
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# 한국 시간(KST) 기준으로 날짜 확인
# UTC 19:00 + 9시간 = KST 04:00 (다음날)
KST_DAY=$(TZ='Asia/Seoul' date +%d)

# 1일이 아니면 종료 (월간 ETL은 매월 1일에만 실행)
if [ "$KST_DAY" != "01" ]; then
    exit 0
fi

# 로그 디렉토리 생성
LOG_DIR="$SCRIPT_DIR/logs"
mkdir -p "$LOG_DIR"

# 타임스탬프
TIMESTAMP=$(TZ='Asia/Seoul' date +%Y%m%d_%H%M%S)
LOG_FILE="$LOG_DIR/monthly_$TIMESTAMP.log"

echo "========================================" >> "$LOG_FILE"
echo "InsightPig Monthly ETL 시작: $(TZ='Asia/Seoul' date)" >> "$LOG_FILE"
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

# Monthly ETL 실행 (향후 개발 예정)
echo "Monthly ETL 미구현 - 향후 개발 예정" >> "$LOG_FILE"
# python run_etl.py monthly >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

echo "========================================" >> "$LOG_FILE"
echo "종료 코드: $EXIT_CODE" >> "$LOG_FILE"
echo "종료 시각: $(TZ='Asia/Seoul' date)" >> "$LOG_FILE"
echo "========================================" >> "$LOG_FILE"

# 오래된 로그 정리 (30일 이상)
find "$LOG_DIR" -name "monthly_*.log" -mtime +30 -delete

exit $EXIT_CODE