#!/bin/bash
# InsightPig ETL 배포 스크립트
# 사용법: ./deploy-etl.sh

set -e

# ========================================
# 설정
# ========================================
ETL_SERVER="10.4.35.10"
ETL_USER="pigplan"
SSH_KEY="E:/ssh key/sshkey/aws/ProdPigplanKey.pem"
REMOTE_PATH="/data/etl/inspig-weekly"

# 로컬 스크립트 경로
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 배포할 파일 목록
DEPLOY_FILES=(
    "weekly_report_etl.py"
    "run_weekly.sh"
    "requirements.txt"
)

# ========================================
# 배포 시작
# ========================================
echo "========================================"
echo "InsightPig ETL 배포"
echo "========================================"
echo "서버: $ETL_USER@$ETL_SERVER"
echo "경로: $REMOTE_PATH"
echo ""

# SSH 연결 테스트
echo "[1/4] SSH 연결 테스트..."
ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$ETL_USER@$ETL_SERVER" "echo '연결 성공'"

# 원격 디렉토리 생성
echo "[2/4] 원격 디렉토리 확인..."
ssh -i "$SSH_KEY" "$ETL_USER@$ETL_SERVER" "mkdir -p $REMOTE_PATH/logs"

# 파일 배포
echo "[3/4] 파일 배포..."
for file in "${DEPLOY_FILES[@]}"; do
    if [ -f "$SCRIPT_DIR/$file" ]; then
        echo "  - $file"
        scp -i "$SSH_KEY" "$SCRIPT_DIR/$file" "$ETL_USER@$ETL_SERVER:$REMOTE_PATH/"
    else
        echo "  - $file (없음, 건너뜀)"
    fi
done

# 실행 권한 부여
echo "[4/4] 실행 권한 설정..."
ssh -i "$SSH_KEY" "$ETL_USER@$ETL_SERVER" "chmod +x $REMOTE_PATH/run_weekly.sh"

echo ""
echo "========================================"
echo "배포 완료!"
echo "========================================"
echo ""
echo "다음 단계:"
echo "1. config.ini 설정 (최초 1회):"
echo "   ssh -i \"$SSH_KEY\" $ETL_USER@$ETL_SERVER"
echo "   cp $REMOTE_PATH/config.ini.example $REMOTE_PATH/config.ini"
echo "   vi $REMOTE_PATH/config.ini"
echo ""
echo "2. 테스트 실행:"
echo "   ssh -i \"$SSH_KEY\" $ETL_USER@$ETL_SERVER"
echo "   cd $REMOTE_PATH"
echo "   conda activate inspig-weekly-etl"
echo "   python weekly_report_etl.py --test --dry-run"
echo ""
echo "3. Crontab 등록 (최초 1회):"
echo "   0 2 * * 1 $REMOTE_PATH/run_weekly.sh"
