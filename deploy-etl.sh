#!/bin/bash
# InsightPig ETL 배포 스크립트
# 로컬 → 운영 서버 (10.4.35.10) 배포

set -e

# ========================================
# 설정
# ========================================
REMOTE_HOST="10.4.35.10"
REMOTE_USER="pigplan"
REMOTE_PATH="/data/etl/inspig"
SSH_KEY="E:/ssh key/sshkey/aws/ProdPigplanKey.pem"

# 색상
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}InsightPig ETL 배포 시작${NC}"
echo -e "${GREEN}========================================${NC}"

# 스크립트 디렉토리
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo -e "${YELLOW}1. 배포 대상 파일 확인${NC}"
echo "   소스: $SCRIPT_DIR"
echo "   대상: $REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH"

# 배포할 파일 목록 (필수 파일만)
FILES=(
    "run_etl.py"
    "run_api.py"
    "run_weekly.sh"
    "weather_etl.py"
    "requirements.txt"
    "config.ini.example"
    "inspig-etl-api.service"
)

DIRS=(
    "src"
)

echo ""
echo -e "${YELLOW}2. SSH 연결 테스트${NC}"
ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$REMOTE_USER@$REMOTE_HOST" "echo '연결 성공'"

echo ""
echo -e "${YELLOW}3. 원격 디렉토리 생성${NC}"
ssh -i "$SSH_KEY" "$REMOTE_USER@$REMOTE_HOST" "mkdir -p $REMOTE_PATH/logs"

echo ""
echo -e "${YELLOW}4. 파일 전송${NC}"

# 개별 파일 전송
for file in "${FILES[@]}"; do
    if [ -f "$file" ]; then
        echo "   $file"
        scp -i "$SSH_KEY" "$file" "$REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH/"
    else
        echo "   $file (없음, 건너뜀)"
    fi
done

# 디렉토리 전송
for dir in "${DIRS[@]}"; do
    if [ -d "$dir" ]; then
        echo "   $dir/"
        scp -i "$SSH_KEY" -r "$dir" "$REMOTE_USER@$REMOTE_HOST:$REMOTE_PATH/"
    fi
done

echo ""
echo -e "${YELLOW}5. 실행 권한 설정${NC}"
ssh -i "$SSH_KEY" "$REMOTE_USER@$REMOTE_HOST" "chmod +x $REMOTE_PATH/run_weekly.sh $REMOTE_PATH/run_etl.py $REMOTE_PATH/run_api.py"

echo ""
echo -e "${YELLOW}6. 배포 확인${NC}"
ssh -i "$SSH_KEY" "$REMOTE_USER@$REMOTE_HOST" "ls -la $REMOTE_PATH/"

echo ""
echo -e "${YELLOW}7. API 서버 재기동${NC}"
# sudo 권한이 필요하므로 pigplan 계정으로는 직접 실행 불가
# root 계정으로 재기동하거나, sudoers 설정 필요
ssh -i "$SSH_KEY" "$REMOTE_USER@$REMOTE_HOST" "sudo systemctl restart inspig-etl-api 2>/dev/null && echo 'API 서버 재기동 완료' || echo 'API 서버 재기동 실패 (수동 재기동 필요)'"

echo ""
echo -e "${YELLOW}8. API 서버 상태 확인${NC}"
sleep 2
curl -s --connect-timeout 5 "http://$REMOTE_HOST:8001/health" && echo "" || echo "헬스체크 실패"

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}배포 완료!${NC}"
echo -e "${GREEN}========================================${NC}"

echo ""
echo -e "${YELLOW}다음 단계:${NC}"
echo "1. 서버 접속:"
echo "   ssh -i \"$SSH_KEY\" $REMOTE_USER@$REMOTE_HOST"
echo ""
echo "2. config.ini 설정:"
echo "   cd $REMOTE_PATH"
echo "   cp config.ini.example config.ini"
echo "   vi config.ini  # DB 패스워드, API 키 입력"
echo ""
echo "3. Conda 환경 설정 (최초 1회):"
echo "   conda create -n inspig-etl python=3.8"
echo "   conda activate inspig-etl"
echo "   pip install -r $REMOTE_PATH/requirements.txt"
echo ""
echo "4. 테스트 실행:"
echo "   python run_etl.py --dry-run"
echo "   python run_etl.py --test"
echo ""
echo "5. Crontab 등록 (주간 배치):"
echo "   crontab -e"
echo "   0 2 * * 1 $REMOTE_PATH/run_weekly.sh"
echo ""
echo "6. API 서버 자동 기동 설정 (systemd):"
echo "   sudo cp $REMOTE_PATH/inspig-etl-api.service /etc/systemd/system/"
echo "   sudo systemctl daemon-reload"
echo "   sudo systemctl enable inspig-etl-api"
echo "   sudo systemctl start inspig-etl-api"
echo "   sudo systemctl status inspig-etl-api"
echo ""
echo "7. API 서버 확인:"
echo "   curl http://localhost:8001/health"
