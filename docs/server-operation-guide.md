# InsightPig ETL 서버 운영 가이드

## 1. 서버 정보

| 항목 | 값 |
|------|-----|
| 서버 IP | 10.4.35.10 |
| ETL API 포트 | 8001 |
| 설치 경로 | /data/etl/inspig |
| 계정 | pigplan |
| Conda 환경 | inspig-etl |

---

## 2. 초기 설치 (최초 1회)

### 2.1 서버 접속

```bash
ssh -i "E:/ssh key/sshkey/aws/ProdPigplanKey.pem" pigplan@10.4.35.10
```

### 2.2 디렉토리 생성 및 이동

```bash
mkdir -p /data/etl/inspig/logs
cd /data/etl/inspig
```

### 2.3 Conda 환경 설정

```bash
source /data/anaconda/anaconda3/etc/profile.d/conda.sh
conda activate inspig-etl
pip install -r requirements.txt
```

### 2.4 설정 파일 생성

```bash
cp config.ini.example config.ini
vi config.ini  # DB 패스워드, API 키 입력
```

### 2.5 ETL 테스트

```bash
source /data/anaconda/anaconda3/etc/profile.d/conda.sh
conda activate inspig-etl
python run_etl.py --dry-run
python run_etl.py --test
```

---

## 3. API 서버 자동 기동 설정 (systemd)

### 3.1 서비스 파일 등록

```bash
sudo cp /data/etl/inspig/inspig-etl-api.service /etc/systemd/system/
sudo systemctl daemon-reload
```

### 3.2 서비스 활성화 (부팅 시 자동 시작)

```bash
sudo systemctl enable inspig-etl-api
```

### 3.3 서비스 시작

```bash
sudo systemctl start inspig-etl-api
```

### 3.4 서비스 상태 확인

```bash
sudo systemctl status inspig-etl-api
```

### 3.5 헬스체크

```bash
curl http://localhost:8001/health
```

---

## 4. 주간 배치 ETL 설정 (Crontab)

### 4.1 Crontab 편집

```bash
crontab -e
```

### 4.2 스케줄 등록 (매주 월요일 02:00)

```
0 2 * * 1 /data/etl/inspig/run_weekly.sh
```

---

## 5. 일상 운영 명령어

### 5.1 API 서버 관리

```bash
# 서비스 시작
sudo systemctl start inspig-etl-api

# 서비스 중지
sudo systemctl stop inspig-etl-api

# 서비스 재시작
sudo systemctl restart inspig-etl-api

# 서비스 상태 확인
sudo systemctl status inspig-etl-api

# 로그 실시간 확인
sudo journalctl -u inspig-etl-api -f
```

### 5.2 API 헬스체크

```bash
curl http://localhost:8001/health
```

### 5.3 수동 ETL 실행

```bash
cd /data/etl/inspig
source /data/anaconda/anaconda3/etc/profile.d/conda.sh
conda activate inspig-etl

# 전체 농장 주간 리포트 생성
python run_etl.py

# 특정 농장만 실행
python run_etl.py --farm 2807

# 테스트 모드 (DB 저장 없음)
python run_etl.py --dry-run

# 특정 날짜 기준 실행
python run_etl.py --date 20251229
```

### 5.4 로그 확인

```bash
# ETL 배치 로그
tail -f /data/etl/inspig/logs/etl_*.log

# API 서버 로그
sudo journalctl -u inspig-etl-api -n 100
```

---

## 6. 배포 후 재기동

배포 스크립트(deploy-etl.sh)가 자동으로 재기동합니다.
수동 재기동이 필요한 경우:

```bash
sudo systemctl restart inspig-etl-api
```

재기동 후 확인:

```bash
# 상태 확인
sudo systemctl status inspig-etl-api

# 헬스체크
curl http://localhost:8001/health
```

---

## 7. sudoers 설정 (선택)

pigplan 계정에서 비밀번호 없이 서비스 관리를 하려면:

```bash
# root 권한으로 실행
sudo visudo -f /etc/sudoers.d/pigplan
```

다음 내용 추가:

```
pigplan ALL=(ALL) NOPASSWD: /bin/systemctl restart inspig-etl-api
pigplan ALL=(ALL) NOPASSWD: /bin/systemctl start inspig-etl-api
pigplan ALL=(ALL) NOPASSWD: /bin/systemctl stop inspig-etl-api
pigplan ALL=(ALL) NOPASSWD: /bin/systemctl status inspig-etl-api
```

---

## 8. 트러블슈팅

### 8.1 API 서버 연결 안됨

```bash
# 서비스 상태 확인
sudo systemctl status inspig-etl-api

# 포트 확인
netstat -tlnp | grep 8001

# 로그 확인
sudo journalctl -u inspig-etl-api -n 50
```

### 8.2 ETL 실행 오류

```bash
# Conda 환경 활성화 확인
source /data/anaconda/anaconda3/etc/profile.d/conda.sh
conda activate inspig-etl
which python

# 의존성 재설치
pip install -r requirements.txt

# 설정 파일 확인
cat /data/etl/inspig/config.ini
```

### 8.3 DB 연결 오류

```bash
# Oracle 클라이언트 확인
echo $ORACLE_HOME
echo $LD_LIBRARY_PATH

# 연결 테스트
python -c "from src.common import Config; c=Config(); print(c.oracle)"
```

---

## 9. API 엔드포인트

| 메서드 | URL | 설명 |
|--------|-----|------|
| GET | /health | 헬스체크 |
| POST | /api/etl/run-farm | 농장별 ETL 실행 |
| GET | /api/etl/status/{farm_no} | 리포트 상태 조회 |

### 9.1 ETL 실행 예시

```bash
curl -X POST http://10.4.35.10:8001/api/etl/run-farm \
  -H "Content-Type: application/json" \
  -d '{"farmNo": 2807, "dayGb": "WEEK"}'
```

### 9.2 상태 조회 예시

```bash
curl http://10.4.35.10:8001/api/etl/status/2807?day_gb=WEEK
```

---

## 10. 연락처

- 시스템 관리자: pigplan 담당자
- 문의: ETL 서버 관련 이슈 발생 시 담당자에게 연락
