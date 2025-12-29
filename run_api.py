#!/usr/bin/env python3
"""
InsightPig ETL API 서버 실행 스크립트

실행:
    python run_api.py
    python run_api.py --port 8001
    python run_api.py --host 127.0.0.1 --port 8000

API 문서:
    http://localhost:8000/docs (Swagger UI)
    http://localhost:8000/redoc (ReDoc)
"""

import argparse
import sys
from pathlib import Path

# 프로젝트 루트를 path에 추가
sys.path.insert(0, str(Path(__file__).parent))

from src.api import run_server


def main():
    parser = argparse.ArgumentParser(description='InsightPig ETL API 서버')
    parser.add_argument('--host', default='0.0.0.0', help='바인드 호스트 (기본: 0.0.0.0)')
    parser.add_argument('--port', type=int, default=8000, help='포트 (기본: 8000)')

    args = parser.parse_args()
    run_server(host=args.host, port=args.port)


if __name__ == '__main__':
    main()
