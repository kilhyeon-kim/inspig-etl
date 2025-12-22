"""
로깅 설정
"""
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional


def setup_logger(
    name: str = "inspig_etl",
    log_path: Optional[str] = None,
    level: int = logging.INFO
) -> logging.Logger:
    """로깅 설정

    Args:
        name: 로거 이름
        log_path: 로그 파일 경로 (None이면 ./logs)
        level: 로그 레벨

    Returns:
        설정된 Logger 인스턴스
    """
    if log_path is None:
        log_path = Path(__file__).parent.parent.parent / "logs"
    else:
        log_path = Path(log_path)

    log_path.mkdir(parents=True, exist_ok=True)

    log_file = log_path / f"{name}_{datetime.now():%Y%m%d}.log"

    # 로거 설정
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # 기존 핸들러 제거 (중복 방지)
    if logger.handlers:
        logger.handlers.clear()

    # 포맷터
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # 파일 핸들러
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # 콘솔 핸들러
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger
