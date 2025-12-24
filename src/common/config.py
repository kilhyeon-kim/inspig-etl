"""
ETL 설정 관리
"""
import configparser
import os
from pathlib import Path
from typing import Optional


class Config:
    """설정 파일 관리 클래스"""

    _instance: Optional['Config'] = None
    _config: Optional[configparser.ConfigParser] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if self._config is None:
            self._config = configparser.ConfigParser()
            self._load()

    def _load(self):
        """설정 파일 로드"""
        # 프로젝트 루트의 config.ini
        root_dir = Path(__file__).parent.parent.parent
        config_path = root_dir / "config.ini"

        if not config_path.exists():
            raise FileNotFoundError(
                f"설정 파일을 찾을 수 없습니다: {config_path}\n"
                "config.ini.example을 복사하여 config.ini를 생성하세요."
            )

        self._config.read(config_path, encoding='utf-8')

    @property
    def database(self) -> dict:
        """데이터베이스 설정"""
        return {
            'user': self._config.get('database', 'user'),
            'password': self._config.get('database', 'password'),
            'dsn': self._config.get('database', 'dsn'),
        }

    @property
    def processing(self) -> dict:
        """처리 설정"""
        return {
            'parallel': self._config.getint('processing', 'parallel', fallback=4),
            'test_mode': self._config.get('processing', 'test_mode', fallback='N'),
        }

    @property
    def logging(self) -> dict:
        """로깅 설정"""
        return {
            'log_path': self._config.get('logging', 'log_path', fallback='./logs'),
        }

    @property
    def api(self) -> dict:
        """API 설정"""
        return {
            'productivity_base_url': self._config.get('api', 'productivity_base_url', fallback='http://10.4.35.10:11000'),
            'productivity_timeout': self._config.getint('api', 'productivity_timeout', fallback=60),
            'productivity_workers': self._config.getint('api', 'productivity_workers', fallback=4),
        }

    @property
    def weather(self) -> dict:
        """기상청 API 설정"""
        return {
            'api_key': self._config.get('weather', 'api_key', fallback=''),
            'base_url': self._config.get('weather', 'base_url', fallback='https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0'),
        }

    def get(self, section: str, key: str, fallback=None):
        """범용 설정 조회"""
        return self._config.get(section, key, fallback=fallback)
