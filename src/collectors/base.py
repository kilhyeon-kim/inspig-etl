"""
데이터 수집기 베이스 클래스
"""
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from ..common import Config, Database


class BaseCollector(ABC):
    """데이터 수집기 베이스 클래스"""

    def __init__(self, config: Optional[Config] = None, db: Optional[Database] = None):
        self.config = config or Config()
        self.db = db or Database(self.config)
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def collect(self, **kwargs) -> List[Dict[str, Any]]:
        """데이터 수집

        Returns:
            수집된 데이터 리스트
        """
        pass

    @abstractmethod
    def save(self, data: List[Dict[str, Any]]) -> int:
        """데이터 저장

        Args:
            data: 저장할 데이터

        Returns:
            저장된 레코드 수
        """
        pass

    def run(self, **kwargs) -> int:
        """수집 및 저장 실행

        Returns:
            처리된 레코드 수
        """
        self.logger.info(f"=== {self.__class__.__name__} 시작 ===")

        try:
            # 데이터 수집
            data = self.collect(**kwargs)
            self.logger.info(f"수집 완료: {len(data)}건")

            if not data:
                self.logger.info("수집된 데이터 없음")
                return 0

            # 데이터 저장
            saved_count = self.save(data)
            self.logger.info(f"저장 완료: {saved_count}건")

            return saved_count

        except Exception as e:
            self.logger.error(f"처리 실패: {e}", exc_info=True)
            raise
        finally:
            self.logger.info(f"=== {self.__class__.__name__} 종료 ===")
