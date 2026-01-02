"""
API 키 관리자
- TS_API_KEY_INFO 테이블 기반 API 키 로드밸런싱
- REQ_CNT가 가장 적은 키 우선 사용
- limit 발생 시 다음 키로 자동 전환
"""
import logging
from typing import Dict, List, Optional
from urllib.parse import unquote

from .database import Database

logger = logging.getLogger(__name__)


class ApiKeyManager:
    """TS_API_KEY_INFO 테이블 기반 API 키 관리자

    사용법:
        manager = ApiKeyManager(db)
        manager.load_keys()

        while manager.has_available_key():
            api_key = manager.get_current_key()
            result = call_api(api_key)

            if result.success:
                manager.increment_count(api_key)
                break
            elif result.is_limit_error:
                manager.mark_key_exhausted(api_key)
                continue
    """

    # API 호출 제한 에러 코드 (공공데이터포털)
    LIMIT_ERROR_CODES = ['22', '99']  # 22: 서비스 요청제한횟수 초과

    def __init__(self, db: Database):
        self.db = db
        self._api_keys: List[Dict] = []
        self._exhausted_keys: set = set()  # limit 발생한 키 인덱스

    def load_keys(self):
        """DB에서 API 키 목록 조회 (REQ_CNT 오름차순)

        TS_API_KEY_INFO 테이블에서 API 키를 조회하여
        호출 횟수가 적은 순서대로 정렬합니다.
        """
        sql = """
            SELECT API_KEY, CREATE_USER, REQ_CNT
            FROM TS_API_KEY_INFO
            ORDER BY REQ_CNT ASC
        """
        self._api_keys = self.db.fetch_dict(sql)
        self._exhausted_keys = set()

        if self._api_keys:
            logger.info(f"API 키 {len(self._api_keys)}개 로드 완료")
            for key in self._api_keys:
                logger.debug(f"  - {key['CREATE_USER']}: REQ_CNT={key['REQ_CNT']}")
        else:
            logger.warning("등록된 API 키가 없습니다.")

    def get_current_key(self) -> Optional[str]:
        """현재 사용할 API 키 반환

        사용 가능한 키 중 REQ_CNT가 가장 적은 키를 반환합니다.
        DB에 URL 인코딩된 상태로 저장되어 있으므로 디코딩하여 반환합니다.

        Returns:
            디코딩된 API 키 문자열, 없으면 None
        """
        available_keys = [
            (i, k) for i, k in enumerate(self._api_keys)
            if i not in self._exhausted_keys
        ]

        if not available_keys:
            logger.error("사용 가능한 API 키가 없습니다.")
            return None

        # 가장 REQ_CNT가 적은 키 사용 (이미 정렬됨)
        idx, key_info = available_keys[0]
        api_key = key_info['API_KEY']

        # URL 인코딩된 키 디코딩
        decoded_key = unquote(api_key)

        logger.debug(f"API 키 사용: {key_info['CREATE_USER']}")
        return decoded_key

    def get_key_owner(self, api_key: str) -> Optional[str]:
        """API 키 소유자 조회

        Args:
            api_key: 디코딩된 API 키

        Returns:
            CREATE_USER 값
        """
        for key_info in self._api_keys:
            if unquote(key_info['API_KEY']) == api_key:
                return key_info['CREATE_USER']
        return None

    def mark_key_exhausted(self, api_key: str):
        """현재 키를 limit 상태로 표시

        해당 키는 더 이상 사용되지 않으며,
        다음 get_current_key() 호출 시 다른 키가 반환됩니다.

        Args:
            api_key: 디코딩된 API 키
        """
        for i, key_info in enumerate(self._api_keys):
            if unquote(key_info['API_KEY']) == api_key:
                self._exhausted_keys.add(i)
                logger.warning(f"API 키 limit 도달: {key_info['CREATE_USER']}")
                break

    def increment_count(self, api_key: str):
        """API 호출 성공 시 REQ_CNT 증가

        Args:
            api_key: 디코딩된 API 키
        """
        # 원본 인코딩 키 찾기
        encoded_key = None
        for key_info in self._api_keys:
            if unquote(key_info['API_KEY']) == api_key:
                encoded_key = key_info['API_KEY']
                break

        if encoded_key:
            sql = """
                UPDATE TS_API_KEY_INFO
                SET REQ_CNT = REQ_CNT + 1
                WHERE API_KEY = :API_KEY
            """
            self.db.execute(sql, {'API_KEY': encoded_key})
            # 커밋은 상위에서 일괄 처리

    def has_available_key(self) -> bool:
        """사용 가능한 키가 있는지 확인

        Returns:
            True if 사용 가능한 키 존재
        """
        return len(self._exhausted_keys) < len(self._api_keys)

    def get_stats(self) -> Dict:
        """API 키 사용 현황 조회

        Returns:
            {
                'total': 전체 키 수,
                'available': 사용 가능한 키 수,
                'exhausted': limit 도달한 키 수,
                'keys': [{owner, req_cnt, exhausted}, ...]
            }
        """
        keys_info = []
        for i, key in enumerate(self._api_keys):
            keys_info.append({
                'owner': key['CREATE_USER'],
                'req_cnt': key['REQ_CNT'],
                'exhausted': i in self._exhausted_keys,
            })

        return {
            'total': len(self._api_keys),
            'available': len(self._api_keys) - len(self._exhausted_keys),
            'exhausted': len(self._exhausted_keys),
            'keys': keys_info,
        }

    def reset_exhausted(self):
        """exhausted 상태 초기화

        모든 키를 다시 사용 가능 상태로 변경합니다.
        (REQ_CNT는 매일 자정에 DB에서 초기화됨)
        """
        self._exhausted_keys = set()
        logger.info("API 키 exhausted 상태 초기화")

    @staticmethod
    def is_limit_error(result_code: str) -> bool:
        """API 응답 코드가 limit 에러인지 확인

        Args:
            result_code: API 응답의 resultCode

        Returns:
            True if limit 에러
        """
        return result_code in ApiKeyManager.LIMIT_ERROR_CODES
