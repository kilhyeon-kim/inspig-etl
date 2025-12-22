"""
주간 리포트 처리 모듈

각 Oracle 프로시저에 대응하는 Python 처리 클래스:
- ConfigProcessor: SP_INS_WEEK_CONFIG (농장 설정값)
- ModonProcessor: SP_INS_WEEK_MODON_POPUP (모돈 현황)
- AlertProcessor: SP_INS_WEEK_ALERT_POPUP (관리대상 모돈)
- MatingProcessor: SP_INS_WEEK_GB_POPUP (교배)
- FarrowingProcessor: SP_INS_WEEK_BM_POPUP (분만)
- WeaningProcessor: SP_INS_WEEK_EU_POPUP (이유)
- AccidentProcessor: SP_INS_WEEK_SG_POPUP (임신사고)
- CullingProcessor: SP_INS_WEEK_DOPE_POPUP (도태폐사)
- ShipmentProcessor: SP_INS_WEEK_SHIP_POPUP (출하)
- ScheduleProcessor: SP_INS_WEEK_SCHEDULE_POPUP (금주 예정)
"""

from .base import BaseProcessor
from .config import ConfigProcessor
from .modon import ModonProcessor
from .alert import AlertProcessor
from .mating import MatingProcessor
from .farrowing import FarrowingProcessor
from .weaning import WeaningProcessor
from .accident import AccidentProcessor
from .culling import CullingProcessor
from .shipment import ShipmentProcessor
from .schedule import ScheduleProcessor

__all__ = [
    'BaseProcessor',
    'ConfigProcessor',
    'ModonProcessor',
    'AlertProcessor',
    'MatingProcessor',
    'FarrowingProcessor',
    'WeaningProcessor',
    'AccidentProcessor',
    'CullingProcessor',
    'ShipmentProcessor',
    'ScheduleProcessor',
]
