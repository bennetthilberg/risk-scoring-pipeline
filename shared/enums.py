from enum import Enum


class EventType(str, Enum):
    SIGNUP = "signup"
    LOGIN = "login"
    TRANSACTION = "transaction"


class RiskBand(str, Enum):
    LOW = "low"
    MEDIUM = "med"
    HIGH = "high"


class ProcessingStatus(str, Enum):
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


CURRENT_SCHEMA_VERSION = 1

RISK_BAND_THRESHOLDS = {
    "low_max": 0.33,
    "med_max": 0.66,
}


def score_to_band(score: float) -> RiskBand:
    if score < RISK_BAND_THRESHOLDS["low_max"]:
        return RiskBand.LOW
    elif score < RISK_BAND_THRESHOLDS["med_max"]:
        return RiskBand.MEDIUM
    return RiskBand.HIGH
