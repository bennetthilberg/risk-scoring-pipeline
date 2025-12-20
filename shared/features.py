FEATURE_ORDER = [
    "txn_count_24h",
    "txn_amount_sum_24h",
    "failed_logins_1h",
    "account_age_days",
    "unique_countries_7d",
    "avg_txn_amount_30d",
]

FEATURE_DEFAULTS = {
    "txn_count_24h": 0,
    "txn_amount_sum_24h": 0.0,
    "failed_logins_1h": 0,
    "account_age_days": 0,
    "unique_countries_7d": 0,
    "avg_txn_amount_30d": 0.0,
}
