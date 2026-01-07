from __future__ import annotations

FIELDS = [
    "city",
    "building_name",
    "floors_units",
    "lease_start_date",
    "lease_tenure_months",
    "lease_end_date",
    "handover_date",
    "rent_free_period_months",
    "rent_start_date",
    "lock_in_period",
    "lock_in_end_date",
    "termination_notice_period_months",
    "renewal_notice_period_months",
    "renewal_option",
    "super_builtup_area_sqft",
    "carpet_area_sqft",
    "efficiency",
    "cam_area_sqft",
    "monthly_rent_rs",
    "rate_per_sqft_rs",
    "monthly_cam_rs",
    "parking_4w_included",
    "parking_2w_included",
    "parking_charges_rs",
    "ifrsd_rs",
    "stamp_duty_rs",
]

DATE_FIELDS = {
    "lease_start_date",
    "lease_end_date",
    "rent_start_date",
    "handover_date",
    "lock_in_end_date",
}

INT_FIELDS = {
    "lease_tenure_months",
    "lock_in_period",
    "rent_free_period_months",
    "termination_notice_period_months",
    "renewal_notice_period_months",
    "parking_4w_included",
    "parking_2w_included",
}

FLOAT_FIELDS = {
    "carpet_area_sqft",
    "super_builtup_area_sqft",
    "cam_area_sqft",
    "efficiency",
    "rate_per_sqft_rs",
}

MONEY_FIELDS = {
    "monthly_cam_rs",
    "monthly_rent_rs",
    "parking_charges_rs",
    "stamp_duty_rs",
    "ifrsd_rs",
}

SCHEMA_VERSION = 1


def coerce_value(field: str, raw: object) -> object:
    if raw is None:
        return None
    if isinstance(raw, str):
        value = raw.strip()
        if value == "":
            return None
        if value.lower() in {"none", "null", "na", "n/a"}:
            return None
    else:
        value = raw

    if field in DATE_FIELDS:
        return value

    if field in INT_FIELDS:
        try:
            return int(float(str(value).replace(",", "")))
        except Exception:
            return value

    if field in FLOAT_FIELDS or field in MONEY_FIELDS:
        try:
            return float(str(value).replace(",", ""))
        except Exception:
            return value

    return value
