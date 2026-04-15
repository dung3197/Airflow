from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastavro import parse_schema, writer


# ============================================================
# 1) Global configuration
# ============================================================

TZ_UTC_PLUS_7 = timezone(timedelta(hours=7))

OUTPUT_DIR = Path("output")
OUTPUT_FILE = OUTPUT_DIR / "ogg_customer_events.avro"
LOG_FILE = OUTPUT_DIR / "ogg_customer_events.log"

TARGET_FILE_SIZE_MB = 50
TARGET_FILE_SIZE_BYTES = TARGET_FILE_SIZE_MB * 1024 * 1024

TABLE_NAME = "STOCK.CUSTOMER"


# ============================================================
# 2) Logging setup
# ============================================================

def setup_logger(log_file: Path) -> logging.Logger:
    log_file.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("ogg_avro_generator")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


# ============================================================
# 3) Helper functions
# ============================================================

def now_iso() -> str:
    return datetime.now(TZ_UTC_PLUS_7).isoformat()


def nullable(avro_type: Any) -> List[Any]:
    return ["null", avro_type]


def decimal_to_string(value: Decimal | None) -> Optional[str]:
    return None if value is None else str(value)


# ============================================================
# 4) Customer schema
# ============================================================

CUSTOMER_COLUMNS: List[Dict[str, Any]] = [
    {"name": "customer_id", "type": nullable("long"), "default": None},
    {"name": "customer_code", "type": nullable("string"), "default": None},
    {"name": "customer_type", "type": nullable("string"), "default": None},
    {"name": "full_name", "type": nullable("string"), "default": None},
    {"name": "short_name", "type": nullable("string"), "default": None},
    {"name": "gender", "type": nullable("string"), "default": None},
    {"name": "date_of_birth", "type": nullable("string"), "default": None},
    {"name": "nationality", "type": nullable("string"), "default": None},
    {"name": "marital_status", "type": nullable("string"), "default": None},
    {"name": "tax_code", "type": nullable("string"), "default": None},
    {"name": "customer_status", "type": nullable("string"), "default": None},
    {"name": "risk_level", "type": nullable("string"), "default": None},
    {"name": "segment", "type": nullable("string"), "default": None},
    {"name": "branch_code", "type": nullable("string"), "default": None},
    {"name": "broker_code", "type": nullable("string"), "default": None},
    {"name": "account_no", "type": nullable("string"), "default": None},
    {"name": "trading_account_no", "type": nullable("string"), "default": None},
    {"name": "custody_account_no", "type": nullable("string"), "default": None},
    {"name": "email", "type": nullable("string"), "default": None},
    {"name": "mobile_phone", "type": nullable("string"), "default": None},
    {"name": "home_phone", "type": nullable("string"), "default": None},
    {"name": "office_phone", "type": nullable("string"), "default": None},
    {"name": "fax", "type": nullable("string"), "default": None},
    {"name": "address_line1", "type": nullable("string"), "default": None},
    {"name": "address_line2", "type": nullable("string"), "default": None},
    {"name": "ward", "type": nullable("string"), "default": None},
    {"name": "district", "type": nullable("string"), "default": None},
    {"name": "province", "type": nullable("string"), "default": None},
    {"name": "country", "type": nullable("string"), "default": None},
    {"name": "postal_code", "type": nullable("string"), "default": None},
    {"name": "contact_person", "type": nullable("string"), "default": None},
    {"name": "occupation", "type": nullable("string"), "default": None},
    {"name": "company_name", "type": nullable("string"), "default": None},
    {"name": "job_title", "type": nullable("string"), "default": None},
    {"name": "annual_income", "type": nullable("string"), "default": None},
    {"name": "net_worth", "type": nullable("string"), "default": None},
    {"name": "source_of_funds", "type": nullable("string"), "default": None},
    {"name": "investment_experience", "type": nullable("string"), "default": None},
    {"name": "risk_appetite", "type": nullable("string"), "default": None},
    {"name": "kyc_status", "type": nullable("string"), "default": None},
    {"name": "kyc_date", "type": nullable("string"), "default": None},
    {"name": "aml_flag", "type": nullable("boolean"), "default": None},
    {"name": "pep_flag", "type": nullable("boolean"), "default": None},
    {"name": "sanction_flag", "type": nullable("boolean"), "default": None},
    {"name": "fatca_flag", "type": nullable("boolean"), "default": None},
    {"name": "fatca_country", "type": nullable("string"), "default": None},
    {"name": "crs_flag", "type": nullable("boolean"), "default": None},
    {"name": "crs_country", "type": nullable("string"), "default": None},
    {"name": "id_type", "type": nullable("string"), "default": None},
    {"name": "id_number", "type": nullable("string"), "default": None},
    {"name": "id_issue_date", "type": nullable("string"), "default": None},
    {"name": "id_issue_place", "type": nullable("string"), "default": None},
    {"name": "id_expiry_date", "type": nullable("string"), "default": None},
    {"name": "passport_number", "type": nullable("string"), "default": None},
    {"name": "passport_issue_date", "type": nullable("string"), "default": None},
    {"name": "passport_expiry_date", "type": nullable("string"), "default": None},
    {"name": "bank_account_no", "type": nullable("string"), "default": None},
    {"name": "bank_name", "type": nullable("string"), "default": None},
    {"name": "bank_branch", "type": nullable("string"), "default": None},
    {"name": "settlement_method", "type": nullable("string"), "default": None},
    {"name": "cash_balance", "type": nullable("string"), "default": None},
    {"name": "margin_balance", "type": nullable("string"), "default": None},
    {"name": "loan_balance", "type": nullable("string"), "default": None},
    {"name": "credit_limit", "type": nullable("string"), "default": None},
    {"name": "available_limit", "type": nullable("string"), "default": None},
    {"name": "portfolio_value", "type": nullable("string"), "default": None},
    {"name": "realized_pnl", "type": nullable("string"), "default": None},
    {"name": "unrealized_pnl", "type": nullable("string"), "default": None},
    {"name": "last_trade_date", "type": nullable("string"), "default": None},
    {"name": "last_login_ts", "type": nullable("string"), "default": None},
    {"name": "preferred_language", "type": nullable("string"), "default": None},
    {"name": "preferred_channel", "type": nullable("string"), "default": None},
    {"name": "sms_opt_in", "type": nullable("boolean"), "default": None},
    {"name": "email_opt_in", "type": nullable("boolean"), "default": None},
    {"name": "mobile_app_opt_in", "type": nullable("boolean"), "default": None},
    {"name": "marketing_opt_in", "type": nullable("boolean"), "default": None},
    {"name": "vip_flag", "type": nullable("boolean"), "default": None},
    {"name": "priority_flag", "type": nullable("boolean"), "default": None},
    {"name": "blacklist_flag", "type": nullable("boolean"), "default": None},
    {"name": "fraud_flag", "type": nullable("boolean"), "default": None},
    {"name": "relationship_manager", "type": nullable("string"), "default": None},
    {"name": "referral_code", "type": nullable("string"), "default": None},
    {"name": "referrer_name", "type": nullable("string"), "default": None},
    {"name": "opened_date", "type": nullable("string"), "default": None},
    {"name": "closed_date", "type": nullable("string"), "default": None},
    {"name": "created_by", "type": nullable("string"), "default": None},
    {"name": "created_ts", "type": nullable("string"), "default": None},
    {"name": "updated_by", "type": nullable("string"), "default": None},
    {"name": "updated_ts", "type": nullable("string"), "default": None},
    {"name": "approved_by", "type": nullable("string"), "default": None},
    {"name": "approved_ts", "type": nullable("string"), "default": None},
    {"name": "remark", "type": nullable("string"), "default": None},
    {"name": "source_system", "type": nullable("string"), "default": None},
    {"name": "external_customer_id", "type": nullable("string"), "default": None},
    {"name": "trading_permission", "type": nullable("string"), "default": None},
    {"name": "derivative_permission", "type": nullable("string"), "default": None},
    {"name": "bond_permission", "type": nullable("string"), "default": None},
    {"name": "mutual_fund_permission", "type": nullable("string"), "default": None},
    {"name": "online_trading_flag", "type": nullable("boolean"), "default": None},
    {"name": "margin_trading_flag", "type": nullable("boolean"), "default": None},
    {"name": "securities_depository_flag", "type": nullable("boolean"), "default": None},
]

for i in range(1, 41):
    CUSTOMER_COLUMNS.append(
        {"name": f"custom_attr_{i:03d}", "type": nullable("string"), "default": None}
    )


CUSTOMER_RECORD_SCHEMA: Dict[str, Any] = {
    "type": "record",
    "name": "CustomerInfo",
    "fields": CUSTOMER_COLUMNS,
}

OGG_ENVELOPE_SCHEMA: Dict[str, Any] = {
    "type": "record",
    "name": "OracleGoldenGateCustomerEnvelope",
    "namespace": "com.example.ogg",
    "fields": [
        {"name": "op_type", "type": "string"},
        {"name": "table", "type": "string"},
        {"name": "current_ts", "type": "string"},
        {"name": "op_ts", "type": "string"},
        {"name": "before", "type": nullable(CUSTOMER_RECORD_SCHEMA), "default": None},
        {"name": "after", "type": nullable("CustomerInfo"), "default": None},
        {"name": "csn", "type": nullable("string"), "default": None},
        {"name": "pos", "type": nullable("string"), "default": None},
    ],
}


# ============================================================
# 5) Data generation
# ============================================================

def build_customer_row(customer_id: int, full_name: str) -> Dict[str, Any]:
    base_text = (
        f"Customer profile for {full_name}. "
        f"Stock company customer record. "
        f"customer_id={customer_id}. "
    )

    row: Dict[str, Any] = {
        "customer_id": customer_id,
        "customer_code": f"CUST{customer_id:07d}",
        "customer_type": "INDIVIDUAL",
        "full_name": full_name,
        "short_name": full_name,
        "gender": "M" if customer_id % 2 == 0 else "F",
        "date_of_birth": "1990-05-20",
        "nationality": "VN",
        "marital_status": "SINGLE",
        "tax_code": f"TAX{customer_id:08d}",
        "customer_status": "ACTIVE",
        "risk_level": "HIGH" if customer_id % 5 == 0 else "MEDIUM",
        "segment": "RETAIL",
        "branch_code": f"BR{customer_id % 20:03d}",
        "broker_code": f"BRK{customer_id % 100:03d}",
        "account_no": f"ACC{customer_id:010d}",
        "trading_account_no": f"TRADE{customer_id:010d}",
        "custody_account_no": f"CSD{customer_id:010d}",
        "email": f"customer{customer_id}@example.com",
        "mobile_phone": f"09{customer_id:08d}"[:10],
        "home_phone": None,
        "office_phone": None,
        "fax": None,
        "address_line1": f"{customer_id} Nguyen Hue Boulevard",
        "address_line2": "Tower A, Floor 12",
        "ward": "Ben Nghe",
        "district": "District 1",
        "province": "Ho Chi Minh City",
        "country": "Vietnam",
        "postal_code": "700000",
        "contact_person": f"Contact {customer_id}",
        "occupation": "Engineer",
        "company_name": "Example Securities Client Company",
        "job_title": "Senior Specialist",
        "annual_income": decimal_to_string(Decimal("500000000")),
        "net_worth": decimal_to_string(Decimal("1500000000")),
        "source_of_funds": "SALARY",
        "investment_experience": "5_YEARS",
        "risk_appetite": "MEDIUM",
        "kyc_status": "COMPLETED",
        "kyc_date": "2025-01-10",
        "aml_flag": False,
        "pep_flag": False,
        "sanction_flag": False,
        "fatca_flag": False,
        "fatca_country": None,
        "crs_flag": False,
        "crs_country": None,
        "id_type": "CCCD",
        "id_number": f"0790{customer_id:08d}"[:12],
        "id_issue_date": "2022-07-15",
        "id_issue_place": "Cuc Canh Sat QLHC",
        "id_expiry_date": "2037-07-15",
        "passport_number": None,
        "passport_issue_date": None,
        "passport_expiry_date": None,
        "bank_account_no": f"9704{customer_id:012d}"[:16],
        "bank_name": "VCB",
        "bank_branch": "Sai Gon",
        "settlement_method": "BANK_TRANSFER",
        "cash_balance": decimal_to_string(Decimal("25000000.50")),
        "margin_balance": decimal_to_string(Decimal("5000000.00")),
        "loan_balance": decimal_to_string(Decimal("0")),
        "credit_limit": decimal_to_string(Decimal("100000000")),
        "available_limit": decimal_to_string(Decimal("95000000")),
        "portfolio_value": decimal_to_string(Decimal("230000000.75")),
        "realized_pnl": decimal_to_string(Decimal("1200000.25")),
        "unrealized_pnl": decimal_to_string(Decimal("3500000.10")),
        "last_trade_date": "2026-04-14",
        "last_login_ts": now_iso(),
        "preferred_language": "vi",
        "preferred_channel": "MOBILE_APP",
        "sms_opt_in": True,
        "email_opt_in": True,
        "mobile_app_opt_in": True,
        "marketing_opt_in": False,
        "vip_flag": customer_id % 10 == 0,
        "priority_flag": customer_id % 7 == 0,
        "blacklist_flag": False,
        "fraud_flag": False,
        "relationship_manager": f"RM{customer_id % 100:03d}",
        "referral_code": f"REF{customer_id % 1000:03d}",
        "referrer_name": f"Referrer {customer_id}",
        "opened_date": "2024-06-01",
        "closed_date": None,
        "created_by": "SYSTEM",
        "created_ts": now_iso(),
        "updated_by": "SYSTEM",
        "updated_ts": now_iso(),
        "approved_by": "SUPERVISOR01",
        "approved_ts": now_iso(),
        "remark": base_text * 3,
        "source_system": "CORE_CRM",
        "external_customer_id": f"EXT{customer_id:08d}",
        "trading_permission": "Y",
        "derivative_permission": "N",
        "bond_permission": "Y",
        "mutual_fund_permission": "Y",
        "online_trading_flag": True,
        "margin_trading_flag": True,
        "securities_depository_flag": True,
    }

    for i in range(1, 41):
        row[f"custom_attr_{i:03d}"] = (
            f"custom-value-{customer_id}-{i} | {base_text} attribute-sequence={i}"
        )

    return row


def build_insert_event(customer_id: int, full_name: str) -> Dict[str, Any]:
    return {
        "op_type": "I",
        "table": TABLE_NAME,
        "current_ts": now_iso(),
        "op_ts": now_iso(),
        "before": None,
        "after": build_customer_row(customer_id, full_name),
        "csn": str(100000000 + customer_id),
        "pos": str(20000000000000000000 + customer_id),
    }


def build_update_event(customer_id: int, old_name: str, new_name: str) -> Dict[str, Any]:
    before = build_customer_row(customer_id, old_name)
    after = build_customer_row(customer_id, new_name)
    after["email"] = f"updated_{customer_id}@example.com"
    after["risk_level"] = "HIGH"
    after["updated_by"] = "BATCH_JOB"
    after["updated_ts"] = now_iso()
    return {
        "op_type": "U",
        "table": TABLE_NAME,
        "current_ts": now_iso(),
        "op_ts": now_iso(),
        "before": before,
        "after": after,
        "csn": str(100000000 + customer_id + 1000),
        "pos": str(20000000000000000000 + customer_id + 1000),
    }


def build_delete_event(customer_id: int, full_name: str) -> Dict[str, Any]:
    return {
        "op_type": "D",
        "table": TABLE_NAME,
        "current_ts": now_iso(),
        "op_ts": now_iso(),
        "before": build_customer_row(customer_id, full_name),
        "after": None,
        "csn": str(100000000 + customer_id + 2000),
        "pos": str(20000000000000000000 + customer_id + 2000),
    }


def build_event(customer_id: int) -> Dict[str, Any]:
    mod = customer_id % 3
    if mod == 1:
        return build_insert_event(customer_id, f"Customer {customer_id}")
    if mod == 2:
        return build_update_event(
            customer_id,
            f"Customer {customer_id} Old",
            f"Customer {customer_id} New",
        )
    return build_delete_event(customer_id, f"Customer {customer_id}")


# ============================================================
# 6) File generation
# ============================================================

def get_file_size(file_path: Path) -> int:
    return file_path.stat().st_size if file_path.exists() else 0


def write_avro_file(output_file: Path, schema: Dict[str, Any], records: List[Dict[str, Any]]) -> None:
    output_file.parent.mkdir(parents=True, exist_ok=True)
    parsed_schema = parse_schema(schema)

    with output_file.open("wb") as f:
        writer(f, parsed_schema, records, codec="deflate", sync_interval=16000)


def generate_avro_file_until_target_size(
    output_file: Path,
    target_size_bytes: int,
    logger: logging.Logger,
    batch_size: int = 1000,
) -> None:
    records: List[Dict[str, Any]] = []
    next_customer_id = 1

    while True:
        for _ in range(batch_size):
            records.append(build_event(next_customer_id))
            next_customer_id += 1

        write_avro_file(output_file, OGG_ENVELOPE_SCHEMA, records)

        current_size = get_file_size(output_file)
        print(
            f"Current file size: {current_size / (1024 * 1024):.2f} MB | "
            f"records generated: {len(records)}"
        )

        if current_size >= target_size_bytes:
            break

    final_size = get_file_size(output_file)

    logger.info(
        "Avro generation completed | file=%s | size_bytes=%d | size_mb=%.2f | records=%d",
        str(output_file.resolve()),
        final_size,
        final_size / (1024 * 1024),
        len(records),
    )

    print("\nGeneration completed successfully.")
    print(f"Output file   : {output_file.resolve()}")
    print(f"Final size    : {final_size / (1024 * 1024):.2f} MB")
    print(f"Total records : {len(records)}")
    print(f"Log file      : {LOG_FILE.resolve()}")


def main() -> None:
    logger = setup_logger(LOG_FILE)
    generate_avro_file_until_target_size(
        output_file=OUTPUT_FILE,
        target_size_bytes=TARGET_FILE_SIZE_BYTES,
        logger=logger,
        batch_size=1000,
    )


if __name__ == "__main__":
    main()
