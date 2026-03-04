corro este codigo:

import os
import csv
import json

# ==============================
# HARD CODED FILE NAMES
# ==============================

CSV_FILE = "sqlserver_3.csv"
JSONL_FILE = "" \
"bigquery_3.json"

# ==============================
# BUILD PATHS
# ==============================

BASE_PATH = os.path.dirname(os.path.abspath(__file__))

csv_path = os.path.join(BASE_PATH, CSV_FILE)
jsonl_path = os.path.join(BASE_PATH, JSONL_FILE)

# ==============================
# READ CSV OrderKey
# ==============================

csv_order_keys = set()

with open(csv_path, "r", encoding="utf-8") as f:
    reader = csv.DictReader(f, delimiter=";")
    for row in reader:
        csv_order_keys.add(str(row["OrderKey"]))

print(f"CSV OrderKeys: {len(csv_order_keys)}")

# ==============================
# READ JSONL order_key
# ==============================

json_order_keys = set()

with open(jsonl_path, "r", encoding="utf-8") as f:
    data = json.load(f)

    for row in data:
        if "order_key" in row and row["order_key"] is not None:
            json_order_keys.add(str(row["order_key"]))

print(f"JSON order_keys: {len(json_order_keys)}")

# ==============================
# INTERSECTION
# ==============================

matches = csv_order_keys.intersection(json_order_keys)

print("===================================")
print(f"OrderKey in CSV: {len(csv_order_keys)}")
print(f"order_key in JSON: {len(json_order_keys)}")
print(f"Matches: {len(matches)}")
print("===================================")

Y tengo este resultado

CSV OrderKeys: 128470
JSON order_keys: 12655
===================================
OrderKey in CSV: 128470
order_key in JSON: 12655
Matches: 1209
===================================

Quiero modificar este codigo:
    #!/usr/bin/env python3
"""
Compare SQL Server CSV vs BigQuery JSON/JSONL export for Orders, ignoring date/time columns.

- SQL Server file: semicolon-separated CSV (as in your sample).
- BigQuery file: JSONL (one JSON object per line) OR a JSON array.
- Source of truth: SQL Server CSV.
- Compares values after robust normalization.
- Fixes your current bug: DO NOT coerce "0"/"1" to bool globally.
  Only coerce to bool for specific BOOL_COLS.
- Writes:
    - comparison_summary.txt
    - mismatch_samples.csv  (up to MAX_MISMATCH_SAMPLES)

Change integrated:
  - DISTINCT mismatch printing now includes a sample of order_key values per distinct mismatch
  - Robust to unhashable values (dict/list/etc.) by keyifying via JSON stringification
"""

from __future__ import annotations

import csv
import json
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


# =========================
# Config (edit if needed)
# =========================
BASE_DIR = Path(__file__).resolve().parent

SQLSERVER_CSV = BASE_DIR / "sqlserver.csv"       # <-- put your SQL Server export here
BIGQUERY_JSON = BASE_DIR / "bigquery_3.json"     # <-- can be JSONL or JSON array

SQL_KEY = "OrderKey"
BQ_KEY = "order_key"

MAX_MISMATCH_SAMPLES = 80

# Columns to ignore entirely (dates/timestamps). We'll auto-detect too, but keep explicit override.
IGNORE_COLS_SQL = {
    "OrderDate",
    "ScheduledPickupDate",
    "ScheduledDeliveryDate",
    "PickupDate",
    "PickupLate",
    "DeliveredDate",
    "CarrierAssignDate",
    "VoidDate",
    "CarrierPortalPostingDate",
    "DeliveryArrivalDate",
    "PickupDepartureDate",
    "PickupServiceFailureReasonCategoryID",
    "DeliveryServiceFailureReasonCategoryID",
}
IGNORE_COLS_BQ = {
    "order_date",
    "scheduled_pickup_date",
    "scheduled_delivery_date",
    "pickup_date",
    "pickup_departure_date",
    "pickup_late",
    "delivered_date",
    "delivery_arrival_date",
    "carrier_assign_date",
    "void_date",
    "carrier_portal_posting_date",
    "origin_scheduled_departure_time",
    "destination_stop_scheduled_departure_time",
    "pickup_service_failure_reason_category_id",
    "delivery_service_failure_reason_category_id",
}

# Real boolean columns ONLY (critical fix). Add/remove based on your schema.
BOOL_COLS_BQ = {
    "is_hazmat",
    "is_drop_and_hook",
}
# SQL side often stores these as 0/1 or Y/N. We still treat them as boolean.
BOOL_COLS_SQL = {
    "IsHazmat",
    "IsDropAndHook",  # if exists in SQL CSV; if not, ignore
}

# Mapping SQL -> BQ (complete-ish for your Orders table)
SQL_TO_BQ: Dict[str, str] = {
    "SourceSystemName": "source_system_name",
    "OrderSourceCode": "order_source_code",
    "BusinessUnitCode": "business_unit_code",
    "ServiceCode": "service_type_code",
    "OrderStatusTypeCode": "order_status_type_code",
    "BrokerageStatusTypeCode": "brokerage_status_code",
    "PickupRescheduledReason": "pickup_rescheduled_reason",
    "DropRescheduledReason": "drop_rescheduled_reason",
    "IsOnHold": "is_on_hold",
    "IsUncommited": "is_uncommited",
    "OrderFinancialStatusTypeCode": "order_financial_status_type_code",

    "OrderDate": "order_date",
    "ScheduledPickupDate": "scheduled_pickup_date",
    "ScheduledDeliveryDate": "scheduled_delivery_date",
    "PickupDate": "pickup_date",
    "PickupDepartureDate": "pickup_departure_date",
    "DeliveredDate": "delivered_date",
    "DeliveryArrivalDate": "delivery_arrival_date",
    "CarrierAssignDate": "carrier_assign_date",
    "VoidDate": "void_date",

    "FreightOptimizerCustomerCode": "freight_optimizer_customer_code",
    "CarrierCode": "carrier_code",
    "EquipmentCode": "equipment_code",

    "OrderKey": "order_key",
    "OrderNumber": "order_number",

    "OrderSalesAmount": "order_sales_amount",
    "LineHaulChargeAmount": "line_haul_charge_amount",
    "FuelChargeAmount": "fuel_charge_amount",
    "TotalAccessorialAmount": "total_accessorial_charge",
    "DetentionAmount": "detention_charge",
    "TONUAmount": "tonu_charge",

    # Pay amounts
    "LineHaulCostAmount": "line_haul_pay_amount",
    "FuelCostAmount": "fuel_pay_amount",
    "OrderCost": "order_cost",

    "LHRate": "lh_rate",
    "TripCount": "trip_count",
    "BillDistance": "bill_distance",
    "WeightInPounds": "weight_in_pounds",

    "OnTimeDelivery": "on_time_delivery",
    "DeliveryDelay": "delivery_delay",
    "OnTimePickup": "on_time_pickup",
    "PickupDelay": "pickup_delay",

    "OriginCity": "origin_city",
    "OriginStateCode": "origin_state_code",
    "OriginPostalCode": "origin_postal_code",
    "OriginCountryCode": "origin_country_code",
    "OriginLocationID": "origin_location_id",

    "DestinationCity": "destination_city",
    "DestinationStateCode": "destination_state_code",
    "DestinationPostalCode": "destination_postal_code",
    "DestinationCountryCode": "destination_country_code",
    "DestinationLocationID": "destination_location_id",

    "EnteredByUserName": "entered_by_user_name",
    "PrimarySalesRepUserName": "primary_sales_rep_user_name",
    "SecondarySalesRepUserName": "secondary_sales_rep_user_name",
    "BookedByUserName": "booked_by_user_name",
    "PrimaryCarrierRepUserName": "primary_carrier_rep_user_name",
    "FreightAssignRepUserName": "freight_assign_rep_user_name",
    "PrimaryCarrierRepLocationCode": "dispatch_office_location_code",

    "CreatedByUser": "created_by_user",
    "IsHazmat": "is_hazmat",
    "Quantity": "quantity",
    "Volume": "volume",

    "CarrierPortalPostingDate": "carrier_portal_posting_date",
    "IsDropAndHook": "is_drop_and_hook",

    "PickupServiceFailureCodeID": "pickup_service_failure_code_id",
    "DeliveryServiceFailureCodeID": "delivery_service_failure_code_id",
    "PercentageOfCommissionPrimary": "percentage_of_commission_primary",

    "CompanyID": "company_id",
    "ValueOfGoods": "value_of_goods",
    "BazookaOrderId": "bazooka_order_id",
    "ShipmentID": "shipment_id",

    "PickupServiceFailureReasonCategoryID": "pickup_service_failure_reason_category_id",
    "DeliveryServiceFailureReasonCategoryID": "delivery_service_failure_reason_category_id",
}


# =========================
# Helpers
# =========================

_THOUSANDS_DOT = re.compile(r"^\d{1,3}(\.\d{3})+$")
_NUMERIC = re.compile(r"^-?\d+(\.\d+)?$")

# Basic ISO-ish datetime detection (only for auto-ignore heuristics)
_ISO_DATEISH = re.compile(r"^\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?)?$")
_SQL_DATEISH = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?$")


def die(msg: str, code: int = 2) -> None:
    print(msg, file=sys.stderr)
    raise SystemExit(code)


def resolve_path(p: Path) -> Path:
    if p.is_absolute():
        return p
    return (BASE_DIR / p).resolve()


def load_sql_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        die(f"[SQL] CSV not found: {path}")

    encodings = ["utf-8", "utf-8-sig", "cp1252", "latin1"]
    last_err: Optional[Exception] = None
    for enc in encodings:
        try:
            df = pd.read_csv(
                path,
                sep=";",
                dtype=str,
                encoding=enc,
                keep_default_na=False,
                na_values=[],
            )
            return df
        except Exception as e:
            last_err = e
            continue
    die(f"[SQL] Failed to read CSV with encodings {encodings}. Last error: {last_err}")


def load_bq_json(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        die(f"[BQ] JSON/JSONL not found: {path}")

    text = path.read_text(encoding="utf-8", errors="replace").strip()
    if not text:
        return []

    # Detect JSON array vs JSONL
    if text[0] == "[":
        try:
            data = json.loads(text)
            if not isinstance(data, list):
                die("[BQ] JSON file starts with '[' but isn't a list.")
            return [x for x in data if isinstance(x, dict)]
        except json.JSONDecodeError as e:
            die(f"[BQ] Failed to parse JSON array: {e}")
    else:
        rows: List[Dict[str, Any]] = []
        with path.open("r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    if isinstance(obj, dict):
                        rows.append(obj)
                except json.JSONDecodeError:
                    continue
        return rows


def to_none_if_nullish(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        if s == "" or s.lower() in {"null", "none", "nan"}:
            return None
        return s
    return v


def normalize_bool(v: Any) -> Optional[bool]:
    v = to_none_if_nullish(v)
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)) and v in (0, 1, 0.0, 1.0):
        return bool(int(v))
    if isinstance(v, str):
        s = v.strip().lower()
        if s in {"y", "yes", "true", "t", "1"}:
            return True
        if s in {"n", "no", "false", "f", "0"}:
            return False
    return None


def normalize_numberish_str(s: str) -> Any:
    s = s.strip()
    if s == "":
        return None

    # "1.000.000.000" -> 1000000000
    if _THOUSANDS_DOT.match(s):
        try:
            return int(s.replace(".", ""))
        except ValueError:
            pass

    # numeric like "3011.00" or "0"
    if _NUMERIC.match(s):
        f = float(s)
        if abs(f - round(f)) < 1e-9:
            return int(round(f))
        return f

    return s.strip().lower()


def normalize_scalar(col: str, v: Any, *, is_bool_col: bool) -> Any:
    """
    CRITICAL FIX:
      - Only convert to bool for boolean columns.
      - Outside boolean columns: "0" stays 0 (numeric), not False.
    """
    v = to_none_if_nullish(v)
    if v is None:
        return None

    if is_bool_col:
        b = normalize_bool(v)
        if b is not None:
            return b
        return str(v).strip().lower()

    # Non-boolean columns
    if isinstance(v, bool):
        return 1 if v else 0

    if isinstance(v, (int, float)):
        if isinstance(v, float) and abs(v - round(v)) < 1e-9:
            return int(round(v))
        return v

    if isinstance(v, str):
        return normalize_numberish_str(v)

    return str(v).strip().lower()


def looks_like_datetime_value(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, str):
        s = v.strip()
        return bool(_ISO_DATEISH.match(s) or _SQL_DATEISH.match(s))
    return False


def auto_detect_date_cols_sql(df: pd.DataFrame) -> set[str]:
    date_cols = set()
    for col in df.columns:
        if col in IGNORE_COLS_SQL:
            continue
        series = df[col].astype(str)
        non_empty = series[series.str.strip().ne("") & series.str.lower().ne("null")]
        if len(non_empty) == 0:
            continue
        sample = non_empty.head(50)
        if sample.map(lambda x: looks_like_datetime_value(x)).mean() > 0.8:
            date_cols.add(col)
    return date_cols


def auto_detect_date_cols_bq(rows: List[Dict[str, Any]]) -> set[str]:
    if not rows:
        return set()
    counts: Dict[str, List[bool]] = defaultdict(list)
    for r in rows[:200]:
        for k, v in r.items():
            if v is None:
                continue
            if isinstance(v, str) and v.strip() == "":
                continue
            counts[k].append(looks_like_datetime_value(v))
    out = set()
    for k, flags in counts.items():
        if flags and (sum(flags) / len(flags)) > 0.8:
            out.add(k)
    return out


def keyify(v: Any) -> str:
    """
    Convert any value into a stable, hashable string representation for grouping DISTINCT mismatches.
    This prevents errors when values are dict/list/etc.
    """
    if v is None:
        return "NULL"
    try:
        return json.dumps(v, sort_keys=True, default=str)
    except Exception:
        return str(v)


@dataclass
class CompareResult:
    total_sql_rows: int
    total_bq_rows: int
    common_keys: int
    missing_in_bq: int
    missing_in_sql: int
    matched_rows: int
    mismatched_rows: int
    mismatch_field_counts: Counter
    mismatch_samples: List[Tuple[str, str, Any, Any]]  # (key, field, sql_val, bq_val)


def compare(sql_df: pd.DataFrame, bq_rows: List[Dict[str, Any]]) -> CompareResult:
    # Build BQ index by key
    bq_by_key: Dict[str, Dict[str, Any]] = {}
    for r in bq_rows:
        k = r.get(BQ_KEY)
        if k is None:
            continue
        k_str = str(k).strip()
        if k_str:
            bq_by_key[k_str] = r

    # Normalize SQL keys
    if SQL_KEY not in sql_df.columns:
        die(f"[SQL] Key column not found: {SQL_KEY}. Columns: {list(sql_df.columns)}")

    sql_df[SQL_KEY] = sql_df[SQL_KEY].astype(str).str.strip()
    sql_by_key = {k: row for k, row in sql_df.set_index(SQL_KEY).iterrows() if str(k).strip()}

    # Auto detect date cols
    auto_sql_dates = auto_detect_date_cols_sql(sql_df)
    auto_bq_dates = auto_detect_date_cols_bq(bq_rows)

    ignore_sql = set(IGNORE_COLS_SQL) | auto_sql_dates
    ignore_bq = set(IGNORE_COLS_BQ) | auto_bq_dates

    sql_keys = set(sql_by_key.keys())
    bq_keys = set(bq_by_key.keys())

    common = sql_keys & bq_keys
    missing_in_bq = sql_keys - bq_keys
    missing_in_sql = bq_keys - sql_keys

    mismatch_field_counts: Counter = Counter()
    mismatch_samples: List[Tuple[str, str, Any, Any]] = []

    matched_rows = 0
    mismatched_rows = 0

    # Compare using SQL as source of truth
    for k in sorted(common):
        sql_row = sql_by_key[k]
        bq_row = bq_by_key[k]

        row_mismatch = False

        for sql_col, bq_col in SQL_TO_BQ.items():
            if sql_col == SQL_KEY:
                continue
            if sql_col in ignore_sql or bq_col in ignore_bq:
                continue
            if sql_col not in sql_row.index:
                continue

            sql_raw = sql_row[sql_col]
            bq_raw = bq_row.get(bq_col)

            is_bool = (sql_col in BOOL_COLS_SQL) or (bq_col in BOOL_COLS_BQ)

            sql_norm = normalize_scalar(sql_col, sql_raw, is_bool_col=is_bool)
            bq_norm = normalize_scalar(bq_col, bq_raw, is_bool_col=is_bool)

            if sql_norm != bq_norm:
                row_mismatch = True
                mismatch_field_counts[bq_col] += 1
                if len(mismatch_samples) < MAX_MISMATCH_SAMPLES:
                    mismatch_samples.append((k, bq_col, sql_norm, bq_norm))

        if row_mismatch:
            mismatched_rows += 1
        else:
            matched_rows += 1

    return CompareResult(
        total_sql_rows=len(sql_df),
        total_bq_rows=len(bq_rows),
        common_keys=len(common),
        missing_in_bq=len(missing_in_bq),
        missing_in_sql=len(missing_in_sql),
        matched_rows=matched_rows,
        mismatched_rows=mismatched_rows,
        mismatch_field_counts=mismatch_field_counts,
        mismatch_samples=mismatch_samples,
    )


def write_outputs(res: CompareResult) -> None:
    summary_path = BASE_DIR / "comparison_summary.txt"
    mismatch_path = BASE_DIR / "mismatch_samples.csv"

    lines = []
    lines.append(f"SQL rows: {res.total_sql_rows}")
    lines.append(f"BQ rows:  {res.total_bq_rows}")
    lines.append(f"Common keys: {res.common_keys}")
    lines.append(f"Missing in BQ:  {res.missing_in_bq}")
    lines.append(f"Missing in SQL: {res.missing_in_sql}")
    lines.append(f"Matched rows:   {res.matched_rows}")
    lines.append(f"Mismatched rows:{res.mismatched_rows}")
    lines.append("")
    lines.append("Top mismatched fields:")
    for field, cnt in res.mismatch_field_counts.most_common(30):
        lines.append(f"  {field}: {cnt}")
    summary_path.write_text("\n".join(lines), encoding="utf-8")

    with mismatch_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["order_key", "field", "sql_value_norm", "bq_value_norm"])
        for k, field, a, b in res.mismatch_samples:
            w.writerow([k, field, a, b])

    print(f"[OK] Wrote {summary_path.name}")
    print(f"[OK] Wrote {mismatch_path.name} (up to {MAX_MISMATCH_SAMPLES} samples)")
    print("")
    print("=== QUICK VIEW ===")
    print("\n".join(lines[:12]))
    print("")
    print("=== MISMATCH SAMPLE (first 20) ===")
    for row in res.mismatch_samples[:20]:
        print(row)

    # ---- UPDATED DISTINCT SECTION ----
    print("=== DISTINCT MISMATCH VALUES (with sample order_key) ===")

    # combo_key -> list of sample order_keys
    combo_keys: Dict[Tuple[str, str, str], List[str]] = defaultdict(list)

    for key, field, sql_val, bq_val in res.mismatch_samples:
        combo_key = (field, keyify(sql_val), keyify(bq_val))
        keys = combo_keys[combo_key]
        if len(keys) < 5:  # sample size
            keys.append(str(key))

    # Print in stable order (as first observed in mismatch_samples)
    seen = set()
    for key, field, sql_val, bq_val in res.mismatch_samples:
        combo_key = (field, keyify(sql_val), keyify(bq_val))
        if combo_key in seen:
            continue
        seen.add(combo_key)
        print(
            f"{field}: {keyify(sql_val)}  !=  {keyify(bq_val)} | "
            f"sample order_key: {combo_keys[combo_key]}"
        )


def main() -> None:
    sql_path = resolve_path(SQLSERVER_CSV)
    bq_path = resolve_path(BIGQUERY_JSON)

    sql_df = load_sql_csv(sql_path)
    bq_rows = load_bq_json(bq_path)

    if sql_df.empty:
        die("[SQL] CSV loaded but is empty.")
    if not bq_rows:
        die("[BQ] JSON/JSONL loaded but has 0 objects.")

    if SQL_KEY not in sql_df.columns:
        die(f"[SQL] Missing key column '{SQL_KEY}' in CSV.")

    res = compare(sql_df, bq_rows)
    write_outputs(res)


if __name__ == "__main__":
    main()

Para hacer el analisis solo sobre las lineas que hacen match en el order_key. O sea todo el porcentaje y todo se hace sobre esos matches.
Entiendes?

Sí. Ahora mismo tu script hace métricas “globales” (incluye faltantes). Tú quieres que todo (matched/mismatched %, top fields, samples, etc.) se calcule solo sobre el universo de keys que existen en ambos lados (common_keys).

En la práctica: el “denominador” pasa a ser common_keys y no total_sql_rows ni total_bq_rows. Los “missing” los puedes seguir reportando, pero no entran en el % ni en los totales de QA.

Abajo te dejo el script ya modificado con ese criterio (hardcoded, mismo flujo). Cambios clave:
	•	write_outputs() calcula:
	•	coverage_common = common_keys / total_sql_rows (solo info)
	•	match_rate_on_common = matched_rows / common_keys
	•	mismatch_rate_on_common = mismatched_rows / common_keys
	•	NO mezcla faltantes en el “porcentaje”.
	•	El resto (field counts, distinct mismatches) ya estaba basado en common, así que queda consistente.

#!/usr/bin/env python3
"""
Compare SQL Server CSV vs BigQuery JSON/JSONL export for Orders, ignoring date/time columns.

ANALYSIS SCOPE (your request):
- ALL QA rates and totals are computed ONLY over keys that exist on BOTH sides (common_keys).
- Missing keys are still reported, but excluded from match/mismatch percentages.

Other features unchanged:
- Robust normalization
- Only coerce booleans for BOOL_COLS_*
- Writes comparison_summary.txt and mismatch_samples.csv
"""

from __future__ import annotations

import csv
import json
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


# =========================
# Config (edit if needed)
# =========================
BASE_DIR = Path(__file__).resolve().parent

SQLSERVER_CSV = BASE_DIR / "sqlserver.csv"
BIGQUERY_JSON = BASE_DIR / "bigquery_3.json"

SQL_KEY = "OrderKey"
BQ_KEY = "order_key"

MAX_MISMATCH_SAMPLES = 80

IGNORE_COLS_SQL = {
    "OrderDate",
    "ScheduledPickupDate",
    "ScheduledDeliveryDate",
    "PickupDate",
    "PickupLate",
    "DeliveredDate",
    "CarrierAssignDate",
    "VoidDate",
    "CarrierPortalPostingDate",
    "DeliveryArrivalDate",
    "PickupDepartureDate",
    "PickupServiceFailureReasonCategoryID",
    "DeliveryServiceFailureReasonCategoryID",
}
IGNORE_COLS_BQ = {
    "order_date",
    "scheduled_pickup_date",
    "scheduled_delivery_date",
    "pickup_date",
    "pickup_departure_date",
    "pickup_late",
    "delivered_date",
    "delivery_arrival_date",
    "carrier_assign_date",
    "void_date",
    "carrier_portal_posting_date",
    "origin_scheduled_departure_time",
    "destination_stop_scheduled_departure_time",
    "pickup_service_failure_reason_category_id",
    "delivery_service_failure_reason_category_id",
}

BOOL_COLS_BQ = {
    "is_hazmat",
    "is_drop_and_hook",
}
BOOL_COLS_SQL = {
    "IsHazmat",
    "IsDropAndHook",
}

SQL_TO_BQ: Dict[str, str] = {
    "SourceSystemName": "source_system_name",
    "OrderSourceCode": "order_source_code",
    "BusinessUnitCode": "business_unit_code",
    "ServiceCode": "service_type_code",
    "OrderStatusTypeCode": "order_status_type_code",
    "BrokerageStatusTypeCode": "brokerage_status_code",
    "PickupRescheduledReason": "pickup_rescheduled_reason",
    "DropRescheduledReason": "drop_rescheduled_reason",
    "IsOnHold": "is_on_hold",
    "IsUncommited": "is_uncommited",
    "OrderFinancialStatusTypeCode": "order_financial_status_type_code",

    "OrderDate": "order_date",
    "ScheduledPickupDate": "scheduled_pickup_date",
    "ScheduledDeliveryDate": "scheduled_delivery_date",
    "PickupDate": "pickup_date",
    "PickupDepartureDate": "pickup_departure_date",
    "DeliveredDate": "delivered_date",
    "DeliveryArrivalDate": "delivery_arrival_date",
    "CarrierAssignDate": "carrier_assign_date",
    "VoidDate": "void_date",

    "FreightOptimizerCustomerCode": "freight_optimizer_customer_code",
    "CarrierCode": "carrier_code",
    "EquipmentCode": "equipment_code",

    "OrderKey": "order_key",
    "OrderNumber": "order_number",

    "OrderSalesAmount": "order_sales_amount",
    "LineHaulChargeAmount": "line_haul_charge_amount",
    "FuelChargeAmount": "fuel_charge_amount",
    "TotalAccessorialAmount": "total_accessorial_charge",
    "DetentionAmount": "detention_charge",
    "TONUAmount": "tonu_charge",

    "LineHaulCostAmount": "line_haul_pay_amount",
    "FuelCostAmount": "fuel_pay_amount",
    "OrderCost": "order_cost",

    "LHRate": "lh_rate",
    "TripCount": "trip_count",
    "BillDistance": "bill_distance",
    "WeightInPounds": "weight_in_pounds",

    "OnTimeDelivery": "on_time_delivery",
    "DeliveryDelay": "delivery_delay",
    "OnTimePickup": "on_time_pickup",
    "PickupDelay": "pickup_delay",

    "OriginCity": "origin_city",
    "OriginStateCode": "origin_state_code",
    "OriginPostalCode": "origin_postal_code",
    "OriginCountryCode": "origin_country_code",
    "OriginLocationID": "origin_location_id",

    "DestinationCity": "destination_city",
    "DestinationStateCode": "destination_state_code",
    "DestinationPostalCode": "destination_postal_code",
    "DestinationCountryCode": "destination_country_code",
    "DestinationLocationID": "destination_location_id",

    "EnteredByUserName": "entered_by_user_name",
    "PrimarySalesRepUserName": "primary_sales_rep_user_name",
    "SecondarySalesRepUserName": "secondary_sales_rep_user_name",
    "BookedByUserName": "booked_by_user_name",
    "PrimaryCarrierRepUserName": "primary_carrier_rep_user_name",
    "FreightAssignRepUserName": "freight_assign_rep_user_name",
    "PrimaryCarrierRepLocationCode": "dispatch_office_location_code",

    "CreatedByUser": "created_by_user",
    "IsHazmat": "is_hazmat",
    "Quantity": "quantity",
    "Volume": "volume",

    "CarrierPortalPostingDate": "carrier_portal_posting_date",
    "IsDropAndHook": "is_drop_and_hook",

    "PickupServiceFailureCodeID": "pickup_service_failure_code_id",
    "DeliveryServiceFailureCodeID": "delivery_service_failure_code_id",
    "PercentageOfCommissionPrimary": "percentage_of_commission_primary",

    "CompanyID": "company_id",
    "ValueOfGoods": "value_of_goods",
    "BazookaOrderId": "bazooka_order_id",
    "ShipmentID": "shipment_id",

    "PickupServiceFailureReasonCategoryID": "pickup_service_failure_reason_category_id",
    "DeliveryServiceFailureReasonCategoryID": "delivery_service_failure_reason_category_id",
}


# =========================
# Helpers
# =========================

_THOUSANDS_DOT = re.compile(r"^\d{1,3}(\.\d{3})+$")
_NUMERIC = re.compile(r"^-?\d+(\.\d+)?$")

_ISO_DATEISH = re.compile(r"^\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?)?$")
_SQL_DATEISH = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d+)?$")


def die(msg: str, code: int = 2) -> None:
    print(msg, file=sys.stderr)
    raise SystemExit(code)


def resolve_path(p: Path) -> Path:
    if p.is_absolute():
        return p
    return (BASE_DIR / p).resolve()


def load_sql_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        die(f"[SQL] CSV not found: {path}")

    encodings = ["utf-8", "utf-8-sig", "cp1252", "latin1"]
    last_err: Optional[Exception] = None
    for enc in encodings:
        try:
            df = pd.read_csv(
                path,
                sep=";",
                dtype=str,
                encoding=enc,
                keep_default_na=False,
                na_values=[],
            )
            return df
        except Exception as e:
            last_err = e
    die(f"[SQL] Failed to read CSV with encodings {encodings}. Last error: {last_err}")


def load_bq_json(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        die(f"[BQ] JSON/JSONL not found: {path}")

    text = path.read_text(encoding="utf-8", errors="replace").strip()
    if not text:
        return []

    # JSON array
    if text[0] == "[":
        try:
            data = json.loads(text)
            if not isinstance(data, list):
                die("[BQ] JSON file starts with '[' but isn't a list.")
            return [x for x in data if isinstance(x, dict)]
        except json.JSONDecodeError as e:
            die(f"[BQ] Failed to parse JSON array: {e}")

    # JSONL
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    rows.append(obj)
            except json.JSONDecodeError:
                continue
    return rows


def to_none_if_nullish(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        if s == "" or s.lower() in {"null", "none", "nan"}:
            return None
        return s
    return v


def normalize_bool(v: Any) -> Optional[bool]:
    v = to_none_if_nullish(v)
    if v is None:
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)) and v in (0, 1, 0.0, 1.0):
        return bool(int(v))
    if isinstance(v, str):
        s = v.strip().lower()
        if s in {"y", "yes", "true", "t", "1"}:
            return True
        if s in {"n", "no", "false", "f", "0"}:
            return False
    return None


def normalize_numberish_str(s: str) -> Any:
    s = s.strip()
    if s == "":
        return None

    if _THOUSANDS_DOT.match(s):
        try:
            return int(s.replace(".", ""))
        except ValueError:
            pass

    if _NUMERIC.match(s):
        f = float(s)
        if abs(f - round(f)) < 1e-9:
            return int(round(f))
        return f

    return s.strip().lower()


def normalize_scalar(col: str, v: Any, *, is_bool_col: bool) -> Any:
    v = to_none_if_nullish(v)
    if v is None:
        return None

    if is_bool_col:
        b = normalize_bool(v)
        if b is not None:
            return b
        return str(v).strip().lower()

    if isinstance(v, bool):
        return 1 if v else 0

    if isinstance(v, (int, float)):
        if isinstance(v, float) and abs(v - round(v)) < 1e-9:
            return int(round(v))
        return v

    if isinstance(v, str):
        return normalize_numberish_str(v)

    return str(v).strip().lower()


def looks_like_datetime_value(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, str):
        s = v.strip()
        return bool(_ISO_DATEISH.match(s) or _SQL_DATEISH.match(s))
    return False


def auto_detect_date_cols_sql(df: pd.DataFrame) -> set[str]:
    date_cols = set()
    for col in df.columns:
        if col in IGNORE_COLS_SQL:
            continue
        series = df[col].astype(str)
        non_empty = series[series.str.strip().ne("") & series.str.lower().ne("null")]
        if len(non_empty) == 0:
            continue
        sample = non_empty.head(50)
        if sample.map(lambda x: looks_like_datetime_value(x)).mean() > 0.8:
            date_cols.add(col)
    return date_cols


def auto_detect_date_cols_bq(rows: List[Dict[str, Any]]) -> set[str]:
    if not rows:
        return set()
    counts: Dict[str, List[bool]] = defaultdict(list)
    for r in rows[:200]:
        for k, v in r.items():
            if v is None:
                continue
            if isinstance(v, str) and v.strip() == "":
                continue
            counts[k].append(looks_like_datetime_value(v))
    out = set()
    for k, flags in counts.items():
        if flags and (sum(flags) / len(flags)) > 0.8:
            out.add(k)
    return out


def keyify(v: Any) -> str:
    if v is None:
        return "NULL"
    try:
        return json.dumps(v, sort_keys=True, default=str)
    except Exception:
        return str(v)


@dataclass
class CompareResult:
    total_sql_rows: int
    total_bq_rows: int
    common_keys: int
    missing_in_bq: int
    missing_in_sql: int
    matched_rows: int
    mismatched_rows: int
    mismatch_field_counts: Counter
    mismatch_samples: List[Tuple[str, str, Any, Any]]


def compare(sql_df: pd.DataFrame, bq_rows: List[Dict[str, Any]]) -> CompareResult:
    bq_by_key: Dict[str, Dict[str, Any]] = {}
    for r in bq_rows:
        k = r.get(BQ_KEY)
        if k is None:
            continue
        k_str = str(k).strip()
        if k_str:
            bq_by_key[k_str] = r

    if SQL_KEY not in sql_df.columns:
        die(f"[SQL] Key column not found: {SQL_KEY}. Columns: {list(sql_df.columns)}")

    sql_df[SQL_KEY] = sql_df[SQL_KEY].astype(str).str.strip()
    sql_by_key = {k: row for k, row in sql_df.set_index(SQL_KEY).iterrows() if str(k).strip()}

    auto_sql_dates = auto_detect_date_cols_sql(sql_df)
    auto_bq_dates = auto_detect_date_cols_bq(bq_rows)

    ignore_sql = set(IGNORE_COLS_SQL) | auto_sql_dates
    ignore_bq = set(IGNORE_COLS_BQ) | auto_bq_dates

    sql_keys = set(sql_by_key.keys())
    bq_keys = set(bq_by_key.keys())

    common = sql_keys & bq_keys
    missing_in_bq = sql_keys - bq_keys
    missing_in_sql = bq_keys - sql_keys

    mismatch_field_counts: Counter = Counter()
    mismatch_samples: List[Tuple[str, str, Any, Any]] = []

    matched_rows = 0
    mismatched_rows = 0

    # IMPORTANT: comparison loops ONLY on common (this is already your desired scope)
    for k in sorted(common):
        sql_row = sql_by_key[k]
        bq_row = bq_by_key[k]

        row_mismatch = False

        for sql_col, bq_col in SQL_TO_BQ.items():
            if sql_col == SQL_KEY:
                continue
            if sql_col in ignore_sql or bq_col in ignore_bq:
                continue
            if sql_col not in sql_row.index:
                continue

            sql_raw = sql_row[sql_col]
            bq_raw = bq_row.get(bq_col)

            is_bool = (sql_col in BOOL_COLS_SQL) or (bq_col in BOOL_COLS_BQ)

            sql_norm = normalize_scalar(sql_col, sql_raw, is_bool_col=is_bool)
            bq_norm = normalize_scalar(bq_col, bq_raw, is_bool_col=is_bool)

            if sql_norm != bq_norm:
                row_mismatch = True
                mismatch_field_counts[bq_col] += 1
                if len(mismatch_samples) < MAX_MISMATCH_SAMPLES:
                    mismatch_samples.append((k, bq_col, sql_norm, bq_norm))

        if row_mismatch:
            mismatched_rows += 1
        else:
            matched_rows += 1

    return CompareResult(
        total_sql_rows=len(sql_df),
        total_bq_rows=len(bq_rows),
        common_keys=len(common),
        missing_in_bq=len(missing_in_bq),
        missing_in_sql=len(missing_in_sql),
        matched_rows=matched_rows,
        mismatched_rows=mismatched_rows,
        mismatch_field_counts=mismatch_field_counts,
        mismatch_samples=mismatch_samples,
    )


def write_outputs(res: CompareResult) -> None:
    summary_path = BASE_DIR / "comparison_summary.txt"
    mismatch_path = BASE_DIR / "mismatch_samples.csv"

    # --- Percentages ONLY over common_keys ---
    common = res.common_keys
    match_rate = (res.matched_rows / common) if common else 0.0
    mismatch_rate = (res.mismatched_rows / common) if common else 0.0

    # Coverage indicators (informational; not part of QA %)
    coverage_sql = (common / res.total_sql_rows) if res.total_sql_rows else 0.0
    coverage_bq = (common / res.total_bq_rows) if res.total_bq_rows else 0.0

    lines = []
    lines.append(f"SQL rows (total): {res.total_sql_rows}")
    lines.append(f"BQ rows (total):  {res.total_bq_rows}")
    lines.append("")
    lines.append(f"Common keys (analysis universe): {res.common_keys}")
    lines.append(f"Coverage vs SQL: {coverage_sql:.4%}")
    lines.append(f"Coverage vs BQ:  {coverage_bq:.4%}")
    lines.append("")
    lines.append(f"Missing in BQ (excluded from %):  {res.missing_in_bq}")
    lines.append(f"Missing in SQL (excluded from %): {res.missing_in_sql}")
    lines.append("")
    lines.append("=== RESULTS ON COMMON KEYS ONLY ===")
    lines.append(f"Matched rows:    {res.matched_rows}  ({match_rate:.4%})")
    lines.append(f"Mismatched rows: {res.mismatched_rows}  ({mismatch_rate:.4%})")
    lines.append("")
    lines.append("Top mismatched fields (counts on common keys):")
    for field, cnt in res.mismatch_field_counts.most_common(30):
        lines.append(f"  {field}: {cnt}")

    summary_path.write_text("\n".join(lines), encoding="utf-8")

    with mismatch_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["order_key", "field", "sql_value_norm", "bq_value_norm"])
        for k, field, a, b in res.mismatch_samples:
            w.writerow([k, field, a, b])

    print(f"[OK] Wrote {summary_path.name}")
    print(f"[OK] Wrote {mismatch_path.name} (up to {MAX_MISMATCH_SAMPLES} samples)")
    print("")
    print("=== QUICK VIEW (COMMON KEYS ONLY) ===")
    # show the core block
    for ln in lines[0:18]:
        print(ln)
    print("")
    print("=== MISMATCH SAMPLE (first 20) ===")
    for row in res.mismatch_samples[:20]:
        print(row)

    print("=== DISTINCT MISMATCH VALUES (with sample order_key) ===")
    combo_keys: Dict[Tuple[str, str, str], List[str]] = defaultdict(list)

    for key, field, sql_val, bq_val in res.mismatch_samples:
        combo_key = (field, keyify(sql_val), keyify(bq_val))
        keys = combo_keys[combo_key]
        if len(keys) < 5:
            keys.append(str(key))

    seen = set()
    for key, field, sql_val, bq_val in res.mismatch_samples:
        combo_key = (field, keyify(sql_val), keyify(bq_val))
        if combo_key in seen:
            continue
        seen.add(combo_key)
        print(
            f"{field}: {keyify(sql_val)}  !=  {keyify(bq_val)} | "
            f"sample order_key: {combo_keys[combo_key]}"
        )


def main() -> None:
    sql_path = resolve_path(SQLSERVER_CSV)
    bq_path = resolve_path(BIGQUERY_JSON)

    sql_df = load_sql_csv(sql_path)
    bq_rows = load_bq_json(bq_path)

    if sql_df.empty:
        die("[SQL] CSV loaded but is empty.")
    if not bq_rows:
        die("[BQ] JSON/JSONL loaded but has 0 objects.")

    if SQL_KEY not in sql_df.columns:
        die(f"[SQL] Missing key column '{SQL_KEY}' in CSV.")

    res = compare(sql_df, bq_rows)

    if res.common_keys == 0:
        die("[QA] common_keys = 0. No overlap between CSV OrderKey and JSON order_key.")

    write_outputs(res)


if __name__ == "__main__":
    main()

Con tus números del script simple:
	•	common_keys ≈ 1209 (matches)
	•	entonces toda la tasa va a ser sobre 1209, no sobre 128470 ni 12655.

Si quieres el siguiente ajuste: que el script también escriba un missing_in_bq_keys.csv y missing_in_sql_keys.csv (solo listas de keys), lo hago.



