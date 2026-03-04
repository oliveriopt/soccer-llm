Tengo este archivo en csv:
  rderNumber,OrderKey,OrderDate,ScheduledPickupDate,ScheduledDeliveryDate,PickupDate,PickupLate,DeliveredDate,CarrierAssignDate,VoidDate,BusinessUnitCode,ServiceCode,SourceSystemName,FreightOptimizerCustomerCode,CarrierCode,EquipmentCode,OrderStatusTypeCode,BrokerageStatusTypeCode,OrderFinancialStatusTypeCode,IsOnHold,IsUncommited,TripCount,OrderSalesAmount,FuelChargeAmount,LineHaulChargeAmount,OrderCost,FuelCostAmount,LineHaulCostAmount,CrossBooked,BounceToBounce,OnTimeDelivery,DeliveryDelay,OnTimePickup,PickupDelay,BillDistance,WeightInPounds,OriginCity,OriginStateCode,OriginPostalCode,OriginCountryCode,OriginLocationID,DestinationCity,DestinationStateCode,DestinationPostalCode,DestinationCountryCode,DestinationLocationID,EnteredByUserName,PrimarySalesRepUserName,SecondarySalesRepUserName,BookedByUserName,PrimaryCarrierRepUserName,FreightAssignRepUserName,PrimaryCarrierRepLocationCode,CreatedByUser,DetentionAmount,TONUAmount,TotalAccessorialAmount,LHRate,IsHazmat,Quantity,Volume,PickupRescheduledReason,DropRescheduledReason,CarrierPortalPostingDate,IsDropAndHook,PickupServiceFailureCodeID,DeliveryServiceFailureCodeID,PercentageOfCommissionPrimary,DeliveryArrivalDate,OrderSourceCode,CompanyID,ValueOfGoods,BazookaOrderId,ShipmentID,PickupDepartureDate,PickupServiceFailureReasonCategoryID,DeliveryServiceFailureReasonCategoryID
21545305,17443795,2026-02-02 14:43:04.63,2026-03-04 13:00:00.00,2026-03-05 02:00:00.00,2026-03-03 19:00:00.00,2026-03-04 13:00:00.00,NULL,2026-02-25 11:43:12.39,NULL,SMBKNX,TL_CONT,Freight Optimizer,LOUICH38,UNKEGRGA,VR,P,05PICKED,NEW,N,N,0,1500.00,0.00,1500.00,1350.00,0.00,1350.00,0,0,NULL,NULL,1,NULL,523.00,30000.00,Baton Rouge,LA,70815,USA,2029265,Douglasville,GA,30135,USA,2370444,tyler.mynatt,tyler.mynatt,NULL,daniel.timmons,daniel.timmons,NULL,COYGXCOV,tyler.mynatt,0.00,0.00,0.00,1500.00,0,26,NULL,Customer,Customer,NULL,0,NULL,NULL,100.00,NULL,FO,14,100000.0000,NULL,310517,2026-03-04 06:46:00.00,NULL,NULL
22331571,19801262,2026-02-25 02:10:57.06,2026-03-02 00:00:00.00,2026-03-06 00:00:00.00,2026-03-02 00:00:00.00,2026-03-02 00:00:00.00,NULL,NULL,NULL,GX7,LTL,Freight Optimizer,INGRCH20,OLDDTHN1,VN53,P,06ONTRAK,NEW,N,N,0,895.19,181.78,713.41,880.94,181.78,699.16,1,0,NULL,NULL,1,NULL,1.00,6073.00,Belcamp,MD,21017,USA,2352686,Fargo,ND,58102,USA,2386510,NULL,casey.priseman,NULL,carla.granato,carla.granato,NULL,XPOCHA,vrep,0.00,0.00,0.00,713.41,0,120,NULL,NULL,NULL,2026-02-25 02:10:59.18,0,NULL,NULL,100.00,NULL,EDI-DOC,14,0.0000,NULL,202570311,2026-03-02 01:00:00.00,NULL,NULL
21056152,16375185,2026-01-08 17:01:23.97,2026-01-15 16:00:00.00,2026-01-16 15:00:00.00,2026-01-15 13:30:00.00,2026-01-15 16:00:00.00,2026-01-16 10:00:00.00,2026-01-13 12:21:41.21,NULL,CKNX,TL_CONT,Freight Optimizer,RADICH30,KPVHKY,VN53,D,09DELVRD,PROCESSING,N,N,0,619.69,48.03,571.66,650.00,0.00,650.00,0,0,1,NULL,1,NULL,126.40,12589.00,Plainfield,IN,46168,USA,2386559,Cincinnati,OH,45246,USA,2757447,gregory.kelly,Alex.Strozyk,NULL,christian.labelle,christian.labelle,NULL,COYGXCOV,gregory.kelly,0.00,0.00,0.00,571.66,0,18,NULL,Customer,Customer,NULL,0,NULL,NULL,100.00,2026-01-16 08:00:00.00,EDI,14,100000.0000,NULL,H20323086,2026-01-15 15:53:30.00,NULL,NULL
22369889,19836970,2026-02-27 17:11:48.14,2026-03-05 09:00:00.00,2026-03-09 11:00:00.00,NULL,NULL,NULL,NULL,NULL,GAI03,TL_SPOT,Freight Optimizer,MICHBLMN,NULL,R53,A,02AVAIL,NEW,N,N,0,7374.75,0.00,7374.75,0.00,0.00,0.00,0,0,NULL,NULL,NULL,NULL,1847.90,41853.00,Eagan,MN,55121,USA,644642,Deerfield Beach,FL,33442,USA,33579,brivas,kmerck,NULL,NULL,NULL,NULL,NULL,brivas,0.00,0.00,0.00,7374.75,0,1317,NULL,Customer,Customer,2026-03-04 09:06:44.86,0,NULL,NULL,100.00,NULL,EDI,1,100000.0000,NULL,202726194,NULL,NULL,NULL
22328546,19798424,2026-02-24 15:19:38.28,2026-02-25 10:30:00.00,2026-02-27 09:30:00.00,2026-02-25 13:40:00.00,NULL,2026-02-27 09:14:00.00,2026-02-24 16:27:32.29,NULL,CHI3,TL_SPOT,Freight Optimizer,STARSEW5,SHIPSAC3,VN53,D,09DELVRD,PROCESSING,N,N,0,3450.00,0.00,3450.00,2600.00,0.00,2600.00,1,0,1,NULL,0,190,915.50,20856.00,Fresno,CA,93702,USA,2454304,Auburn,WA,98001,USA,2616156,cyoung,jarguelles,NULL,skhaira,skhaira,NULL,XPOVAN2,cyoung,0.00,0.00,0.00,3450.00,0,11,NULL,NULL,NULL,NULL,0,11,NULL,100.00,2026-02-27 08:53:54.00,EDI,1,100000.0000,NULL,SB02016865,2026-02-25 16:00:00.00,4,NULL
21829898,18208045,2026-02-12 12:44:52.74,2026-02-16 12:00:00.00,2026-02-17 05:00:00.00,2026-02-16 10:08:00.00,2026-02-16 12:00:00.00,2026-02-17 06:57:44.00,2026-02-15 08:37:00.76,NULL,CDET2,TL_CONT,Freight Optimizer,ABSOCH16,ACEDCIOH,VN53,D,09DELVRD,PROCESSING,N,N,0,1128.30,128.30,724.07,1025.00,0.00,1025.00,0,0,1,NULL,1,NULL,256.60,35896.00,Champaign,IL,61822,USA,2386573,Windsor,WI,53598,USA,536920,shelby.rudolph,joshua.mcbride,NULL,anthony.garetto,anthony.garetto,NULL,COYGXCOV,shelby.rudolph,0.00,0.00,275.93,724.07,0,28,NULL,Customer,Customer,NULL,0,NULL,NULL,100.00,2026-02-17 04:30:11.00,EDI,14,100000.0000,NULL,0001290272,2026-02-16 14:53:18.00,NULL,NULL


Y en json tengo este:
[{
  "source_system_name": "Freight Optimizer",
  "order_source_code": "RPA",
  "business_unit_code": "CDET",
  "service_type_code": "TL_SPOT",
  "order_status_type_code": "V",
  "brokerage_status_code": "10VOID",
  "pickup_rescheduled_reason": null,
  "drop_rescheduled_reason": "Customer",
  "is_on_hold": "Y",
  "is_uncommited": "N",
  "order_financial_status_type_code": "VOID",
  "order_date": "2025-12-18T14:36:21.260000",
  "scheduled_pickup_date": "2025-12-22T05:00:00",
  "scheduled_delivery_date": "2025-12-23T12:00:00",
  "pickup_date": null,
  "pickup_departure_date": null,
  "delivered_date": null,
  "delivery_arrival_date": null,
  "carrier_assign_date": null,
  "void_date": null,
  "freight_optimizer_customer_code": "3MCOCHI4",
  "carrier_code": null,
  "equipment_code": "V",
  "order_key": "16248619",
  "order_number": "20921632",
  "order_sales_amount": "1800",
  "line_haul_charge_amount": "1800",
  "fuel_charge_amount": "0",
  "total_accessorial_charge": "0",
  "detention_charge": "0",
  "tonu_charge": "0",
  "line_haul_pay_amount": "0.0",
  "fuel_pay_amount": "0.0",
  "order_cost": "0.0",
  "lh_rate": "1800",
  "trip_count": "0",
  "bill_distance": "347.0",
  "weight_in_pounds": "42143",
  "on_time_delivery": null,
  "delivery_delay": null,
  "on_time_pickup": null,
  "pickup_delay": null,
  "origin_city": "Quincy",
  "origin_state_code": "IL",
  "origin_postal_code": "62305",
  "origin_country_code": "USA",
  "origin_location_id": "2738928",
  "destination_city": "Hartford City",
  "destination_state_code": "IN",
  "destination_postal_code": "47348",
  "destination_country_code": "USA",
  "destination_location_id": "2738929",
  "entered_by_user_name": null,
  "primary_sales_rep_user_name": "ray.eichenlaub",
  "secondary_sales_rep_user_name": null,
  "booked_by_user_name": null,
  "primary_carrier_rep_user_name": null,
  "freight_assign_rep_user_name": null,
  "dispatch_office_location_code": null,
  "cross_booked": "0",
  "bounce_to_bounce": "0",
  "created_by_user": "RPA BOT",
  "is_hazmat": "false",
  "quantity": "17",
  "pickup_late": "2025-12-22T06:00:00",
  "carrier_portal_posting_date": null,
  "is_drop_and_hook": "false",
  "pickup_service_failure_code_id": null,
  "pickup_service_failure_reason_category_id": null,
  "delivery_service_failure_code_id": null,
  "delivery_service_failure_reason_category_id": null,
  "percentage_of_commission_primary": "100.0",
  "company_id": null,
  "value_of_goods": "100000",
  "bazooka_order_id": null,
  "shipment_id": "123580",
  "origin_scheduled_departure_time": null,
  "destination_stop_scheduled_departure_time": null,
  "trip_id": "16350527",
  "trip_brokerage_status_id": "1",
  "carrier_id": null
}, {
  "source_system_name": "Freight Optimizer",
  "order_source_code": "EDI",
  "business_unit_code": "CDET2",
  "service_type_code": "TL_CONT",
  "order_status_type_code": "V",
  "brokerage_status_code": "10VOID",
  "pickup_rescheduled_reason": null,
  "drop_rescheduled_reason": null,
  "is_on_hold": "Y",
  "is_uncommited": "N",
  "order_financial_status_type_code": "VOID",
  "order_date": "2026-02-16T08:52:32.360000",
  "scheduled_pickup_date": "2026-02-23T00:00:00",
  "scheduled_delivery_date": "2026-02-23T00:00:00",
  "pickup_date": null,
  "pickup_departure_date": null,
  "delivered_date": null,
  "delivery_arrival_date": null,
  "carrier_assign_date": null,
  "void_date": null,
  "freight_optimizer_customer_code": "ABSOCH16",
  "carrier_code": null,
  "equipment_code": "VN53",
  "order_key": "19720907",
  "order_number": "22246177",
  "order_sales_amount": "1128.3",
  "line_haul_charge_amount": "1128.3",
  "fuel_charge_amount": "0",
  "total_accessorial_charge": "0",
  "detention_charge": "0",
  "tonu_charge": "0",
  "line_haul_pay_amount": "0.0",
  "fuel_pay_amount": "0.0",
  "order_cost": "0.0",
  "lh_rate": "1128.3",
  "trip_count": "0",
  "bill_distance": "151.5",
  "weight_in_pounds": "41613",
  "on_time_delivery": null,
  "delivery_delay": null,
  "on_time_pickup": null,
  "pickup_delay": null,
  "origin_city": "Woodridge",
  "origin_state_code": "IL",
  "origin_postal_code": "60517",
  "origin_country_code": "USA",
  "origin_location_id": "2343141",
  "destination_city": "Windsor",
  "destination_state_code": "WI",
  "destination_postal_code": "53598",
  "destination_country_code": "USA",
  "destination_location_id": "536920",
  "entered_by_user_name": "joe.reagan",
  "primary_sales_rep_user_name": "joshua.mcbride",
  "secondary_sales_rep_user_name": null,
  "booked_by_user_name": null,
  "primary_carrier_rep_user_name": null,
  "freight_assign_rep_user_name": null,
  "dispatch_office_location_code": null,
  "cross_booked": "0",
  "bounce_to_bounce": "0",
  "created_by_user": "joe.reagan",
  "is_hazmat": "false",
  "quantity": "18",
  "pickup_late": null,
  "carrier_portal_posting_date": null,
  "is_drop_and_hook": "false",
  "pickup_service_failure_code_id": null,
  "pickup_service_failure_reason_category_id": null,
  "delivery_service_failure_code_id": null,
  "delivery_service_failure_reason_category_id": null,
  "percentage_of_commission_primary": "100.0",
  "company_id": null,
  "value_of_goods": "100000",
  "bazooka_order_id": null,
  "shipment_id": "0001291203",
  "origin_scheduled_departure_time": null,
  "destination_stop_scheduled_departure_time": null,
  "trip_id": "19824993",
  "trip_brokerage_status_id": "1",
  "carrier_id": null
}, {
  "source_system_name": "Freight Optimizer",
  "order_source_code": "EDI-DOC",
  "business_unit_code": "CLT8",
  "service_type_code": "LTL",
  "order_status_type_code": "V",
  "brokerage_status_code": "10VOID",
  "pickup_rescheduled_reason": null,
  "drop_rescheduled_reason": null,
  "is_on_hold": "N",
  "is_uncommited": "N",
  "order_financial_status_type_code": "VOID",
  "order_date": "2026-02-21T06:06:17.830000",
  "scheduled_pickup_date": "2026-02-21T07:00:00",
  "scheduled_delivery_date": "2026-02-23T09:00:00",
  "pickup_date": null,
  "pickup_departure_date": null,
  "delivered_date": null,
  "delivery_arrival_date": null,
  "carrier_assign_date": null,
  "void_date": null,
  "freight_optimizer_customer_code": "ACUICHN1",
  "carrier_code": null,
  "equipment_code": "LTL",
  "order_key": "19777628",
  "order_number": "22306320",
  "order_sales_amount": "96.48",
  "line_haul_charge_amount": "85",
  "fuel_charge_amount": "11.48",
  "total_accessorial_charge": "0",
  "detention_charge": "0",
  "tonu_charge": "0",
  "line_haul_pay_amount": "76.0",
  "fuel_pay_amount": "10.26",
  "order_cost": "86.26",
  "lh_rate": "85",
  "trip_count": "0",
  "bill_distance": "1.0",
  "weight_in_pounds": "18",
  "on_time_delivery": null,
  "delivery_delay": null,
  "on_time_pickup": null,
  "pickup_delay": null,
  "origin_city": "Conyers",
  "origin_state_code": "GA",
  "origin_postal_code": "30012",
  "origin_country_code": "USA",
  "origin_location_id": "2787566",
  "destination_city": "Clover",
  "destination_state_code": "SC",
  "destination_postal_code": "29710",
  "destination_country_code": "USA",
  "destination_location_id": "2825324",
  "entered_by_user_name": null,
  "primary_sales_rep_user_name": "justin.lawing",
  "secondary_sales_rep_user_name": null,
  "booked_by_user_name": null,
  "primary_carrier_rep_user_name": null,
  "freight_assign_rep_user_name": null,
  "dispatch_office_location_code": null,
  "cross_booked": "0",
  "bounce_to_bounce": "0",
  "created_by_user": "vrep",
  "is_hazmat": "false",
  "quantity": "1",
  "pickup_late": "2026-02-21T09:00:00",
  "carrier_portal_posting_date": null,
  "is_drop_and_hook": "false",
  "pickup_service_failure_code_id": null,
  "pickup_service_failure_reason_category_id": null,
  "delivery_service_failure_code_id": null,
  "delivery_service_failure_reason_category_id": null,
  "percentage_of_commission_primary": "100.0",
  "company_id": null,
  "value_of_goods": "0",
  "bazooka_order_id": null,
  "shipment_id": "AYI00848245",
  "origin_scheduled_departure_time": null,
  "destination_stop_scheduled_departure_time": null,
  "trip_id": "19882378",
  "trip_brokerage_status_id": "1",
  "carrier_id": null
}]

Tengo este codigo que analiza:

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

SQLSERVER_CSV = BASE_DIR / "sqlserver_3.csv"
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


Quiero que lo cambies para que lean los dos tipos de archivos. Igual compara sobre los order_key que hagan match en los dos archivos. 
