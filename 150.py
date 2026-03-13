Tengo este header en sqlserver:

OrderNumber	OrderKey	OrderDate	ScheduledPickupDate	ScheduledDeliveryDate	PickupDate	PickupLate	DeliveredDate	CarrierAssignDate	VoidDate	BusinessUnitCode	ServiceCode	SourceSystemName	FreightOptimizerCustomerCode	CarrierCode	EquipmentCode	OrderStatusTypeCode	BrokerageStatusTypeCode	OrderFinancialStatusTypeCode	IsOnHold	IsUncommited	TripCount	OrderSalesAmount	FuelChargeAmount	LineHaulChargeAmount	OrderCost	FuelCostAmount	LineHaulCostAmount	CrossBooked	BounceToBounce	OnTimeDelivery	DeliveryDelay	OnTimePickup	PickupDelay	BillDistance	WeightInPounds	OriginCity	OriginStateCode	OriginPostalCode	OriginCountryCode	OriginLocationID	DestinationCity	DestinationStateCode	DestinationPostalCode	DestinationCountryCode	DestinationLocationID	EnteredByUserName	PrimarySalesRepUserName	SecondarySalesRepUserName	BookedByUserName	PrimaryCarrierRepUserName	FreightAssignRepUserName	PrimaryCarrierRepLocationCode	CreatedByUser	DetentionAmount	TONUAmount	TotalAccessorialAmount	LHRate	IsHazmat	Quantity	Volume	PickupRescheduledReason	DropRescheduledReason	CarrierPortalPostingDate	IsDropAndHook	PickupServiceFailureCodeID	DeliveryServiceFailureCodeID	PercentageOfCommissionPrimary	DeliveryArrivalDate	OrderSourceCode	CompanyID	ValueOfGoods	BazookaOrderId	ShipmentID	PickupDepartureDate	PickupServiceFailureReasonCategoryID	DeliveryServiceFailureReasonCategoryID
21461778	17345009	2026-01-26 12:41:31.64	2026-01-27 17:00:00.00	2026-01-28 11:00:00.00	2026-01-27 17:00:00.00	NULL	2026-01-28 19:33:00.00	2026-01-27 10:21:59.22	NULL	VAN	TL_CONT	Freight Optimizer	NATURIB1	VEERBRO6	VR	D	09DELVRD	COMPLETE	N	N	0	995.00	0.00	995.00	998.55	0.00	914.42	1	0	1	NULL	1	NULL	602.50	42000.00	Sussex	WI	53089	USA	141031	Thornhill	ON	L4J8X9	CAN	1547924	maxilom.camaongay	sromero	NULL	JCross	JCross	NULL	XPOCAN	maxilom.camaongay	0.00	0.00	0.00	995.00	0	1	NULL	NULL	Customer	NULL	0	NULL	NULL	100.00	2026-01-28 11:00:00.00	FO	2	100000.0000	NULL	NULL	2026-01-28 00:31:00.00	NULL	NULL
21461958	17345122	2026-01-26 12:47:53.23	2026-01-31 18:00:00.00	2026-02-02 23:00:00.00	2026-02-01 00:27:57.00	NULL	2026-02-03 00:27:48.00	2026-01-29 15:46:16.71	NULL	LAK3	TL_SPOT	Freight Optimizer	JOHNEDM3	JAJCGAFL	VN53	D	09DELVRD	PROCESSING	N	N	0	3720.00	400.00	3320.00	3400.00	0.00	3400.00	1	0	0	4	0	387	1006.00	40058.00	Middletown	DE	19709	USA	2397824	Lakeland	FL	33815	USA	1991838	ckane	agunnufson	NULL	tom.sobleskyii	tom.sobleskyii	NULL	COYGXCOV	ckane	0.00	0.00	0.00	3320.00	0	1	NULL	Customer	Customer	NULL	0	NULL	NULL	100.00	2026-02-02 23:04:56.00	EDI	1	100000.0000	NULL	SA21446837	2026-02-01 12:08:27.64	NULL	NULL
21017823	16339081	2026-01-05 15:03:28.16	2026-01-06 10:00:00.00	2026-01-07 08:00:00.00	2026-01-06 09:45:00.00	2026-01-06 15:30:00.00	2026-01-06 18:26:00.00	2026-01-05 15:32:16.79	NULL	GAI41	TL_SPOT	Freight Optimizer	TEXTSTMO	EMPIFLNC	VN53	D	09DELVRD	PENDING	N	N	0	799.50	149.50	650.00	500.00	0.00	500.00	0	0	1	NULL	1	NULL	346.90	907.00	Bonifay	FL	32425	USA	1976159	Graniteville	SC	29829	USA	1009953	johana.cruz	jack.gayton	NULL	ssaccavino	ssaccavino	NULL	XPOGAI4	johana.cruz	0.00	0.00	0.00	650.00	0	1	NULL	NULL	Customer	NULL	0	NULL	NULL	100.00	2026-01-06 18:00:00.00	FO	1	100000.0000	NULL	NULL	2026-01-06 09:50:00.00	NULL	NULL
21142469	16456432	2026-01-20 08:40:42.64	2026-02-09 13:00:00.00	2026-02-10 06:00:00.00	2026-02-09 17:04:14.62	NULL	2026-02-10 05:21:01.00	2026-02-09 13:52:08.22	NULL	CMH1	TL_SPOT	Freight Optimizer	TARGMIM6	CARGWIIL	R53	D	09DELVRD	PENDING	N	N	0	2286.20	206.20	1880.00	2000.00	0.00	2000.00	0	0	1	NULL	0	244	424.20	39702.00	Greeneville	TN	37745	USA	2652412	Chambersburg	PA	17202	USA	538442	jessica.valore	ECaldwell	NULL	jbute	jbute	NULL	XPOCMH	jessica.valore	0.00	0.00	200.00	1880.00	0	1456	NULL	Customer	Customer	NULL	0	NULL	NULL	100.00	2026-02-10 02:45:39.78	EDI	1	100000.0000	NULL	67957151	2026-02-09 18:27:28.27	NULL	NULL

y este en jsonl:
[{
  "source_system_name": "Freight Optimizer",
  "order_source_code": "XPOC",
  "business_unit_code": "SMBDEN",
  "service_type_code": "LTL",
  "order_status_type_code": "D",
  "brokerage_status_code": "09DELVRD",
  "pickup_rescheduled_reason": null,
  "drop_rescheduled_reason": null,
  "is_on_hold": "N",
  "is_uncommited": "N",
  "order_financial_status_type_code": "PROCESSING",
  "order_date": "2026-01-01T17:10:02.920000",
  "scheduled_pickup_date": "2026-01-02T13:00:00",
  "scheduled_delivery_date": "2026-01-05T14:00:00",
  "pickup_date": "2026-01-02T12:47:00",
  "pickup_departure_date": "2026-01-02T12:47:00",
  "delivered_date": "2026-01-05T13:28:00",
  "delivery_arrival_date": "2026-01-05T13:08:00",
  "carrier_assign_date": null,
  "void_date": null,
  "freight_optimizer_customer_code": "WESECHIL",
  "carrier_code": "COAAMI",
  "equipment_code": "V",
  "order_key": "16320666",
  "order_number": "20998108",
  "order_sales_amount": "337.88",
  "line_haul_charge_amount": "217",
  "fuel_charge_amount": "19.88",
  "total_accessorial_charge": "101",
  "detention_charge": "0",
  "tonu_charge": "0",
  "line_haul_pay_amount": "142",
  "fuel_pay_amount": "19.88",
  "order_cost": "262.88",
  "lh_rate": "217",
  "trip_count": "0",
  "bill_distance": "1.0",
  "weight_in_pounds": "300",
  "on_time_delivery": "1",
  "delivery_delay": null,
  "on_time_pickup": "1",
  "pickup_delay": null,
  "origin_city": null,
  "origin_state_code": null,
  "origin_postal_code": null,
  "origin_country_code": null,
  "origin_location_id": "2749596",
  "destination_city": null,
  "destination_state_code": null,
  "destination_postal_code": null,
  "destination_country_code": null,
  "destination_location_id": "2749597",
  "entered_by_user_name": "tyler.hulegaard",
  "primary_sales_rep_user_name": null,
  "secondary_sales_rep_user_name": null,
  "booked_by_user_name": "Connect CustPortal",
  "primary_carrier_rep_user_name": "Connect CustPortal",
  "freight_assign_rep_user_name": null,
  "dispatch_office_location_code": null,
  "cross_booked": "0",
  "bounce_to_bounce": "0",
  "created_by_user": "tyler.hulegaard",
  "is_hazmat": "false",
  "quantity": null,
  "pickup_late": "2026-01-02T15:00:00",
  "carrier_portal_posting_date": "2026-01-01T17:10:07.400000",
  "is_drop_and_hook": "false",
  "pickup_service_failure_code_id": null,
  "pickup_service_failure_reason_category_id": null,
  "delivery_service_failure_code_id": null,
  "delivery_service_failure_reason_category_id": null,
  "percentage_of_commission_primary": null,
  "company_id": "14",
  "value_of_goods": "0",
  "bazooka_order_id": null,
  "shipment_id": null,
  "origin_scheduled_departure_time": null,
  "destination_stop_scheduled_departure_time": null,
  "trip_id": "16422888",
  "trip_brokerage_status_id": "8",
  "carrier_id": "254244"
}, {
  "source_system_name": "Freight Optimizer",
  "order_source_code": "XPOC",
  "business_unit_code": "SMBDEN",
  "service_type_code": "LTL",
  "order_status_type_code": "D",
  "brokerage_status_code": "09DELVRD",
  "pickup_rescheduled_reason": null,
  "drop_rescheduled_reason": null,
  "is_on_hold": "N",
  "is_uncommited": "N",
  "order_financial_status_type_code": "COMPLETE",
  "order_date": "2026-01-01T17:07:47.910000",
  "scheduled_pickup_date": "2026-01-02T13:00:00",
  "scheduled_delivery_date": "2026-01-05T16:00:00",
  "pickup_date": "2026-01-02T12:47:00",
  "pickup_departure_date": "2026-01-02T12:47:00",
  "delivered_date": "2026-01-05T11:02:00",
  "delivery_arrival_date": "2026-01-05T10:47:00",
  "carrier_assign_date": null,
  "void_date": null,
  "freight_optimizer_customer_code": "WESECHIL",
  "carrier_code": "COAAMI",
  "equipment_code": "V",
  "order_key": "16320665",
  "order_number": "20998107",
  "order_sales_amount": "350.55",
  "line_haul_charge_amount": "293.9",
  "fuel_charge_amount": "30.65",
  "total_accessorial_charge": "26",
  "detention_charge": "0",
  "tonu_charge": "0",
  "line_haul_pay_amount": "218.9",
  "fuel_pay_amount": "30.65",
  "order_cost": "275.55",
  "lh_rate": "293.9",
  "trip_count": "0",
  "bill_distance": "1.0",
  "weight_in_pounds": "300",
  "on_time_delivery": "1",
  "delivery_delay": null,
  "on_time_pickup": "1",
  "pickup_delay": null,
  "origin_city": null,
  "origin_state_code": null,
  "origin_postal_code": null,
  "origin_country_code": null,
  "origin_location_id": "2749594",
  "destination_city": null,
  "destination_state_code": null,
  "destination_postal_code": null,
  "destination_country_code": "USA",
  "destination_location_id": "2650291",
  "entered_by_user_name": "tyler.hulegaard",
  "primary_sales_rep_user_name": null,
  "secondary_sales_rep_user_name": null,
  "booked_by_user_name": "Connect CustPortal",
  "primary_carrier_rep_user_name": "Connect CustPortal",
  "freight_assign_rep_user_name": null,
  "dispatch_office_location_code": null,
  "cross_booked": "0",
  "bounce_to_bounce": "0",
  "created_by_user": "tyler.hulegaard",
  "is_hazmat": "false",
  "quantity": null,
  "pickup_late": "2026-01-02T15:00:00",
  "carrier_portal_posting_date": "2026-01-01T17:07:51.340000",
  "is_drop_and_hook": "false",
  "pickup_service_failure_code_id": null,
  "pickup_service_failure_reason_category_id": null,
  "delivery_service_failure_code_id": null,
  "delivery_service_failure_reason_category_id": null,
  "percentage_of_commission_primary": null,
  "company_id": "14",
  "value_of_goods": "0",
  "bazooka_order_id": null,
  "shipment_id": null,
  "origin_scheduled_departure_time": null,
  "destination_stop_scheduled_departure_time": null,
  "trip_id": "16422887",
  "trip_brokerage_status_id": "8",
  "carrier_id": "254244"
}, {
  "source_system_name": "Freight Optimizer",
  "order_source_code": "XPOC",
  "business_unit_code": "CLT1",
  "service_type_code": "LTL",
  "order_status_type_code": "D",
  "brokerage_status_code": "09DELVRD",
  "pickup_rescheduled_reason": null,
  "drop_rescheduled_reason": null,
  "is_on_hold": "N",
  "is_uncommited": "N",
  "order_financial_status_type_code": "COMPLETE",
  "order_date": "2026-01-01T18:26:53.120000",
  "scheduled_pickup_date": "2026-01-02T09:00:00",
  "scheduled_delivery_date": "2026-01-06T16:00:00",
  "pickup_date": "2026-01-02T15:31:00",
  "pickup_departure_date": "2026-01-02T15:46:00",
  "delivered_date": "2026-01-08T11:50:00",
  "delivery_arrival_date": "2026-01-08T11:49:00",
  "carrier_assign_date": null,
  "void_date": null,
  "freight_optimizer_customer_code": "PONDBEC1",
  "carrier_code": "SAIAJOGA",
  "equipment_code": "V",
  "order_key": "16320675",
  "order_number": "20998118",
  "order_sales_amount": "449.5",
  "line_haul_charge_amount": "257.28",
  "fuel_charge_amount": "54.72",
  "total_accessorial_charge": "137.5",
  "detention_charge": "0",
  "tonu_charge": "0",
  "line_haul_pay_amount": "180.19",
  "fuel_pay_amount": "54.72",
  "order_cost": "411.41",
  "lh_rate": "257.2789",
  "trip_count": "0",
  "bill_distance": "1.0",
  "weight_in_pounds": "185",
  "on_time_delivery": "0",
  "delivery_delay": "2629",
  "on_time_pickup": "1",
  "pickup_delay": null,
  "origin_city": null,
  "origin_state_code": null,
  "origin_postal_code": null,
  "origin_country_code": "USA",
  "origin_location_id": "2660930",
  "destination_city": null,
  "destination_state_code": null,
  "destination_postal_code": null,
  "destination_country_code": "USA",
  "destination_location_id": "2723427",
  "entered_by_user_name": "aiden.desroches",
  "primary_sales_rep_user_name": null,
  "secondary_sales_rep_user_name": null,
  "booked_by_user_name": "Connect CustPortal",
  "primary_carrier_rep_user_name": "Connect CustPortal",
  "freight_assign_rep_user_name": null,
  "dispatch_office_location_code": null,
  "cross_booked": "0",
  "bounce_to_bounce": "0",
  "created_by_user": "aiden.desroches",
  "is_hazmat": "false",
  "quantity": null,
  "pickup_late": "2026-01-02T17:00:00",
  "carrier_portal_posting_date": "2026-01-01T18:26:55",
  "is_drop_and_hook": "false",
  "pickup_service_failure_code_id": null,
  "pickup_service_failure_reason_category_id": null,
  "delivery_service_failure_code_id": null,
  "delivery_service_failure_reason_category_id": null,
  "percentage_of_commission_primary": null,
  "company_id": "1",
  "value_of_goods": "0",
  "bazooka_order_id": null,
  "shipment_id": null,
  "origin_scheduled_departure_time": null,
  "destination_stop_scheduled_departure_time": null,
  "trip_id": "16422897",
  "trip_brokerage_status_id": "8",
  "carrier_id": "279155"
}]

Quiero que hagas un codigo en python para:

1) Tomar los orderKey y order_key y compararlos 1 a 1.
2) Mapea los nombres de las columnas, uno esta en modo UpperCase y el otro en snake case.
3) En Csv ten en cuenta que est en formato tsv, y que las fechas tienen un espacio, o sea esta en formato YYYY-MM-DD hh:mm:ss.ss. 
4) Los decimales redondea a dos decimales, por ejemplo hay varios numeros que tienen 4 decimales, redondea a dos decimales hacia arriba y comparalos.
5) Los eneteros comparalos como enteros
6) En sql los NULL son qsi, y en json son "null"
7) ALgunos campos booleanos en sql server estan como 0 y 1, mientras que en json estan como True y Falso, ten en cuenta eso.
8) Haz el codigo harcoded, donde la entrada se escribe en el codigo y el csv y el json estan en la misma carpeta del codigo .py
9) Para evitar problemas de PATH del compilador, hazlo universal usando la libreria PATH de python.
10) QUieor un resumen de laslineas que hacen match y missmatch, las que hacen mismatch hazme un top, e igualmente haz una muestra de las que hacen mimsatch, con el orderkey y la informacion que esta haciendo missmatch.



Aquí tienes el código completo, hardcoded, para comparar el TSV de SQL Server contra el JSON/JSONL de BigQuery usando orderKey vs order_key, con mapeo de columnas, normalización de nulos, fechas, enteros, decimales y booleanos.

Cámbiale solo estos dos nombres si tus archivos se llaman distinto:

SQLSERVER_TSV_FILE = "sqlserver.tsv"
JSON_FILE = "bigquery.jsonl"

Código:

from __future__ import annotations

import csv
import json
import math
import re
from decimal import Decimal, InvalidOperation, ROUND_CEILING
from pathlib import Path
from datetime import datetime
from collections import Counter, defaultdict

# ============================================================
# HARD CODED INPUT FILES
# Ambos archivos deben estar en la misma carpeta que este .py
# ============================================================
SQLSERVER_TSV_FILE = "sqlserver.tsv"
JSON_FILE = "bigquery.jsonl"

BASE_DIR = Path(__file__).resolve().parent
SQLSERVER_TSV_PATH = BASE_DIR / SQLSERVER_TSV_FILE
JSON_PATH = BASE_DIR / JSON_FILE

# ============================================================
# CONFIG
# ============================================================

# Cuántos ejemplos mostrar
TOP_MISMATCH_COLUMNS = 20
SAMPLE_MISMATCH_ROWS = 10

# Normalización de nombres especiales que NO siguen solo PascalCase -> snake_case
SPECIAL_COLUMN_MAP = {
    "ServiceCode": "service_type_code",
    "BrokerageStatusTypeCode": "brokerage_status_code",
    "OrderFinancialStatusTypeCode": "order_financial_status_type_code",
    "DetentionAmount": "detention_charge",
    "TONUAmount": "tonu_charge",
    "TotalAccessorialAmount": "total_accessorial_charge",
    "FuelCostAmount": "fuel_pay_amount",
    "LineHaulCostAmount": "line_haul_pay_amount",
    "PrimaryCarrierRepLocationCode": "dispatch_office_location_code",
    "PickupServiceFailureCodeID": "pickup_service_failure_code_id",
    "DeliveryServiceFailureCodeID": "delivery_service_failure_code_id",
    "PickupServiceFailureReasonCategoryID": "pickup_service_failure_reason_category_id",
    "DeliveryServiceFailureReasonCategoryID": "delivery_service_failure_reason_category_id",
    "LHRate": "lh_rate",
    "OrderKey": "order_key",
    "OrderNumber": "order_number",
    "OrderDate": "order_date",
    "ScheduledPickupDate": "scheduled_pickup_date",
    "ScheduledDeliveryDate": "scheduled_delivery_date",
    "PickupDate": "pickup_date",
    "PickupLate": "pickup_late",
    "DeliveredDate": "delivered_date",
    "CarrierAssignDate": "carrier_assign_date",
    "VoidDate": "void_date",
    "BusinessUnitCode": "business_unit_code",
    "SourceSystemName": "source_system_name",
    "FreightOptimizerCustomerCode": "freight_optimizer_customer_code",
    "CarrierCode": "carrier_code",
    "EquipmentCode": "equipment_code",
    "OrderStatusTypeCode": "order_status_type_code",
    "IsOnHold": "is_on_hold",
    "IsUncommited": "is_uncommited",
    "TripCount": "trip_count",
    "OrderSalesAmount": "order_sales_amount",
    "FuelChargeAmount": "fuel_charge_amount",
    "LineHaulChargeAmount": "line_haul_charge_amount",
    "OrderCost": "order_cost",
    "CrossBooked": "cross_booked",
    "BounceToBounce": "bounce_to_bounce",
    "OnTimeDelivery": "on_time_delivery",
    "DeliveryDelay": "delivery_delay",
    "OnTimePickup": "on_time_pickup",
    "PickupDelay": "pickup_delay",
    "BillDistance": "bill_distance",
    "WeightInPounds": "weight_in_pounds",
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
    "CreatedByUser": "created_by_user",
    "IsHazmat": "is_hazmat",
    "Quantity": "quantity",
    "PickupRescheduledReason": "pickup_rescheduled_reason",
    "DropRescheduledReason": "drop_rescheduled_reason",
    "CarrierPortalPostingDate": "carrier_portal_posting_date",
    "IsDropAndHook": "is_drop_and_hook",
    "PercentageOfCommissionPrimary": "percentage_of_commission_primary",
    "DeliveryArrivalDate": "delivery_arrival_date",
    "OrderSourceCode": "order_source_code",
    "CompanyID": "company_id",
    "ValueOfGoods": "value_of_goods",
    "BazookaOrderId": "bazooka_order_id",
    "ShipmentID": "shipment_id",
    "PickupDepartureDate": "pickup_departure_date",
}

# Tipos esperados por campo
DECIMAL_FIELDS = {
    "order_sales_amount",
    "fuel_charge_amount",
    "line_haul_charge_amount",
    "order_cost",
    "fuel_pay_amount",
    "line_haul_pay_amount",
    "total_accessorial_charge",
    "detention_charge",
    "tonu_charge",
    "lh_rate",
    "bill_distance",
    "weight_in_pounds",
    "percentage_of_commission_primary",
    "value_of_goods",
}

INTEGER_FIELDS = {
    "order_key",
    "trip_count",
    "delivery_delay",
    "pickup_delay",
    "origin_location_id",
    "destination_location_id",
    "quantity",
    "pickup_service_failure_code_id",
    "delivery_service_failure_code_id",
    "pickup_service_failure_reason_category_id",
    "delivery_service_failure_reason_category_id",
    "company_id",
    "bazooka_order_id",
    "trip_id",
    "trip_brokerage_status_id",
    "carrier_id",
}

BOOLEAN_FIELDS = {
    "is_hazmat",
    "is_drop_and_hook",
}

ZERO_ONE_BOOLEAN_FIELDS = {
    "cross_booked",
    "bounce_to_bounce",
    "on_time_delivery",
    "on_time_pickup",
}

DATETIME_FIELDS = {
    "order_date",
    "scheduled_pickup_date",
    "scheduled_delivery_date",
    "pickup_date",
    "pickup_departure_date",
    "delivered_date",
    "delivery_arrival_date",
    "carrier_assign_date",
    "void_date",
    "pickup_late",
    "carrier_portal_posting_date",
    "origin_scheduled_departure_time",
    "destination_stop_scheduled_departure_time",
}

# ============================================================
# HELPERS
# ============================================================

def pascal_to_snake(name: str) -> str:
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    s2 = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1)
    return s2.lower()

def map_sql_column_to_json_column(sql_col: str) -> str:
    return SPECIAL_COLUMN_MAP.get(sql_col, pascal_to_snake(sql_col))

def normalize_null(value):
    if value is None:
        return None
    if isinstance(value, str):
        v = value.strip()
        if v == "":
            return None
        if v.upper() == "NULL":
            return None
        if v.lower() == "null":
            return None
    return value

def parse_bool(value):
    value = normalize_null(value)
    if value is None:
        return None

    if isinstance(value, bool):
        return value

    if isinstance(value, (int, float)):
        return bool(int(value))

    v = str(value).strip().lower()
    if v in {"1", "true", "t", "yes", "y"}:
        return True
    if v in {"0", "false", "f", "no", "n"}:
        return False

    return value

def parse_zero_one(value):
    value = normalize_null(value)
    if value is None:
        return None

    if isinstance(value, bool):
        return 1 if value else 0

    if isinstance(value, int):
        return value

    if isinstance(value, float):
        return int(value)

    v = str(value).strip().lower()
    if v in {"true", "t", "yes", "y"}:
        return 1
    if v in {"false", "f", "no", "n"}:
        return 0
    try:
        return int(Decimal(v))
    except Exception:
        return value

def round_up_2_decimals(value):
    value = normalize_null(value)
    if value is None:
        return None

    try:
        d = Decimal(str(value).strip())
        return d.quantize(Decimal("0.01"), rounding=ROUND_CEILING)
    except (InvalidOperation, ValueError):
        return value

def parse_int(value):
    value = normalize_null(value)
    if value is None:
        return None
    try:
        return int(Decimal(str(value).strip()))
    except Exception:
        return value

def parse_datetime_like(value):
    value = normalize_null(value)
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")

    v = str(value).strip()

    # Soporta:
    # 2026-01-26 12:41:31.64
    # 2026-01-01T17:10:02.920000
    # 2026-01-02T13:00:00
    # 2026-01-02 13:00:00.00
    v = v.replace("T", " ")

    # Quita Z final si existe
    if v.endswith("Z"):
        v = v[:-1]

    # Intento robusto con fromisoformat
    try:
        dt = datetime.fromisoformat(v)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        pass

    # Intentos manuales
    fmts = [
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
    ]
    for fmt in fmts:
        try:
            dt = datetime.strptime(v, fmt)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            continue

    return v

def normalize_value(field_name: str, value):
    value = normalize_null(value)

    if field_name in DATETIME_FIELDS:
        return parse_datetime_like(value)

    if field_name in BOOLEAN_FIELDS:
        return parse_bool(value)

    if field_name in ZERO_ONE_BOOLEAN_FIELDS:
        return parse_zero_one(value)

    if field_name in INTEGER_FIELDS:
        return parse_int(value)

    if field_name in DECIMAL_FIELDS:
        return round_up_2_decimals(value)

    # Normalización genérica de strings
    if isinstance(value, str):
        return value.strip()

    return value

def values_equal(a, b):
    return a == b

# ============================================================
# LOADERS
# ============================================================

def load_sqlserver_tsv(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"No existe el TSV: {path}")

    rows = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            clean_row = {}
            for sql_col, val in row.items():
                json_col = map_sql_column_to_json_column(sql_col)
                clean_row[json_col] = val
            rows.append(clean_row)
    return rows

def load_json_or_jsonl(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"No existe el JSON/JSONL: {path}")

    text = path.read_text(encoding="utf-8-sig").strip()
    if not text:
        return []

    # Si viene como array JSON completo
    if text.startswith("["):
        data = json.loads(text)
        if not isinstance(data, list):
            raise ValueError("El archivo JSON no contiene una lista.")
        return data

    # Si viene en formato JSONL
    rows = []
    with path.open("r", encoding="utf-8-sig") as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as e:
                raise ValueError(f"Error parseando JSONL en línea {line_num}: {e}") from e
    return rows

# ============================================================
# COMPARISON
# ============================================================

def index_by_order_key(rows, source_name):
    idx = {}
    duplicates = []

    for row in rows:
        raw_key = row.get("order_key")
        key = parse_int(raw_key)

        if key is None:
            continue

        if key in idx:
            duplicates.append(key)

        idx[key] = row

    if duplicates:
        print(f"[WARN] {source_name}: order_key duplicados encontrados: {sorted(set(duplicates))[:20]}")
    return idx

def compare_rows(sql_row, json_row):
    all_fields = sorted(set(sql_row.keys()) | set(json_row.keys()))
    diffs = []

    for field in all_fields:
        sql_val = normalize_value(field, sql_row.get(field))
        json_val = normalize_value(field, json_row.get(field))

        if not values_equal(sql_val, json_val):
            diffs.append({
                "field": field,
                "sql_value": sql_val,
                "json_value": json_val,
            })

    return diffs

def main():
    print("=" * 100)
    print("COMPARADOR SQL SERVER TSV vs BIGQUERY JSON/JSONL POR ORDER_KEY")
    print("=" * 100)
    print(f"TSV : {SQLSERVER_TSV_PATH}")
    print(f"JSON: {JSON_PATH}")
    print()

    sql_rows = load_sqlserver_tsv(SQLSERVER_TSV_PATH)
    json_rows = load_json_or_jsonl(JSON_PATH)

    print(f"Filas SQL TSV cargadas : {len(sql_rows)}")
    print(f"Filas JSON cargadas    : {len(json_rows)}")
    print()

    sql_idx = index_by_order_key(sql_rows, "SQL TSV")
    json_idx = index_by_order_key(json_rows, "JSON")

    sql_keys = set(sql_idx.keys())
    json_keys = set(json_idx.keys())

    common_keys = sorted(sql_keys & json_keys)
    only_sql = sorted(sql_keys - json_keys)
    only_json = sorted(json_keys - sql_keys)

    print(f"Order keys en ambos         : {len(common_keys)}")
    print(f"Order keys solo en SQL TSV  : {len(only_sql)}")
    print(f"Order keys solo en JSON     : {len(only_json)}")
    print()

    if only_sql:
        print("Muestra order_key solo en SQL:")
        print(only_sql[:20])
        print()

    if only_json:
        print("Muestra order_key solo en JSON:")
        print(only_json[:20])
        print()

    match_count = 0
    mismatch_count = 0
    mismatch_counter = Counter()
    mismatch_samples = []

    for key in common_keys:
        diffs = compare_rows(sql_idx[key], json_idx[key])

        if not diffs:
            match_count += 1
        else:
            mismatch_count += 1
            for d in diffs:
                mismatch_counter[d["field"]] += 1

            mismatch_samples.append({
                "order_key": key,
                "diff_count": len(diffs),
                "diffs": diffs,
            })

    print("=" * 100)
    print("RESUMEN")
    print("=" * 100)
    print(f"Total comparados con misma key : {len(common_keys)}")
    print(f"Rows MATCH                     : {match_count}")
    print(f"Rows MISMATCH                  : {mismatch_count}")
    print()

    if common_keys:
        pct_match = (match_count / len(common_keys)) * 100
        pct_mismatch = (mismatch_count / len(common_keys)) * 100
        print(f"% MATCH    : {pct_match:.2f}%")
        print(f"% MISMATCH : {pct_mismatch:.2f}%")
        print()

    print("=" * 100)
    print(f"TOP {TOP_MISMATCH_COLUMNS} COLUMNAS CON MÁS MISMATCH")
    print("=" * 100)
    if mismatch_counter:
        for col, cnt in mismatch_counter.most_common(TOP_MISMATCH_COLUMNS):
            print(f"{col}: {cnt}")
    else:
        print("No hay mismatches por columna.")
    print()

    print("=" * 100)
    print(f"MUESTRA DE {min(SAMPLE_MISMATCH_ROWS, len(mismatch_samples))} FILAS CON MISMATCH")
    print("=" * 100)

    # Ordena por más diferencias primero
    mismatch_samples.sort(key=lambda x: x["diff_count"], reverse=True)

    if mismatch_samples:
        for sample in mismatch_samples[:SAMPLE_MISMATCH_ROWS]:
            print(f"\norder_key = {sample['order_key']} | diff_count = {sample['diff_count']}")
            print("-" * 80)
            for d in sample["diffs"][:50]:
                print(
                    f"field={d['field']} | SQL={repr(d['sql_value'])} | JSON={repr(d['json_value'])}"
                )
            if len(sample["diffs"]) > 50:
                print(f"... y {len(sample['diffs']) - 50} diferencias más")
    else:
        print("No hay filas con mismatch.")
    print()

    print("=" * 100)
    print("FIN")
    print("=" * 100)

if __name__ == "__main__":
    main()

Unas notas directas para que no te falle:
	1.	El TSV debe tener como primera fila exactamente el header.
	2.	El JSON puede venir en dos formatos y este script soporta ambos:
	•	un arreglo completo: [ {...}, {...} ]
	•	jsonl real: una línea por objeto.
	3.	El script compara por order_key, que en el TSV sale desde OrderKey.
	4.	Los decimales se redondean a 2 decimales con ROUND_CEILING, o sea hacia arriba.
	5.	Las fechas se normalizan a YYYY-MM-DD HH:MM:SS, así evitas ruido por milisegundos.
	6.	Los NULL, null, vacío y None se tratan como nulos.
	7.	Los booleanos 0/1, true/false, Y/N se normalizan según el campo.

Si quieres, en el siguiente paso te lo dejo además en versión que exporta el detalle de mismatches a CSV.

