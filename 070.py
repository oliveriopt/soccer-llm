Tengo esta query:
DECLARE start_dt DATETIME DEFAULT DATETIME(DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY));
DECLARE end_dt   DATETIME DEFAULT CURRENT_DATETIME();
SELECT start_dt, end_dt;

CREATE TEMP TABLE order_ids (
  order_id INT64 NOT NULL
);

INSERT INTO order_ids(order_id)    
SELECT DISTINCT order_id AS order_id
FROM (
      SELECT o.order_id
      FROM rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_orders_order o
      WHERE (o.created_date >= start_dt AND  o.created_date < end_dt)
      OR (o.updated_date >= start_dt AND o.updated_date < end_dt)
      LIMIT 10000
);

CREATE TEMP TABLE order_amounts (
  order_id INT64 NOT NULL,
  order_sales_amount NUMERIC,
  line_haul_charge_amount NUMERIC,
  fuel_charge_amount NUMERIC,
  total_accessorial_charge NUMERIC,
  detention_charge NUMERIC,
  tonu_charge NUMERIC,
  line_haul_pay_amount NUMERIC,
  fuel_pay_amount NUMERIC,
  order_cost NUMERIC,
  lh_rate NUMERIC
);

 INSERT INTO order_amounts(
  order_id,
  order_sales_amount,
  line_haul_charge_amount,
  fuel_charge_amount,
  total_accessorial_charge,
  detention_charge,
  tonu_charge,
  line_haul_pay_amount,
  fuel_pay_amount,
  order_cost,
  lh_rate
 )
SELECT
  o.order_id,

  -- total charges overall
  IFNULL((
    SELECT ch.amount
    FROM `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.fn_order_total_charge`() AS ch
    WHERE ch.order_id = o.order_id
  ), 0) AS order_sales_amount,

  -- total LH charges
  IFNULL((
    SELECT lh.amount
    FROM `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.fn_order_total_linehaul_charge`() AS lh
    WHERE lh.order_id = o.order_id
  ), 0) AS line_haul_charge_amount,

  -- total fuel charges
  IFNULL((
    SELECT f.amount
    FROM `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.fn_order_total_fuel_charge`() AS f
    WHERE f.order_id = o.order_id
  ), 0) AS fuel_charge_amount,

  -- total accessorial charges (SUM del conjunto)
  IFNULL((
    SELECT SUM(a.amount)
    FROM `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.fn_order_total_accessorial_charge`() AS a
    WHERE a.order_id = o.order_id
  ), 0) AS total_accessorial_charge,

  -- total detention
  IFNULL((
    SELECT d.amount
    FROM `rxo-dataeng-datalake-uat.sqlserver_to_bq_silver.fn_order_detention_charges`() AS d
    WHERE d.order_id = o.order_id
  ), 0) AS detention_charge,

  -- total TONU
  IFNULL((
    SELECT t.amount
    FROM `rxo-dataeng-datalake-uat.sqlserver_to_bq_silver.fn_order_tonu_charges`() AS t
    WHERE t.order_id = o.order_id
  ), 0) AS tonu_charge,

  -- payouts
  IFNULL((
  SELECT CAST(lhp.amount AS NUMERIC)
  FROM `rxo-dataeng-datalake-uat.sqlserver_to_bq_silver.fn_order_total_linehaul_pay`() AS lhp
  WHERE lhp.order_id = o.order_id
), NUMERIC '0') AS line_haul_pay_amount,

  ROUND(
  IFNULL((
    SELECT CAST(fp.amount AS NUMERIC)
    FROM `rxo-dataeng-datalake-uat.sqlserver_to_bq_silver.fn_order_total_fuel_pay`() AS fp
    WHERE fp.order_id = o.order_id
  ), NUMERIC '0'),
  2
) AS fuel_pay_amount,

  ROUND(
  IFNULL((
    SELECT CAST(cp.amount AS NUMERIC)
    FROM `rxo-dataeng-datalake-uat.sqlserver_to_bq_silver.fn_order_total_pay`() AS cp
    WHERE cp.order_id = o.order_id
  ), NUMERIC '0'),
  2
) AS order_cost,

  -- rate desde LH charge
  IFNULL((
    SELECT lh.lh_rate
    FROM `rxo-dataeng-datalake-uat.sqlserver_to_bq_silver.fn_order_total_linehaul_charge`() AS lh
    WHERE lh.order_id = o.order_id
  ), 0) AS lh_rate

FROM order_ids AS o;

 CREATE TABLE IF NOT EXISTS `rxo-dataeng-datalake-np.sqlserver_to_bq_silver.baz_settled_order_migration` (
   order_id INT64
 );


-- INSERT equivalente
INSERT INTO `rxo-dataeng-datalake-np.sqlserver_to_bq_silver.baz_settled_order_migration` (order_id)
SELECT DISTINCT
  o.order_id
FROM `rxo-dataeng-datalake-uat.sqlserver_to_bq_silver.xpomaster_orders_assignhistory` AS ah
JOIN `rxo-dataeng-datalake-uat.sqlserver_to_bq_silver.xpomaster_orders_ordertrip`    AS ot
  ON ah.trip_id = ot.trip_id
JOIN `rxo-dataeng-datalake-uat.sqlserver_to_bq_silver.xpomaster_orders_order`        AS o
  ON ot.order_id = o.order_id
WHERE ah.created_by = 'BazDataMigration'
  AND ah.created_date >= start_dt
  AND NOT EXISTS (
    SELECT 1
    FROM `rxo-dataeng-datalake-uat.sqlserver_to_bq_silver.baz_settled_order_migration` AS m2
    WHERE m2.order_id = o.order_id
  );

-- DELETE equivalente sobre la tabla/CTE de IDs
-- Asumo una TEMP TABLE/TABLE/CTE llamada order_ids con columna order_id
DELETE FROM order_ids
WHERE order_id IN (
  SELECT order_id
  FROM `rxo-dataeng-datalake-np.sqlserver_to_bq_silver.baz_settled_order_migration`
);


CREATE TEMP TABLE locations (
  order_id INT64 NOT NULL,
  origin_city STRING,
  origin_state_code STRING,
  origin_postal_code STRING,
  origin_country_code STRING,
  origin_location_id INT64,
  destination_city STRING,
  destination_state_code STRING,
  destination_postal_code STRING,
  destination_country_code STRING,
  destination_location_id INT64
);

INSERT INTO locations(order_id, origin_city, origin_state_code, origin_postal_code, origin_country_code, origin_location_id,destination_city,
  destination_state_code,
  destination_postal_code,
  destination_country_code,
  destination_location_id)
SELECT
  o.order_id,
  -- Origin
  lco.name       AS origin_city,
  lso.state_code AS origin_state_code,
  lzo.zip_code   AS origin_postal_code,
  lmao.country_code AS origin_country_code,
  oloc.location_id  AS origin_location_id,

  -- Destination
  lcd.name       AS destination_city,
  lsd.state_code AS destination_state_code,
  lzd.zip_code   AS destination_postal_code,
  lmad.country_code AS destination_country_code,
  dloc.location_id  AS destination_location_id

FROM `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_orders_order` AS o
JOIN order_ids AS ord_is
  ON o.order_id = ord_is.order_id

LEFT JOIN `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_orders_stop` AS origin_stop
  ON origin_stop.stop_id = o.origin_stop_id

LEFT JOIN `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_orders_stop` AS destination_stop
  ON destination_stop.stop_id = o.destination_stop_id

-- Origin
LEFT JOIN `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_locale_address` AS a
  ON a.address_id = origin_stop.address_id
LEFT JOIN `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_locale_city` AS lco
  ON lco.city_id = a.city_id
LEFT JOIN `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_locale_state` AS lso
  ON lso.state_id = a.state_id
LEFT JOIN `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_locale_zip` AS lzo
  ON lzo.zip_id = a.zip_id
LEFT JOIN `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_locale_country` AS lmao
  ON lmao.country_id = a.country_id
LEFT JOIN `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_customer_location` AS oloc
  ON oloc.location_id = origin_stop.location_id

-- Destination
LEFT JOIN `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_locale_address` AS a2
  ON a2.address_id = destination_stop.address_id
LEFT JOIN `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_locale_city` AS lcd
  ON lcd.city_id = a2.city_id
LEFT JOIN `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_locale_state` AS lsd
  ON lsd.state_id = a2.state_id
LEFT JOIN `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_locale_zip` AS lzd
  ON lzd.zip_id = a2.zip_id
LEFT JOIN `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_locale_country` AS lmad
  ON lmad.country_id = a2.country_id
LEFT JOIN `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_customer_location` AS dloc
  ON dloc.location_id = destination_stop.location_id;


CREATE TEMP TABLE servicefailure (
  stop_id INT64 NOT NULL,
  service_failure_code_id INT64,
  reason_category_id INT64
);

INSERT INTO servicefailure  (stop_id, service_failure_code_id,reason_category_id)
SELECT
  sf.stop_id,
  sf.service_failure_code_id,
  sf.reason_category_id
FROM `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_orders_servicefailure` AS sf
WHERE sf.created_date >= start_dt   
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY sf.stop_id
  ORDER BY sf.created_date ASC
) = 1;


CREATE TEMP TABLE reschedulereason (
  stop_id INT64 NOT NULL,
  reschedule_reason STRING
);

INSERT INTO reschedulereason (stop_id, reschedule_reason)
SELECT
  rt.stop_id AS stop_id,
  CASE
    WHEN reason.Code IN ('RXO Operational/Sales Failure', 'Carrier assigned to load late') THEN 'RXO'
    WHEN reason.Code IN ('Carrier Service Failure', 'Carrier late') THEN 'Carrier'
    WHEN reason.Code IN ('Acceptable Reschedule (not a service failure)') THEN 'Customer'
  END AS reschedule_reason
FROM `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_orders_rescheduledtrip` AS rt
JOIN `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_dbo_rescheduledtripcodes` AS reason
  ON rt.reason_code_id = reason.id
WHERE rt.created_date >= start_dt AND rt.created_date < end_dt  -- parámetro de consulta
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY rt.stop_id
  ORDER BY rt.created_date ASC
) = 1;


WITH cte_orders AS (
  SELECT
    CASE WHEN o.is_on_hold THEN 'Y' ELSE 'N' END AS is_on_hold,
    CASE WHEN o.is_uncommited THEN 'Y' ELSE 'N' END AS is_uncommited,
    o.created_date AS created_date,
    o.order_id     AS order_id,
    CAST(o.code AS STRING) AS code,  -- si quieres limitar a 20: SUBSTR(CAST(o.code AS STRING), 1, 20)
    CASE WHEN o.bill_distance < 100000 THEN o.bill_distance END AS bill_distance,
    -- TRY_CAST(... AS DECIMAL(9,2)) -> en BQ: SAFE_CAST y redondeo
    ROUND(SAFE_CAST(o.weight AS NUMERIC), 2) AS weight,
    o.created_by,
    o.is_hazmat AS is_hazmat,
    o.customer_id,
    o.reporting_code_id,
    o.service_type_id,
    o.order_status_type_id,
    o.financial_status_type_id,
    o.requested_trailer_equipment_type_id,
    o.origin_stop_id,
    o.destination_stop_id,
    o.carrier_portal_posting_date,
    o.is_drop_and_hook,
    o.source_type_id,
    o.value_of_goods,
    o.shipment_id
  FROM `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_orders_order` AS o
  JOIN order_ids AS ord_is
    ON o.order_id = ord_is.order_id
  WHERE o.order_status_type_id IN (1, 2, 3, 4, 5)
)
SELECT
  -- Provenance
  'Freight Optimizer'                                            AS source_system_name,
  src.code                                                       AS order_source_code,
  TRIM(rc.code)                                                  AS business_unit_code,              -- RTRIM(LTRIM(...))
  SUBSTR(CAST(st.code AS STRING), 1, 20)                         AS service_type_code,               -- CONVERT(VARCHAR(20), st.Code)
-- 
-- Status
   ost.code                                                       AS order_status_type_code,
   bstatus.code                                                   AS brokerage_status_code,
   pickupreason.reschedule_reason                                 AS pickup_rescheduled_reason,
   dropreason.reschedule_reason                                   AS drop_rescheduled_reason,
   o.is_on_hold                                                   AS is_on_hold,
   o.is_uncommited                                                AS is_uncommited,
   finance_status.code                                            AS order_financial_status_type_code,
-- 
-- -- Fechas relevantes
   o.created_date                                                 AS order_date,
   origin_stop.scheduled_early_arrival_time                       AS scheduled_pickup_date,
   IFNULL(destination_stop.scheduled_late_arrival_time,
        destination_stop.scheduled_early_arrival_time)            AS scheduled_delivery_date,
   origin_stop.actual_arrival_time                                AS pickup_date,
   origin_stop.actual_departure_time                              AS pickup_departure_date,
   destination_stop.actual_departure_time                         AS delivered_date,
   destination_stop.actual_arrival_time                           AS delivery_arrival_date,
   carrier_assign.carrier_assign_date                             AS carrier_assign_date,
   CAST(voided.void_date AS TIMESTAMP)                            AS void_date,                       -- CONVERT(DATETIME2(2), ...)
-- 
-- -- Códigos/identificadores
   SUBSTR(CAST(cust.code AS STRING), 1, 20)                       AS freight_optimizer_customer_code, -- CONVERT(VARCHAR(20), cust.Code)
   SUBSTR(CAST(car.code  AS STRING), 1, 15)                       AS carrier_code,                    -- CONVERT(VARCHAR(15), car.Code)
   TRIM(trailer_equipment.code)                                   AS equipment_code,
   o.order_id                                                     AS order_key,
   SUBSTR(CAST(o.code AS STRING), 1, 20)                          AS order_number,                    -- CONVERT(VARCHAR(20), o.Code)
-- 
-- -- Money (de tus tablas/CTEs previas, e.g., order_amounts)
   order_sales_amount                                             AS order_sales_amount,
   line_haul_charge_amount                                        AS line_haul_charge_amount,
   fuel_charge_amount                                             AS fuel_charge_amount,
   total_accessorial_charge                                       AS total_accessorial_charge,
   detention_charge                                               AS detention_charge,
   tonu_charge                                                    AS tonu_charge,
   line_haul_pay_amount                                           AS line_haul_pay_amount,
   fuel_pay_amount                                                AS fuel_pay_amount,
   order_cost                                                     AS order_cost,
-- 
-- -- Otros
   0                                                              AS trip_count,                     
   o.bill_distance                                                AS bill_distance,
   ROUND(SAFE_CAST(o.weight AS NUMERIC), 2)                       AS weight_in_pounds,                -- TRY_CAST(..., 9,2)
-- 
-- -- Timing: On-time y retrasos (usa TIMESTAMP_DIFF si tus columnas son TIMESTAMP;
-- -- si fueran DATETIME, cambia a DATETIME_DIFF)
 CASE
   WHEN destination_stop.actual_arrival_time >
        IFNULL(destination_stop.scheduled_late_arrival_time,
               destination_stop.scheduled_early_arrival_time)
     THEN 0
   WHEN destination_stop.actual_arrival_time <=
        IFNULL(destination_stop.scheduled_late_arrival_time,
               destination_stop.scheduled_early_arrival_time)
     THEN 1
   ELSE NULL
 END                                                            AS on_time_delivery,
-- 
 CASE
   WHEN destination_stop.actual_arrival_time >
        IFNULL(destination_stop.scheduled_late_arrival_time,
               destination_stop.scheduled_early_arrival_time)
     THEN TIMESTAMP_DIFF(
            destination_stop.actual_arrival_time,
            IFNULL(destination_stop.scheduled_late_arrival_time,
                   destination_stop.scheduled_early_arrival_time),
            MINUTE
          )
   ELSE NULL
 END                                                            AS delivery_delay,
-- 
   CASE
     WHEN origin_stop.actual_arrival_time >
          IFNULL(origin_stop.scheduled_late_arrival_time,
                 origin_stop.scheduled_early_arrival_time)
       THEN 0
     WHEN origin_stop.actual_arrival_time <=
          IFNULL(origin_stop.scheduled_late_arrival_time,
                 origin_stop.scheduled_early_arrival_time)
       THEN 1
     ELSE NULL
   END                                                            AS on_time_pickup,
   
   CASE
     WHEN origin_stop.actual_arrival_time >
          IFNULL(origin_stop.scheduled_late_arrival_time,
                 origin_stop.scheduled_early_arrival_time)
       THEN TIMESTAMP_DIFF(
              origin_stop.actual_arrival_time,
              IFNULL(origin_stop.scheduled_late_arrival_time,
                     origin_stop.scheduled_early_arrival_time),
              MINUTE
            )
     ELSE NULL
   END                                                            AS pickup_delay,
-- 
-- -- Origin (si vienes de tu TEMP/CTE `locations`, ya están como columnas)
   loc.origin_city                                                    AS origin_city,
   loc.origin_state_code                                              AS origin_state_code,
   loc.origin_postal_code                                             AS origin_postal_code,
   loc.origin_country_code                                            AS origin_country_code,
   loc.origin_location_id                                             AS origin_location_id,
-- 
-- -- Destination
   loc.destination_city                                               AS destination_city,
   loc.destination_state_code                                         AS destination_state_code,
   loc.destination_postal_code                                        AS destination_postal_code,
   loc.destination_country_code                                       AS destination_country_code,
   loc.destination_location_id                                        AS destination_location_id,
-- 
-- -- Sales Reps
   SUBSTR(CAST(entered_by_salesperson.code       AS STRING), 1, 20) AS entered_by_user_name,
   SUBSTR(CAST(primary_salesperson.code          AS STRING), 1, 20) AS primary_sales_rep_user_name,
   SUBSTR(CAST(secondary_salesperson.code        AS STRING), 1, 20) AS secondary_sales_rep_user_name,
-- 
-- -- Carrier Reps
   SUBSTR(CAST(IFNULL(booked_by_salesperson.code, booked_by_user.user_name) AS STRING), 1, 20)
                                                                AS booked_by_user_name,
   SUBSTR(CAST(IFNULL(primary_carrier_salesperson.code, primary_carrier_user.user_name) AS STRING), 1, 20)
                                                                  AS primary_carrier_rep_user_name,
 SUBSTR(CAST(freight_assign_salesperson.code    AS STRING), 1, 20)
                                                                AS freight_assign_rep_user_name,
 TRIM(IFNULL(dispatcher_office_first.code, dispatcher_office_first2.code))
                                                                AS dispatch_office_location_code,
-- 
 CASE
   WHEN dispatcher_office_first.code IN ('XPOSPA')
        AND ol.company_code NOT IN ('COY MEX', 'COY US')                THEN 1  -- LC
   WHEN dispatcher_office_first.company_code IN ('COY MEX', 'COY US')
        AND ol.company_code NOT IN ('COY MEX', 'COY US')                THEN 1  -- LC
   WHEN dispatcher_office_first.code != ol.code
        AND dispatcher_office_first.company_code NOT IN ('COY MEX', 'COY US')
                                                                       THEN 1  -- LR
   ELSE 0
 END                                                            AS cross_booked,
-- 
   0                                                              AS bounce_to_bounce,
   o.created_by                                                   AS created_by_user,
   lh_rate                                                        AS lh_rate,                -- de tus montos
   o.is_hazmat                                                    AS is_hazmat,
   comm.quantity                                                  AS quantity,
   origin_stop.scheduled_late_arrival_time                        AS pickup_late,
   o.carrier_portal_posting_date                                  AS carrier_portal_posting_date,
   o.is_drop_and_hook                                             AS is_drop_and_hook,
-- 
-- -- Service Failures (de tus TEMP/CTEs previas)
   pickupservicefailure.service_failure_code_id                   AS pickup_service_failure_code_id,
   pickupservicefailure.reason_category_id                        AS pickup_service_failure_reason_category_id,
   destservicefailure.service_failure_code_id                     AS delivery_service_failure_code_id,
   destservicefailure.reason_category_id                          AS delivery_service_failure_reason_category_id,
-- 
-- -- Otros
   inside_sales_primary.percentage_of_commission                  AS percentage_of_commission_primary,
   rc.company_id                                                  AS company_id,
   o.value_of_goods                                               AS value_of_goods,
   obaz.bazooka_order_id                                          AS bazooka_order_id,
   o.shipment_id                                                  AS shipment_id,
   origin_stop.scheduled_departure_time                           AS origin_scheduled_departure_time,
  destination_stop.scheduled_departure_time                      AS destination_stop_scheduled_departure_time


FROM
  cte_orders AS o
JOIN
  order_amounts AS amt
  ON o.order_id = amt.order_id
LEFT JOIN
  `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_orders_sourcetype` AS src
  ON o.source_type_id = src.source_type_id
JOIN `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_customer_customer` AS cust
  ON cust.customer_id = o.customer_id
LEFT JOIN `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_orders_stop` AS destination_stop
  ON destination_stop.stop_id = o.destination_stop_id
LEFT JOIN `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_orders_stop` AS origin_stop
  ON origin_stop.stop_id = o.origin_stop_id
LEFT JOIN `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_orders_trip` AS trip
  ON trip.trip_id = origin_stop.trip_id
LEFT JOIN `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_orders_tripbrokeragestatustype` AS bstatus
  ON bstatus.trip_brokerage_status_type_id = trip.trip_brokerage_status_id
LEFT JOIN `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_carrier_carrier` AS car
  ON car.carrier_id = trip.carrier_id
LEFT JOIN `rxo-dataeng-datalake-uat.sqlserver_to_bq_silver.fn_order_void_date`() AS voided
  ON voided.order_id = o.order_id
LEFT JOIN `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_dbo_reportingcode` AS rc
  ON rc.reporting_code_id = o.reporting_code_id
LEFT JOIN `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_dbo_officelocation` AS ol
  ON rc.office_location_id = ol.office_location_id
LEFT JOIN `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_orders_servicetype` AS st
  ON st.service_type_id = o.service_type_id
JOIN `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_orders_orderstatustype` AS ost
  ON ost.order_status_type_id = o.order_status_type_id
JOIN `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_accounting_financialstatustype` AS finance_status
  ON finance_status.financial_status_type_id = o.financial_status_type_id
LEFT JOIN `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_dbo_equipmenttype` AS trailer_equipment
  ON trailer_equipment.equipment_type_id = o.requested_trailer_equipment_type_id

LEFT JOIN (
  SELECT
    order_id,
    salesperson_id,
    percentage_of_commission
  FROM `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_orders_ordersalesrep`
  WHERE is_outside_sales = FALSE     -- Inside
    AND is_primary = TRUE            -- Primary
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_id
    ORDER BY percentage_of_commission DESC
  ) = 1
) AS inside_sales_primary
ON inside_sales_primary.order_id = o.order_id

  LEFT JOIN (
  SELECT
    order_id,
    salesperson_id
  FROM `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_orders_ordersalesrep`
  WHERE is_outside_sales = FALSE   -- Inside
    AND is_primary = FALSE         -- Secondary
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_id
    ORDER BY percentage_of_commission DESC
  ) = 1
) AS inside_sales_secondary
ON inside_sales_secondary.order_id = o.order_id

LEFT JOIN `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_security_user` AS entered_by_user
  ON entered_by_user.user_name = o.created_by
LEFT JOIN `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_security_salesperson` AS entered_by_salesperson
  ON entered_by_salesperson.user_id = entered_by_user.user_id
LEFT JOIN `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_security_salesperson` AS primary_salesperson
  ON primary_salesperson.salesperson_id = inside_sales_primary.salesperson_id

LEFT JOIN `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_security_salesperson` AS secondary_salesperson
  ON secondary_salesperson.salesperson_id = inside_sales_secondary.salesperson_id
LEFT JOIN `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_security_salesperson` AS booked_by_salesperson
  ON booked_by_salesperson.user_id = trip.booked_by_user_id
LEFT JOIN `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_security_user` AS booked_by_user
  ON booked_by_user.user_id = trip.booked_by_user_id
-- PrimaryCarrierRep
LEFT JOIN `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_security_salesperson` AS primary_carrier_salesperson
  ON primary_carrier_salesperson.user_id = trip.dispatcher_user_id
LEFT JOIN `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_security_user` AS primary_carrier_user
  ON primary_carrier_user.user_id = trip.dispatcher_user_id
LEFT JOIN `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_dbo_officelocation` AS dispatcher_office_first
  ON dispatcher_office_first.office_location_id = primary_carrier_salesperson.office_location_id
LEFT JOIN `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_dbo_officelocation` AS dispatcher_office_first2
  ON dispatcher_office_first2.office_location_id = primary_carrier_user.office_location_id

LEFT JOIN `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_security_salesperson` AS freight_assign_salesperson
  ON freight_assign_salesperson.user_id = trip.freight_assigned_actual_carrier_rep_id
 LEFT JOIN `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.fn_trip_carrier_assign_date`() AS carrier_assign
   ON carrier_assign.trip_id = trip.trip_id
  AND carrier_assign.carrier_id = trip.carrier_id  
LEFT JOIN (
  SELECT
    order_id,
    SUM(quantity) AS quantity
  FROM `rxo-dataeng-datalake-prd.sqlserver_to_bq_silver.xpomaster_orders_ordercommodity` AS com
  GROUP BY order_id
) AS comm
  ON o.order_id = comm.order_id
LEFT JOIN locations AS loc
  ON loc.order_id = o.order_id
LEFT JOIN reschedulereason AS pickupreason
  ON pickupreason.stop_id = o.origin_stop_id

LEFT JOIN reschedulereason AS dropreason
  ON dropreason.stop_id = o.destination_stop_id

LEFT JOIN servicefailure AS pickupservicefailure
  ON o.origin_stop_id = pickupservicefailure.stop_id

LEFT JOIN servicefailure AS destservicefailure
  ON o.destination_stop_id = destservicefailure.stop_id

LEFT JOIN `rxo-dataeng-datalake-uat.sqlserver_to_bq_silver.xpomaster_orders_fobazookaordermapping` AS obaz
  ON o.order_id = obaz.order_id


Quiero convertirla en una store procedure. ANtes de hacer nada, dame los pasos para mejorar performance. Tambien quiero meter un parametro si el dia es NULL entonces es full load de 90 dias, si no es null es incremental del valor que este.
