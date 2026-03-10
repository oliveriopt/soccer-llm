CREATE OR REPLACE PROCEDURE `sqlserver_to_bq_silver.sp_load_orders`(
  p_lookback_days INT64
)
BEGIN
  DECLARE v_lookback_days INT64 DEFAULT IFNULL(p_lookback_days, 90);
  DECLARE start_dt DATETIME DEFAULT DATETIME_SUB(CURRENT_DATETIME(), INTERVAL v_lookback_days DAY);
  DECLARE end_dt   DATETIME DEFAULT CURRENT_DATETIME();

  -------------------------------------------------------------------
  -- 1) ORDER IDS
  -------------------------------------------------------------------
  CREATE TEMP TABLE order_ids (
    order_id INT64 NOT NULL
  );

  IF p_lookback_days IS NULL THEN
    INSERT INTO order_ids(order_id)
    SELECT DISTINCT o.order_id
    FROM `sqlserver_to_bq_silver.xpomaster_orders_order` o
    WHERE o.created_date >= start_dt
      AND o.created_date < end_dt;
  ELSE
    INSERT INTO order_ids(order_id)
    SELECT DISTINCT o.order_id
    FROM `sqlserver_to_bq_silver.xpomaster_orders_order` o
    WHERE (o.created_date >= start_dt AND o.created_date < end_dt)
       OR (o.updated_date >= start_dt AND o.updated_date < end_dt);
  END IF;

  -------------------------------------------------------------------
  -- 2) BAZ SETTLED ORDER MIGRATION
  -------------------------------------------------------------------
  CREATE TABLE IF NOT EXISTS `sqlserver_to_bq_silver.baz_settled_order_migration` (
    order_id INT64
  );

  INSERT INTO `sqlserver_to_bq_silver.baz_settled_order_migration` (order_id)
  SELECT DISTINCT o.order_id
  FROM `sqlserver_to_bq_silver.xpomaster_orders_assignhistory` AS ah
  JOIN `sqlserver_to_bq_silver.xpomaster_orders_ordertrip` AS ot
    ON ah.trip_id = ot.trip_id
  JOIN `sqlserver_to_bq_silver.xpomaster_orders_order` AS o
    ON ot.order_id = o.order_id
  WHERE ah.created_by = 'BazDataMigration'
    AND ah.created_date >= start_dt
    AND NOT EXISTS (
      SELECT 1
      FROM `sqlserver_to_bq_silver.baz_settled_order_migration` AS m2
      WHERE m2.order_id = o.order_id
    );

  DELETE FROM order_ids
  WHERE order_id IN (
    SELECT order_id
    FROM `sqlserver_to_bq_silver.baz_settled_order_migration`
  );

  -------------------------------------------------------------------
  -- 3) ORDER AMOUNTS
  -------------------------------------------------------------------
  CREATE TEMP TABLE order_amounts AS
  WITH accessorial AS (
    SELECT
      order_id,
      SUM(amount) AS total_accessorial_charge
    FROM `sqlserver_to_bq_silver.fn_order_total_accessorial_charge`()
    GROUP BY order_id
  )
  SELECT
    o.order_id,
    IFNULL(ch.amount, 0) AS order_sales_amount,
    IFNULL(lh.amount, 0) AS line_haul_charge_amount,
    IFNULL(f.amount, 0) AS fuel_charge_amount,
    IFNULL(a.total_accessorial_charge, 0) AS total_accessorial_charge,
    IFNULL(d.amount, 0) AS detention_charge,
    IFNULL(t.amount, 0) AS tonu_charge,
    IFNULL(CAST(lhp.amount AS NUMERIC), NUMERIC '0') AS line_haul_pay_amount,
    ROUND(IFNULL(CAST(fp.amount AS NUMERIC), NUMERIC '0'), 2) AS fuel_pay_amount,
    ROUND(IFNULL(CAST(cp.amount AS NUMERIC), NUMERIC '0'), 2) AS order_cost,
    IFNULL(lh.lh_rate, 0) AS lh_rate
  FROM order_ids o
  LEFT JOIN `sqlserver_to_bq_silver.fn_order_total_charge`() ch
    ON ch.order_id = o.order_id
  LEFT JOIN `sqlserver_to_bq_silver.fn_order_total_linehaul_charge`() lh
    ON lh.order_id = o.order_id
  LEFT JOIN `sqlserver_to_bq_silver.fn_order_total_fuel_charge`() f
    ON f.order_id = o.order_id
  LEFT JOIN accessorial a
    ON a.order_id = o.order_id
  LEFT JOIN `sqlserver_to_bq_silver.fn_order_detention_charges`() d
    ON d.order_id = o.order_id
  LEFT JOIN `sqlserver_to_bq_silver.fn_order_tonu_charges`() t
    ON t.order_id = o.order_id
  LEFT JOIN `sqlserver_to_bq_silver.fn_order_total_linehaul_pay`() lhp
    ON lhp.order_id = o.order_id
  LEFT JOIN `sqlserver_to_bq_silver.fn_order_total_fuel_pay`() fp
    ON fp.order_id = o.order_id
  LEFT JOIN `sqlserver_to_bq_silver.fn_order_total_pay`() cp
    ON cp.order_id = o.order_id;

  -------------------------------------------------------------------
  -- 4) LOCATIONS
  -------------------------------------------------------------------
  CREATE TEMP TABLE locations AS
  SELECT
    o.order_id,
    lco.name AS origin_city,
    lso.state_code AS origin_state_code,
    lzo.zip_code AS origin_postal_code,
    lmao.country_code AS origin_country_code,
    oloc.location_id AS origin_location_id,
    lcd.name AS destination_city,
    lsd.state_code AS destination_state_code,
    lzd.zip_code AS destination_postal_code,
    lmad.country_code AS destination_country_code,
    dloc.location_id AS destination_location_id
  FROM `sqlserver_to_bq_silver.xpomaster_orders_order` AS o
  JOIN order_ids ord_is
    ON o.order_id = ord_is.order_id
  LEFT JOIN `sqlserver_to_bq_silver.xpomaster_orders_stop` AS origin_stop
    ON origin_stop.stop_id = o.origin_stop_id
  LEFT JOIN `sqlserver_to_bq_silver.xpomaster_orders_stop` AS destination_stop
    ON destination_stop.stop_id = o.destination_stop_id
  LEFT JOIN `sqlserver_to_bq_silver.xpomaster_locale_address` AS a
    ON a.address_id = origin_stop.address_id
  LEFT JOIN `sqlserver_to_bq_silver.xpomaster_locale_city` AS lco
    ON lco.city_id = a.city_id
  LEFT JOIN `sqlserver_to_bq_silver.xpomaster_locale_state` AS lso
    ON lso.state_id = a.state_id
  LEFT JOIN `sqlserver_to_bq_silver.xpomaster_locale_zip` AS lzo
    ON lzo.zip_id = a.zip_id
  LEFT JOIN `sqlserver_to_bq_silver.xpomaster_locale_country` AS lmao
    ON lmao.country_id = a.country_id
  LEFT JOIN `sqlserver_to_bq_silver.xpomaster_customer_location` AS oloc
    ON oloc.location_id = origin_stop.location_id
  LEFT JOIN `sqlserver_to_bq_silver.xpomaster_locale_address` AS a2
    ON a2.address_id = destination_stop.address_id
  LEFT JOIN `sqlserver_to_bq_silver.xpomaster_locale_city` AS lcd
    ON lcd.city_id = a2.city_id
  LEFT JOIN `sqlserver_to_bq_silver.xpomaster_locale_state` AS lsd
    ON lsd.state_id = a2.state_id
  LEFT JOIN `sqlserver_to_bq_silver.xpomaster_locale_zip` AS lzd
    ON lzd.zip_id = a2.zip_id
  LEFT JOIN `sqlserver_to_bq_silver.xpomaster_locale_country` AS lmad
    ON lmad.country_id = a2.country_id
  LEFT JOIN `sqlserver_to_bq_silver.xpomaster_customer_location` AS dloc
    ON dloc.location_id = destination_stop.location_id;

  -------------------------------------------------------------------
  -- 5) SERVICE FAILURE
  -------------------------------------------------------------------
  CREATE TEMP TABLE servicefailure AS
  SELECT
    sf.stop_id,
    sf.service_failure_code_id,
    sf.reason_category_id
  FROM `sqlserver_to_bq_silver.xpomaster_orders_servicefailure` AS sf
  WHERE sf.created_date >= start_dt
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY sf.stop_id
    ORDER BY sf.created_date ASC
  ) = 1;

  -------------------------------------------------------------------
  -- 6) RESCHEDULE REASON
  -------------------------------------------------------------------
  CREATE TEMP TABLE reschedulereason AS
  SELECT
    rt.stop_id,
    CASE
      WHEN reason.code IN ('RXO Operational/Sales Failure', 'Carrier assigned to load late') THEN 'RXO'
      WHEN reason.code IN ('Carrier Service Failure', 'Carrier late') THEN 'Carrier'
      WHEN reason.code IN ('Acceptable Reschedule (not a service failure)') THEN 'Customer'
    END AS reschedule_reason
  FROM `sqlserver_to_bq_silver.xpomaster_orders_rescheduledtrip` AS rt
  JOIN `sqlserver_to_bq_silver.xpomaster_dbo_rescheduledtripcodes` AS reason
    ON rt.reason_code_id = reason.id
  WHERE rt.created_date >= start_dt
    AND rt.created_date < end_dt
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY rt.stop_id
    ORDER BY rt.created_date ASC
  ) = 1;

  -------------------------------------------------------------------
  -- 7) COMMODITY QTY FILTERING
  -------------------------------------------------------------------
  CREATE TEMP TABLE commodity_qty AS
  SELECT
    com.order_id,
    SUM(com.quantity) AS quantity
  FROM `sqlserver_to_bq_silver.xpomaster_orders_ordercommodity` AS com
  JOIN order_ids o
    ON o.order_id = com.order_id
  GROUP BY com.order_id;

  -------------------------------------------------------------------
  -- 8) SALES REPS FILTERING
  -------------------------------------------------------------------
  CREATE TEMP TABLE inside_sales_primary AS
  SELECT
    osr.order_id,
    osr.salesperson_id,
    osr.percentage_of_commission
  FROM `sqlserver_to_bq_silver.xpomaster_orders_ordersalesrep` osr
  JOIN order_ids o
    ON o.order_id = osr.order_id
  WHERE osr.is_outside_sales = FALSE
    AND osr.is_primary = TRUE
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY osr.order_id
    ORDER BY osr.percentage_of_commission DESC
  ) = 1;

  CREATE TEMP TABLE inside_sales_secondary AS
  SELECT
    osr.order_id,
    osr.salesperson_id
  FROM `sqlserver_to_bq_silver.xpomaster_orders_ordersalesrep` osr
  JOIN order_ids o
    ON o.order_id = osr.order_id
  WHERE osr.is_outside_sales = FALSE
    AND osr.is_primary = FALSE
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY osr.order_id
    ORDER BY osr.percentage_of_commission DESC
  ) = 1;

  -------------------------------------------------------------------
  -- 9) CTE_ORDERS TEMP TABLE
  -------------------------------------------------------------------
  CREATE TEMP TABLE cte_orders AS
  SELECT
    CASE WHEN o.is_on_hold THEN 'Y' ELSE 'N' END AS is_on_hold,
    CASE WHEN o.is_uncommited THEN 'Y' ELSE 'N' END AS is_uncommited,
    o.created_date,
    o.order_id,
    CAST(o.code AS STRING) AS code,
    CASE WHEN o.bill_distance < 100000 THEN o.bill_distance END AS bill_distance,
    ROUND(SAFE_CAST(o.weight AS NUMERIC), 2) AS weight,
    o.created_by,
    o.is_hazmat,
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
  FROM `sqlserver_to_bq_silver.xpomaster_orders_order` AS o
  JOIN order_ids ord_is
    ON o.order_id = ord_is.order_id
  WHERE o.order_status_type_id IN (1, 2, 3, 4, 5);

  -------------------------------------------------------------------
  -- 10) SOURCE FINAL
  -------------------------------------------------------------------
  CREATE TEMP TABLE tmp_src AS
  SELECT
    'Freight Optimizer' AS source_system_name,
    src.code AS order_source_code,
    TRIM(rc.code) AS business_unit_code,
    SUBSTR(CAST(st.code AS STRING), 1, 20) AS service_type_code,

    ost.code AS order_status_type_code,
    bstatus.code AS brokerage_status_code,
    pickupreason.reschedule_reason AS pickup_rescheduled_reason,
    dropreason.reschedule_reason AS drop_rescheduled_reason,
    o.is_on_hold AS is_on_hold,
    o.is_uncommited AS is_uncommited,
    finance_status.code AS order_financial_status_type_code,

    o.created_date AS order_date,
    origin_stop.scheduled_early_arrival_time AS scheduled_pickup_date,
    IFNULL(destination_stop.scheduled_late_arrival_time, destination_stop.scheduled_early_arrival_time) AS scheduled_delivery_date,
    origin_stop.actual_arrival_time AS pickup_date,
    origin_stop.actual_departure_time AS pickup_departure_date,
    destination_stop.actual_departure_time AS delivered_date,
    destination_stop.actual_arrival_time AS delivery_arrival_date,
    carrier_assign.carrier_assign_date AS carrier_assign_date,
    CAST(voided.void_date AS TIMESTAMP) AS void_date,

    SUBSTR(CAST(cust.code AS STRING), 1, 20) AS freight_optimizer_customer_code,
    SUBSTR(CAST(car.code AS STRING), 1, 15) AS carrier_code,
    TRIM(trailer_equipment.code) AS equipment_code,
    o.order_id AS order_key,
    SUBSTR(CAST(o.code AS STRING), 1, 20) AS order_number,

    amt.order_sales_amount AS order_sales_amount,
    amt.line_haul_charge_amount AS line_haul_charge_amount,
    amt.fuel_charge_amount AS fuel_charge_amount,
    amt.total_accessorial_charge AS total_accessorial_charge,
    amt.detention_charge AS detention_charge,
    amt.tonu_charge AS tonu_charge,
    amt.line_haul_pay_amount AS line_haul_pay_amount,
    amt.fuel_pay_amount AS fuel_pay_amount,
    amt.order_cost AS order_cost,
    amt.lh_rate AS lh_rate,

    0 AS trip_count,
    o.bill_distance AS bill_distance,
    ROUND(SAFE_CAST(o.weight AS NUMERIC), 2) AS weight_in_pounds,

    CASE
      WHEN destination_stop.actual_arrival_time >
           IFNULL(destination_stop.scheduled_late_arrival_time, destination_stop.scheduled_early_arrival_time) THEN 0
      WHEN destination_stop.actual_arrival_time <=
           IFNULL(destination_stop.scheduled_late_arrival_time, destination_stop.scheduled_early_arrival_time) THEN 1
      ELSE NULL
    END AS on_time_delivery,

    CASE
      WHEN destination_stop.actual_arrival_time >
           IFNULL(destination_stop.scheduled_late_arrival_time, destination_stop.scheduled_early_arrival_time)
      THEN TIMESTAMP_DIFF(
             destination_stop.actual_arrival_time,
             IFNULL(destination_stop.scheduled_late_arrival_time, destination_stop.scheduled_early_arrival_time),
             MINUTE
           )
      ELSE NULL
    END AS delivery_delay,

    CASE
      WHEN origin_stop.actual_arrival_time >
           IFNULL(origin_stop.scheduled_late_arrival_time, origin_stop.scheduled_early_arrival_time) THEN 0
      WHEN origin_stop.actual_arrival_time <=
           IFNULL(origin_stop.scheduled_late_arrival_time, origin_stop.scheduled_early_arrival_time) THEN 1
      ELSE NULL
    END AS on_time_pickup,

    CASE
      WHEN origin_stop.actual_arrival_time >
           IFNULL(origin_stop.scheduled_late_arrival_time, origin_stop.scheduled_early_arrival_time)
      THEN TIMESTAMP_DIFF(
             origin_stop.actual_arrival_time,
             IFNULL(origin_stop.scheduled_late_arrival_time, origin_stop.scheduled_early_arrival_time),
             MINUTE
           )
      ELSE NULL
    END AS pickup_delay,

    loc.origin_city,
    loc.origin_state_code,
    loc.origin_postal_code,
    loc.origin_country_code,
    loc.origin_location_id,

    loc.destination_city,
    loc.destination_state_code,
    loc.destination_postal_code,
    loc.destination_country_code,
    loc.destination_location_id,

    SUBSTR(CAST(entered_by_salesperson.code AS STRING), 1, 20) AS entered_by_user_name,
    SUBSTR(CAST(primary_salesperson.code AS STRING), 1, 20) AS primary_sales_rep_user_name,
    SUBSTR(CAST(secondary_salesperson.code AS STRING), 1, 20) AS secondary_sales_rep_user_name,

    SUBSTR(CAST(IFNULL(booked_by_salesperson.code, booked_by_user.user_name) AS STRING), 1, 20) AS booked_by_user_name,
    SUBSTR(CAST(IFNULL(primary_carrier_salesperson.code, primary_carrier_user.user_name) AS STRING), 1, 20) AS primary_carrier_rep_user_name,
    SUBSTR(CAST(freight_assign_salesperson.code AS STRING), 1, 20) AS freight_assign_rep_user_name,
    TRIM(IFNULL(dispatcher_office_first.code, dispatcher_office_first2.code)) AS dispatch_office_location_code,

    CASE
      WHEN dispatcher_office_first.code IN ('XPOSPA')
           AND ol.company_code NOT IN ('COY MEX', 'COY US') THEN 1
      WHEN dispatcher_office_first.company_code IN ('COY MEX', 'COY US')
           AND ol.company_code NOT IN ('COY MEX', 'COY US') THEN 1
      WHEN dispatcher_office_first.code != ol.code
           AND dispatcher_office_first.company_code NOT IN ('COY MEX', 'COY US') THEN 1
      ELSE 0
    END AS cross_booked,

    0 AS bounce_to_bounce,
    o.created_by AS created_by_user,
    o.is_hazmat AS is_hazmat,
    comm.quantity AS quantity,
    origin_stop.scheduled_late_arrival_time AS pickup_late,
    o.carrier_portal_posting_date AS carrier_portal_posting_date,
    o.is_drop_and_hook AS is_drop_and_hook,

    pickupservicefailure.service_failure_code_id AS pickup_service_failure_code_id,
    pickupservicefailure.reason_category_id AS pickup_service_failure_reason_category_id,
    destservicefailure.service_failure_code_id AS delivery_service_failure_code_id,
    destservicefailure.reason_category_id AS delivery_service_failure_reason_category_id,

    inside_sales_primary.percentage_of_commission AS percentage_of_commission_primary,
    rc.company_id AS company_id,
    o.value_of_goods AS value_of_goods,
    obaz.bazooka_order_id AS bazooka_order_id,
    o.shipment_id AS shipment_id,
    origin_stop.scheduled_departure_time AS origin_scheduled_departure_time,
    destination_stop.scheduled_departure_time AS destination_stop_scheduled_departure_time,
    trip.trip_id AS trip_id,
    trip.trip_brokerage_status_id AS trip_brokerage_status_id,
    trip.carrier_id AS carrier_id
  FROM cte_orders AS o
  JOIN order_amounts AS amt
    ON o.order_id = amt.order_id
  LEFT JOIN `sqlserver_to_bq_silver.xpomaster_orders_sourcetype` AS src
    ON o.source_type_id = src.source_type_id
  JOIN `sqlserver_to_bq_silver.xpomaster_customer_customer` AS cust
    ON cust.customer_id = o.customer_id
  LEFT JOIN `sqlserver_to_bq_silver.xpomaster_orders_stop` AS destination_stop
    ON destination_stop.stop_id = o.destination_stop_id
  LEFT JOIN `sqlserver_to_bq_silver.xpomaster_orders_stop` AS origin_stop
    ON origin_stop.stop_id = o.origin_stop_id
  LEFT JOIN `sqlserver_to_bq_silver.xpomaster_orders_trip` AS trip
    ON trip.trip_id = origin_stop.trip_id
  LEFT JOIN `sqlserver_to_bq_silver.xpomaster_orders_tripbrokeragestatustype` AS bstatus
    ON bstatus.trip_brokerage_status_type_id = trip.trip_brokerage_status_id
  LEFT JOIN `sqlserver_to_bq_silver.xpomaster_carrier_carrier` AS car
    ON car.carrier_id = trip.carrier_id
  LEFT JOIN `sqlserver_to_bq_silver.fn_order_void_date`() AS voided
    ON voided.order_id = o.order_id
  LEFT JOIN `sqlserver_to_bq_silver.xpomaster_dbo_reportingcode` AS rc
    ON rc.reporting_code_id = o.reporting_code_id
  LEFT JOIN `sqlserver_to_bq_silver.xpomaster_dbo_officelocation` AS ol
    ON rc.office_location_id = ol.office_location_id
  LEFT JOIN `sqlserver_to_bq_silver.xpomaster_orders_servicetype` AS st
    ON st.service_type_id = o.service_type_id
  JOIN `sqlserver_to_bq_silver.xpomaster_orders_orderstatustype` AS ost
    ON ost.order_status_type_id = o.order_status_type_id
  JOIN `sqlserver_to_bq_silver.xpomaster_accounting_financialstatustype` AS finance_status
    ON finance_status.financial_status_type_id = o.financial_status_type_id
  LEFT JOIN `sqlserver_to_bq_silver.xpomaster_dbo_equipmenttype` AS trailer_equipment
    ON trailer_equipment.equipment_type_id = o.requested_trailer_equipment_type_id
  LEFT JOIN inside_sales_primary
    ON inside_sales_primary.order_id = o.order_id
  LEFT JOIN inside_sales_secondary
    ON inside_sales_secondary.order_id = o.order_id
  LEFT JOIN `sqlserver_to_bq_silver.xpomaster_security_user` AS entered_by_user
    ON entered_by_user.user_name = o.created_by
  LEFT JOIN `sqlserver_to_bq_silver.xpomaster_security_salesperson` AS entered_by_salesperson
    ON entered_by_salesperson.user_id = entered_by_user.user_id
  LEFT JOIN `sqlserver_to_bq_silver.xpomaster_security_salesperson` AS primary_salesperson
    ON primary_salesperson.salesperson_id = inside_sales_primary.salesperson_id
  LEFT JOIN `sqlserver_to_bq_silver.xpomaster_security_salesperson` AS secondary_salesperson
    ON secondary_salesperson.salesperson_id = inside_sales_secondary.salesperson_id
  LEFT JOIN `sqlserver_to_bq_silver.xpomaster_security_salesperson` AS booked_by_salesperson
    ON booked_by_salesperson.user_id = trip.booked_by_user_id
  LEFT JOIN `sqlserver_to_bq_silver.xpomaster_security_user` AS booked_by_user
    ON booked_by_user.user_id = trip.booked_by_user_id
  LEFT JOIN `sqlserver_to_bq_silver.xpomaster_security_salesperson` AS primary_carrier_salesperson
    ON primary_carrier_salesperson.user_id = trip.dispatcher_user_id
  LEFT JOIN `sqlserver_to_bq_silver.xpomaster_security_user` AS primary_carrier_user
    ON primary_carrier_user.user_id = trip.dispatcher_user_id
  LEFT JOIN `sqlserver_to_bq_silver.xpomaster_dbo_officelocation` AS dispatcher_office_first
    ON dispatcher_office_first.office_location_id = primary_carrier_salesperson.office_location_id
  LEFT JOIN `sqlserver_to_bq_silver.xpomaster_dbo_officelocation` AS dispatcher_office_first2
    ON dispatcher_office_first2.office_location_id = primary_carrier_user.office_location_id
  LEFT JOIN `sqlserver_to_bq_silver.xpomaster_security_salesperson` AS freight_assign_salesperson
    ON freight_assign_salesperson.user_id = trip.freight_assigned_actual_carrier_rep_id
  LEFT JOIN `sqlserver_to_bq_silver.fn_trip_carrier_assign_date`() AS carrier_assign
    ON carrier_assign.trip_id = trip.trip_id
   AND carrier_assign.carrier_id = trip.carrier_id
  LEFT JOIN commodity_qty AS comm
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
  LEFT JOIN `sqlserver_to_bq_silver.xpomaster_orders_fobazookaordermapping` AS obaz
    ON o.order_id = obaz.order_id;

  -------------------------------------------------------------------
  -- 11) DUPLICATE VALIDATION
  -------------------------------------------------------------------
  ASSERT (
    SELECT COUNT(*) = 0
    FROM (
      SELECT order_key
      FROM tmp_src
      GROUP BY order_key
      HAVING COUNT(*) > 1
    )
  ) AS 'tmp_src contains duplicate order_key values';

  -------------------------------------------------------------------
  -- 12) CREATE TARGET IF NOT EXISTS
  -------------------------------------------------------------------
  EXECUTE IMMEDIATE """
    CREATE TABLE IF NOT EXISTS `sqlserver_to_bq_silver.stagingbrokerage_staging_orders`
    PARTITION BY DATE(order_date)
    CLUSTER BY order_key
    AS
    SELECT * FROM tmp_src WHERE 1 = 0
  """;

  -------------------------------------------------------------------
  -- 13) MERGE
  -------------------------------------------------------------------
  MERGE `sqlserver_to_bq_silver.stagingbrokerage_staging_orders` T
  USING tmp_src S
    ON T.order_key = S.order_key
  WHEN MATCHED THEN
    UPDATE SET
      source_system_name = S.source_system_name,
      order_source_code = S.order_source_code,
      business_unit_code = S.business_unit_code,
      service_type_code = S.service_type_code,
      order_status_type_code = S.order_status_type_code,
      brokerage_status_code = S.brokerage_status_code,
      pickup_rescheduled_reason = S.pickup_rescheduled_reason,
      drop_rescheduled_reason = S.drop_rescheduled_reason,
      is_on_hold = S.is_on_hold,
      is_uncommited = S.is_uncommited,
      order_financial_status_type_code = S.order_financial_status_type_code,
      order_date = S.order_date,
      scheduled_pickup_date = S.scheduled_pickup_date,
      scheduled_delivery_date = S.scheduled_delivery_date,
      pickup_date = S.pickup_date,
      pickup_departure_date = S.pickup_departure_date,
      delivered_date = S.delivered_date,
      delivery_arrival_date = S.delivery_arrival_date,
      carrier_assign_date = S.carrier_assign_date,
      void_date = S.void_date,
      freight_optimizer_customer_code = S.freight_optimizer_customer_code,
      carrier_code = S.carrier_code,
      equipment_code = S.equipment_code,
      order_number = S.order_number,
      order_sales_amount = S.order_sales_amount,
      line_haul_charge_amount = S.line_haul_charge_amount,
      fuel_charge_amount = S.fuel_charge_amount,
      total_accessorial_charge = S.total_accessorial_charge,
      detention_charge = S.detention_charge,
      tonu_charge = S.tonu_charge,
      line_haul_pay_amount = S.line_haul_pay_amount,
      fuel_pay_amount = S.fuel_pay_amount,
      order_cost = S.order_cost,
      lh_rate = S.lh_rate,
      trip_count = S.trip_count,
      bill_distance = S.bill_distance,
      weight_in_pounds = S.weight_in_pounds,
      on_time_delivery = S.on_time_delivery,
      delivery_delay = S.delivery_delay,
      on_time_pickup = S.on_time_pickup,
      pickup_delay = S.pickup_delay,
      origin_city = S.origin_city,
      origin_state_code = S.origin_state_code,
      origin_postal_code = S.origin_postal_code,
      origin_country_code = S.origin_country_code,
      origin_location_id = S.origin_location_id,
      destination_city = S.destination_city,
      destination_state_code = S.destination_state_code,
      destination_postal_code = S.destination_postal_code,
      destination_country_code = S.destination_country_code,
      destination_location_id = S.destination_location_id,
      entered_by_user_name = S.entered_by_user_name,
      primary_sales_rep_user_name = S.primary_sales_rep_user_name,
      secondary_sales_rep_user_name = S.secondary_sales_rep_user_name,
      booked_by_user_name = S.booked_by_user_name,
      primary_carrier_rep_user_name = S.primary_carrier_rep_user_name,
      freight_assign_rep_user_name = S.freight_assign_rep_user_name,
      dispatch_office_location_code = S.dispatch_office_location_code,
      cross_booked = S.cross_booked,
      bounce_to_bounce = S.bounce_to_bounce,
      created_by_user = S.created_by_user,
      is_hazmat = S.is_hazmat,
      quantity = S.quantity,
      pickup_late = S.pickup_late,
      carrier_portal_posting_date = S.carrier_portal_posting_date,
      is_drop_and_hook = S.is_drop_and_hook,
      pickup_service_failure_code_id = S.pickup_service_failure_code_id,
      pickup_service_failure_reason_category_id = S.pickup_service_failure_reason_category_id,
      delivery_service_failure_code_id = S.delivery_service_failure_code_id,
      delivery_service_failure_reason_category_id = S.delivery_service_failure_reason_category_id,
      percentage_of_commission_primary = S.percentage_of_commission_primary,
      company_id = S.company_id,
      value_of_goods = S.value_of_goods,
      bazooka_order_id = S.bazooka_order_id,
      shipment_id = S.shipment_id,
      origin_scheduled_departure_time = S.origin_scheduled_departure_time,
      destination_stop_scheduled_departure_time = S.destination_stop_scheduled_departure_time,
      trip_id = S.trip_id,
      trip_brokerage_status_id = S.trip_brokerage_status_id,
      carrier_id = S.carrier_id
  WHEN NOT MATCHED THEN
    INSERT (
      source_system_name,
      order_source_code,
      business_unit_code,
      service_type_code,
      order_status_type_code,
      brokerage_status_code,
      pickup_rescheduled_reason,
      drop_rescheduled_reason,
      is_on_hold,
      is_uncommited,
      order_financial_status_type_code,
      order_date,
      scheduled_pickup_date,
      scheduled_delivery_date,
      pickup_date,
      pickup_departure_date,
      delivered_date,
      delivery_arrival_date,
      carrier_assign_date,
      void_date,
      freight_optimizer_customer_code,
      carrier_code,
      equipment_code,
      order_key,
      order_number,
      order_sales_amount,
      line_haul_charge_amount,
      fuel_charge_amount,
      total_accessorial_charge,
      detention_charge,
      tonu_charge,
      line_haul_pay_amount,
      fuel_pay_amount,
      order_cost,
      lh_rate,
      trip_count,
      bill_distance,
      weight_in_pounds,
      on_time_delivery,
      delivery_delay,
      on_time_pickup,
      pickup_delay,
      origin_city,
      origin_state_code,
      origin_postal_code,
      origin_country_code,
      origin_location_id,
      destination_city,
      destination_state_code,
      destination_postal_code,
      destination_country_code,
      destination_location_id,
      entered_by_user_name,
      primary_sales_rep_user_name,
      secondary_sales_rep_user_name,
      booked_by_user_name,
      primary_carrier_rep_user_name,
      freight_assign_rep_user_name,
      dispatch_office_location_code,
      cross_booked,
      bounce_to_bounce,
      created_by_user,
      is_hazmat,
      quantity,
      pickup_late,
      carrier_portal_posting_date,
      is_drop_and_hook,
      pickup_service_failure_code_id,
      pickup_service_failure_reason_category_id,
      delivery_service_failure_code_id,
      delivery_service_failure_reason_category_id,
      percentage_of_commission_primary,
      company_id,
      value_of_goods,
      bazooka_order_id,
      shipment_id,
      origin_scheduled_departure_time,
      destination_stop_scheduled_departure_time,
      trip_id,
      trip_brokerage_status_id,
      carrier_id
    )
    VALUES (
      S.source_system_name,
      S.order_source_code,
      S.business_unit_code,
      S.service_type_code,
      S.order_status_type_code,
      S.brokerage_status_code,
      S.pickup_rescheduled_reason,
      S.drop_rescheduled_reason,
      S.is_on_hold,
      S.is_uncommited,
      S.order_financial_status_type_code,
      S.order_date,
      S.scheduled_pickup_date,
      S.scheduled_delivery_date,
      S.pickup_date,
      S.pickup_departure_date,
      S.delivered_date,
      S.delivery_arrival_date,
      S.carrier_assign_date,
      S.void_date,
      S.freight_optimizer_customer_code,
      S.carrier_code,
      S.equipment_code,
      S.order_key,
      S.order_number,
      S.order_sales_amount,
      S.line_haul_charge_amount,
      S.fuel_charge_amount,
      S.total_accessorial_charge,
      S.detention_charge,
      S.tonu_charge,
      S.line_haul_pay_amount,
      S.fuel_pay_amount,
      S.order_cost,
      S.lh_rate,
      S.trip_count,
      S.bill_distance,
      S.weight_in_pounds,
      S.on_time_delivery,
      S.delivery_delay,
      S.on_time_pickup,
      S.pickup_delay,
      S.origin_city,
      S.origin_state_code,
      S.origin_postal_code,
      S.origin_country_code,
      S.origin_location_id,
      S.destination_city,
      S.destination_state_code,
      S.destination_postal_code,
      S.destination_country_code,
      S.destination_location_id,
      S.entered_by_user_name,
      S.primary_sales_rep_user_name,
      S.secondary_sales_rep_user_name,
      S.booked_by_user_name,
      S.primary_carrier_rep_user_name,
      S.freight_assign_rep_user_name,
      S.dispatch_office_location_code,
      S.cross_booked,
      S.bounce_to_bounce,
      S.created_by_user,
      S.is_hazmat,
      S.quantity,
      S.pickup_late,
      S.carrier_portal_posting_date,
      S.is_drop_and_hook,
      S.pickup_service_failure_code_id,
      S.pickup_service_failure_reason_category_id,
      S.delivery_service_failure_code_id,
      S.delivery_service_failure_reason_category_id,
      S.percentage_of_commission_primary,
      S.company_id,
      S.value_of_goods,
      S.bazooka_order_id,
      S.shipment_id,
      S.origin_scheduled_departure_time,
      S.destination_stop_scheduled_departure_time,
      S.trip_id,
      S.trip_brokerage_status_id,
      S.carrier_id
    );

END;
