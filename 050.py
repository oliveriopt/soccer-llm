tengo este sp:

CREATE OR REPLACE PROCEDURE `sqlserver_to_bq_silver.sp_load_orders`(lookback_days INT64)
BEGIN

  DECLARE start_dt DATETIME DEFAULT DATETIME(DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY));

  DECLARE target_dataset STRING DEFAULT 'sqlserver_to_bq_silver';
  DECLARE target_table   STRING DEFAULT 'stagingbrokerage_staging_orders';
  DECLARE target_fqtn    STRING DEFAULT FORMAT('`%s.%s`', target_dataset, target_table);

  -- SQL Server equivalent: FreightBrokerage.dbo.BAZSettledOrderMigration
  DECLARE baz_table STRING DEFAULT 'baz_settled_order_migration';
  DECLARE baz_fqtn  STRING DEFAULT FORMAT('`%s.%s`', target_dataset, baz_table);

  DECLARE table_exists BOOL DEFAULT FALSE;

  -- Check if target exists
  SET table_exists = (
    SELECT COUNT(1) > 0
    FROM `sqlserver_to_bq_silver.INFORMATION_SCHEMA.TABLES`
    WHERE table_name = target_table
  );

  -- Ensure BAZ migration table exists (persistent, like SQL Server table)
  EXECUTE IMMEDIATE FORMAT("""
    CREATE TABLE IF NOT EXISTS %s (
      order_id     INT64,
      created_date DATETIME
    )
  """, baz_fqtn);

  -- Upsert-like insert (Insert if not exists), last 1 day, like SQL Server GETDATE()-1
  EXECUTE IMMEDIATE FORMAT("""
    INSERT INTO %s (order_id, created_date)
    SELECT DISTINCT
      ot.order_id AS order_id,
      CAST(ah.created_date AS DATETIME) AS created_date
    FROM `sqlserver_to_bq_silver.xpomaster_orders_assignhistory` ah
    JOIN `sqlserver_to_bq_silver.xpomaster_orders_ordertrip` ot
      ON ot.trip_id = ah.trip_id
    WHERE ah.created_by = 'BazDataMigration'
      AND ah.created_date >= DATETIME_SUB(CURRENT_DATETIME(), INTERVAL 1 DAY)
      AND NOT EXISTS (
        SELECT 1
        FROM %s m
        WHERE m.order_id = ot.order_id
      )
  """, baz_fqtn, baz_fqtn);

  -- Build the incremental source set into a temp table (used by MERGE)
  CREATE TEMP TABLE tmp_src AS
  WITH
  params AS (SELECT start_dt AS start_dt),

  /* ============================================================
     1:1 incremental OrderIDs set (SQL Server @OrderIDs equivalent)
     ============================================================ */
  order_ids_raw AS (
    SELECT DISTINCT order_id
    FROM (
      -- Order changed
      SELECT o.order_id
      FROM `sqlserver_to_bq_silver.xpomaster_orders_order` o
      JOIN params p ON TRUE
      WHERE o.created_date >= p.start_dt OR o.updated_date >= p.start_dt

      -- UNION DISTINCT

      -- -- OrderTrip changed
      -- SELECT ot.order_id
      -- FROM `sqlserver_to_bq_silver.xpomaster_orders_ordertrip` ot
    

      UNION DISTINCT

      -- Trip changed (via OrderTrip)
      SELECT ot.order_id
      FROM `sqlserver_to_bq_silver.xpomaster_orders_ordertrip` ot
      JOIN `sqlserver_to_bq_silver.xpomaster_orders_trip` t
        ON t.trip_id = ot.trip_id
      JOIN params p ON TRUE
      WHERE t.created_date >= p.start_dt OR t.updated_date >= p.start_dt

      UNION DISTINCT

      -- Stop changed (via OrderTrip on trip_id)
      SELECT ot.order_id
      FROM `sqlserver_to_bq_silver.xpomaster_orders_stop` s
      JOIN `sqlserver_to_bq_silver.xpomaster_orders_ordertrip` ot
        ON ot.trip_id = s.trip_id
      JOIN params p ON TRUE
      WHERE s.created_date >= p.start_dt OR s.updated_date >= p.start_dt

      UNION DISTINCT

      -- OrderCharge changed
      SELECT oc.order_id
      FROM `sqlserver_to_bq_silver.xpomaster_orders_ordercharge` oc
      JOIN params p ON TRUE
      WHERE oc.created_date >= p.start_dt OR oc.updated_date >= p.start_dt

      UNION DISTINCT

      -- AccountsReceivable changed (has order_id)
      SELECT ar.order_id
      FROM `sqlserver_to_bq_silver.xpomaster_accounting_accountsreceivable` ar
      JOIN params p ON TRUE
      WHERE ar.created_date >= p.start_dt OR ar.updated_date >= p.start_dt

      UNION DISTINCT

      -- ChargeAdjustment changed (join to AR to get order_id)
      SELECT ar.order_id
      FROM `sqlserver_to_bq_silver.xpomaster_accounting_chargeadjustment` adj
      JOIN `sqlserver_to_bq_silver.xpomaster_accounting_accountsreceivable` ar
        ON ar.accounts_receivable_id = adj.accounts_receivable_id
      JOIN params p ON TRUE
      WHERE adj.created_date >= p.start_dt OR adj.updated_date >= p.start_dt

      UNION DISTINCT

      -- TripDeduction changed (via OrderTrip)
      SELECT ot.order_id
      FROM `sqlserver_to_bq_silver.xpomaster_orders_tripdeduction` td
      JOIN `sqlserver_to_bq_silver.xpomaster_orders_ordertrip` ot
        ON ot.trip_id = td.trip_id
      JOIN params p ON TRUE
      WHERE td.created_date >= p.start_dt OR td.updated_date >= p.start_dt

      UNION DISTINCT

      -- AccountsPayable changed (direct if order_id exists)
      SELECT ap.order_id
      FROM `sqlserver_to_bq_silver.xpomaster_accounting_accountspayable` ap
      JOIN params p ON TRUE
      WHERE (ap.created_date >= p.start_dt OR ap.updated_date >= p.start_dt)
        AND ap.order_id IS NOT NULL

      UNION DISTINCT

      -- AccountsPayable changed via TripId -> OrderTrip (fallback)
      SELECT ot.order_id
      FROM `sqlserver_to_bq_silver.xpomaster_accounting_accountspayable` ap
      JOIN `sqlserver_to_bq_silver.xpomaster_orders_ordertrip` ot
        ON ot.trip_id = ap.trip_id
      JOIN params p ON TRUE
      WHERE ap.created_date >= p.start_dt OR ap.updated_date >= p.start_dt
    )
  ),

  /* ============================================================
     SQL Server BAZ exclusion (delete from @OrderIDs)
     ============================================================ */
  order_ids AS (
    SELECT order_id FROM order_ids_raw
    EXCEPT DISTINCT
    SELECT order_id FROM `sqlserver_to_bq_silver.baz_settled_order_migration`
  ),

  cte_orders AS (
    SELECT
      o.order_id,
      o.created_date,
      o.code,
      CASE WHEN o.bill_distance < 100000 THEN o.bill_distance END AS bill_distance,
      SAFE_CAST(o.weight AS NUMERIC) AS weight,
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
      o.is_on_hold,
      o.is_uncommited,
      o.source_type_id,
      o.value_of_goods,
      o.shipment_id,
      o.carrier_portal_posting_date,
      o.is_drop_and_hook,

      -- ✅ CAMINO A: usar el trip_count ya calculado en la tabla Order
      o.trip_count AS order_trip_count

    FROM `sqlserver_to_bq_silver.xpomaster_orders_order` o
    JOIN order_ids oi USING(order_id)
    WHERE o.order_status_type_id IN (1,2,3,4,5)
  ),

  stops AS (
    SELECT
      o.order_id,

      os1.stop_id AS origin_stop_id,
      os1.trip_id AS origin_trip_id,
      os1.address_id AS origin_address_id,
      os1.location_id AS origin_location_id,
      os1.scheduled_early_arrival_time AS origin_sched_early_arrival_time,
      os1.scheduled_late_arrival_time  AS origin_sched_late_arrival_time,
      os1.actual_arrival_time          AS origin_actual_arrival_time,
      os1.actual_departure_time        AS origin_actual_departure_time,
      os1.scheduled_departure_time     AS origin_scheduled_departure_time,

      os2.stop_id AS dest_stop_id,
      os2.trip_id AS dest_trip_id,
      os2.address_id AS dest_address_id,
      os2.location_id AS dest_location_id,
      os2.scheduled_early_arrival_time AS dest_sched_early_arrival_time,
      os2.scheduled_late_arrival_time  AS dest_sched_late_arrival_time,
      os2.actual_arrival_time          AS dest_actual_arrival_time,
      os2.actual_departure_time        AS dest_actual_departure_time,
      os2.scheduled_departure_time     AS dest_scheduled_departure_time

    FROM cte_orders o
    LEFT JOIN `sqlserver_to_bq_silver.xpomaster_orders_stop` os1
      ON os1.stop_id = o.origin_stop_id
    LEFT JOIN `sqlserver_to_bq_silver.xpomaster_orders_stop` os2
      ON os2.stop_id = o.destination_stop_id
  ),

  resolved_geo AS (
    SELECT
      s.order_id,

      COALESCE(a1.city_id, a1f.city_id)       AS origin_city_id,
      COALESCE(a1.state_id, a1f.state_id)     AS origin_state_id,
      COALESCE(a1.zip_id, a1f.zip_id)         AS origin_zip_id,
      COALESCE(a1.country_id, a1f.country_id) AS origin_country_id,

      COALESCE(a2.city_id, a2f.city_id)       AS dest_city_id,
      COALESCE(a2.state_id, a2f.state_id)     AS dest_state_id,
      COALESCE(a2.zip_id, a2f.zip_id)         AS dest_zip_id,
      COALESCE(a2.country_id, a2f.country_id) AS dest_country_id,

      s.origin_location_id,
      s.dest_location_id,

      s.origin_sched_early_arrival_time,
      s.origin_sched_late_arrival_time,
      s.origin_actual_arrival_time,
      s.origin_actual_departure_time,
      s.origin_scheduled_departure_time,

      s.dest_sched_early_arrival_time,
      s.dest_sched_late_arrival_time,
      s.dest_actual_arrival_time,
      s.dest_actual_departure_time,
      s.dest_scheduled_departure_time,

      s.origin_trip_id
    FROM stops s
    LEFT JOIN `sqlserver_to_bq_silver.xpomaster_locale_address` a1
      ON a1.address_id = s.origin_address_id
    LEFT JOIN `sqlserver_to_bq_silver.xpomaster_locale_address` a2
      ON a2.address_id = s.dest_address_id
    LEFT JOIN `sqlserver_to_bq_silver.xpomaster_customer_location` loc1
      ON loc1.location_id = s.origin_location_id
    LEFT JOIN `sqlserver_to_bq_silver.xpomaster_customer_location` loc2
      ON loc2.location_id = s.dest_location_id
    LEFT JOIN `sqlserver_to_bq_silver.xpomaster_locale_address` a1f
      ON a1f.address_id = loc1.address_id
    LEFT JOIN `sqlserver_to_bq_silver.xpomaster_locale_address` a2f
      ON a2f.address_id = loc2.address_id
  ),

  resolved_locations AS (
    SELECT
      rg.order_id,

      c1.name AS origin_city,
      st1.state_code AS origin_state_code,
      z1.zip_code AS origin_zip_code,
      co1.country_code AS origin_country_code,

      c2.name AS destination_city,
      st2.state_code AS destination_state_code,
      z2.zip_code AS destination_zip_code,
      co2.country_code AS destination_country_code,

      rg.origin_location_id,
      rg.dest_location_id,

      rg.origin_sched_early_arrival_time,
      rg.origin_sched_late_arrival_time,
      rg.origin_actual_arrival_time,
      rg.origin_actual_departure_time,
      rg.origin_scheduled_departure_time,

      rg.dest_sched_early_arrival_time,
      rg.dest_sched_late_arrival_time,
      rg.dest_actual_arrival_time,
      rg.dest_actual_departure_time,
      rg.dest_scheduled_departure_time,

      rg.origin_trip_id
    FROM resolved_geo rg
    LEFT JOIN `sqlserver_to_bq_silver.xpomaster_locale_city` c1
      ON c1.city_id = rg.origin_city_id
    LEFT JOIN `sqlserver_to_bq_silver.xpomaster_locale_state` st1
      ON st1.state_id = rg.origin_state_id
    LEFT JOIN `sqlserver_to_bq_silver.xpomaster_locale_zip` z1
      ON z1.zip_id = rg.origin_zip_id
    LEFT JOIN `sqlserver_to_bq_silver.xpomaster_locale_country` co1
      ON co1.country_id = rg.origin_country_id

    LEFT JOIN `sqlserver_to_bq_silver.xpomaster_locale_city` c2
      ON c2.city_id = rg.dest_city_id
    LEFT JOIN `sqlserver_to_bq_silver.xpomaster_locale_state` st2
      ON st2.state_id = rg.dest_state_id
    LEFT JOIN `sqlserver_to_bq_silver.xpomaster_locale_zip` z2
      ON z2.zip_id = rg.dest_zip_id
    LEFT JOIN `sqlserver_to_bq_silver.xpomaster_locale_country` co2
      ON co2.country_id = rg.dest_country_id
  ),

  trip_dim AS (
    SELECT
      rl.order_id,
      rl.origin_trip_id AS trip_id,
      t.trip_brokerage_status_id,
      t.carrier_id,
      t.booked_by_user_id,
      t.dispatcher_user_id,
      t.freight_assigned_actual_carrier_rep_id
    FROM resolved_locations rl
    LEFT JOIN `sqlserver_to_bq_silver.xpomaster_orders_trip` t
      ON t.trip_id = rl.origin_trip_id
  ),

  reschedule_reason AS (
    SELECT
      stop_id,
      CASE
        WHEN code IN ('RXO Operational/Sales Failure','Carrier assigned to load late') THEN 'RXO'
        WHEN code IN ('Carrier Service Failure','Carrier late') THEN 'Carrier'
        WHEN code IN ('Acceptable Reschedule (not a service failure)') THEN 'Customer'
        ELSE NULL
      END AS reschedule_reason
    FROM (
      SELECT
        rt.stop_id,
        rc.code,
        rt.created_date,
        ROW_NUMBER() OVER (PARTITION BY rt.stop_id ORDER BY rt.created_date ASC) AS rn
      FROM `sqlserver_to_bq_silver.xpomaster_orders_rescheduledtrip` rt
      JOIN `sqlserver_to_bq_silver.xpomaster_dbo_rescheduledtripcodes` rc
        ON rc.id = rt.reason_code_id
      JOIN params p
        ON rt.created_date >= p.start_dt
    )
    WHERE rn = 1
  ),

  service_failure AS (
    SELECT
      stop_id,
      service_failure_code_id,
      reason_category_id
    FROM (
      SELECT
        sf.stop_id,
        sf.service_failure_code_id,
        sf.reason_category_id,
        sf.created_date,
        ROW_NUMBER() OVER (PARTITION BY sf.stop_id ORDER BY sf.created_date ASC) AS rn
      FROM `sqlserver_to_bq_silver.xpomaster_orders_servicefailure` sf
      JOIN params p
        ON sf.created_date >= p.start_dt
    )
    WHERE rn = 1
  ),

  commodity_qty AS (
    SELECT
      order_id,
      SUM(IFNULL(quantity, 0)) AS quantity
    FROM `sqlserver_to_bq_silver.xpomaster_orders_ordercommodity`
    GROUP BY order_id
  ),

  reporting AS (
    SELECT rc.reporting_code_id, rc.code AS reporting_code, rc.office_location_id
    FROM `sqlserver_to_bq_silver.xpomaster_dbo_reportingcode` rc
  ),

  office_location AS (
    SELECT ol.office_location_id, TRIM(ol.code) AS office_location_code, ol.company_code
    FROM `sqlserver_to_bq_silver.xpomaster_dbo_officelocation` ol
  ),

  service_type AS (
    SELECT st.service_type_id, CAST(st.code AS STRING) AS service_type_code
    FROM `sqlserver_to_bq_silver.xpomaster_orders_servicetype` st
  ),

  source_type AS (
    SELECT st.source_type_id, st.code AS order_source_code
    FROM `sqlserver_to_bq_silver.xpomaster_orders_sourcetype` st
  ),

  trip_brokerage_status AS (
    SELECT tbst.trip_brokerage_status_type_id, tbst.code AS brokerage_status_code
    FROM `sqlserver_to_bq_silver.xpomaster_orders_tripbrokeragestatustype` tbst
  ),

  order_status_type AS (
    SELECT ost.order_status_type_id, ost.code AS order_status_type_code
    FROM `sqlserver_to_bq_silver.xpomaster_orders_orderstatustype` ost
  ),

  financial_status_type AS (
    SELECT fst.financial_status_type_id, fst.code AS order_financial_status_type_code
    FROM `sqlserver_to_bq_silver.xpomaster_accounting_financialstatustype` fst
  ),

  customer_dim AS (
    SELECT c.customer_id, CAST(c.code AS STRING) AS freight_optimizer_customer_code
    FROM `sqlserver_to_bq_silver.xpomaster_customer_customer` c
  ),

  carrier_dim AS (
    SELECT c.carrier_id, CAST(c.code AS STRING) AS carrier_code
    FROM `sqlserver_to_bq_silver.xpomaster_carrier_carrier` c
  ),

  equipment_dim AS (
    SELECT e.equipment_type_id, TRIM(CAST(e.code AS STRING)) AS equipment_code
    FROM `sqlserver_to_bq_silver.xpomaster_dbo_equipmenttype` e
  ),

  salesperson AS (
    SELECT sp.salesperson_id, sp.user_id, CAST(sp.code AS STRING) AS salesperson_code, sp.office_location_id
    FROM `sqlserver_to_bq_silver.xpomaster_security_salesperson` sp
  ),

  sec_user AS (
    SELECT u.user_id, u.user_name, u.office_location_id
    FROM `sqlserver_to_bq_silver.xpomaster_security_user` u
  ),

  inside_sales_primary AS (
    SELECT osr.order_id, osr.salesperson_id, osr.percentage_of_commission
    FROM `sqlserver_to_bq_silver.xpomaster_orders_ordersalesrep` osr
    WHERE osr.is_outside_sales = FALSE AND osr.is_primary = TRUE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY osr.order_id ORDER BY osr.percentage_of_commission DESC) = 1
  ),

  inside_sales_secondary AS (
    SELECT osr.order_id, osr.salesperson_id, osr.percentage_of_commission
    FROM `sqlserver_to_bq_silver.xpomaster_orders_ordersalesrep` osr
    WHERE osr.is_outside_sales = FALSE AND osr.is_primary = FALSE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY osr.order_id ORDER BY osr.percentage_of_commission DESC) = 1
  ),

  bazooka AS (
    SELECT order_id, bazooka_order_id
    FROM `sqlserver_to_bq_silver.xpomaster_orders_fobazookaordermapping`
  )

  SELECT
    'Freight Optimizer' AS source_system_name,
    src.order_source_code AS order_source_code,
    TRIM(rep.reporting_code) AS business_unit_code,
    svt.service_type_code AS service_type_code,

    ost.order_status_type_code AS order_status_type_code,
    bstat.brokerage_status_code AS brokerage_status_code,

    pr.reschedule_reason AS pickup_rescheduled_reason,
    dr.reschedule_reason AS drop_rescheduled_reason,

    CASE WHEN o.is_on_hold THEN 'Y' ELSE 'N' END AS is_on_hold,
    CASE WHEN o.is_uncommited THEN 'Y' ELSE 'N' END AS is_uncommited,

    fst.order_financial_status_type_code AS order_financial_status_type_code,

    o.created_date AS order_date,
    rl.origin_sched_early_arrival_time AS scheduled_pickup_date,
    COALESCE(rl.dest_sched_late_arrival_time, rl.dest_sched_early_arrival_time) AS scheduled_delivery_date,
    rl.origin_actual_arrival_time AS pickup_date,
    rl.origin_actual_departure_time AS pickup_departure_date,
    rl.dest_actual_departure_time AS delivered_date,
    rl.dest_actual_arrival_time AS delivery_arrival_date,

    tca.carrier_assign_date AS carrier_assign_date,
    ov.void_date AS void_date,

    cust.freight_optimizer_customer_code AS freight_optimizer_customer_code,
    car.carrier_code AS carrier_code,
    eq.equipment_code AS equipment_code,

    o.order_id AS order_key,
    CAST(o.code AS STRING) AS order_number,

    IFNULL(tc.amount, 0) AS order_sales_amount,
    IFNULL(tlh.amount, 0) AS line_haul_charge_amount,
    IFNULL(tfc.amount, 0) AS fuel_charge_amount,
    IFNULL(tac.amount, 0) AS total_accessorial_charge,
    IFNULL(det.amount, 0) AS detention_charge,
    IFNULL(tonu.amount, 0) AS tonu_charge,
    IFNULL(tlhpay.amount, 0) AS line_haul_pay_amount,
    IFNULL(tfpay.amount, 0) AS fuel_pay_amount,
    IFNULL(tpay.amount, 0) AS order_cost,

    IFNULL(tlh.lh_rate, 0) AS lh_rate,

    -- ✅ CAMINO A: trip_count desde Order
    IFNULL(o.order_trip_count, 0) AS trip_count,

    o.bill_distance AS bill_distance,
    o.weight AS weight_in_pounds,

    CASE
      WHEN rl.dest_actual_arrival_time IS NULL THEN NULL
      WHEN rl.dest_actual_arrival_time > COALESCE(rl.dest_sched_late_arrival_time, rl.dest_sched_early_arrival_time) THEN 0
      WHEN rl.dest_actual_arrival_time <= COALESCE(rl.dest_sched_late_arrival_time, rl.dest_sched_early_arrival_time) THEN 1
      ELSE NULL
    END AS on_time_delivery,

    CASE
      WHEN rl.dest_actual_arrival_time > COALESCE(rl.dest_sched_late_arrival_time, rl.dest_sched_early_arrival_time)
      THEN DATETIME_DIFF(
        rl.dest_actual_arrival_time,
        COALESCE(rl.dest_sched_late_arrival_time, rl.dest_sched_early_arrival_time),
        MINUTE
      )
      ELSE NULL
    END AS delivery_delay,

    CASE
      WHEN rl.origin_actual_arrival_time IS NULL THEN NULL
      WHEN rl.origin_actual_arrival_time > COALESCE(rl.origin_sched_late_arrival_time, rl.origin_sched_early_arrival_time) THEN 0
      WHEN rl.origin_actual_arrival_time <= COALESCE(rl.origin_sched_late_arrival_time, rl.origin_sched_early_arrival_time) THEN 1
      ELSE NULL
    END AS on_time_pickup,

    CASE
      WHEN rl.origin_actual_arrival_time > COALESCE(rl.origin_sched_late_arrival_time, rl.origin_sched_early_arrival_time)
      THEN DATETIME_DIFF(
        rl.origin_actual_arrival_time,
        COALESCE(rl.origin_sched_late_arrival_time, rl.origin_sched_early_arrival_time),
        MINUTE
      )
      ELSE NULL
    END AS pickup_delay,

    rl.origin_city,
    rl.origin_state_code,
    rl.origin_zip_code AS origin_postal_code,
    rl.origin_country_code,
    rl.origin_location_id,

    rl.destination_city,
    rl.destination_state_code,
    rl.destination_zip_code AS destination_postal_code,
    rl.destination_country_code,
    rl.dest_location_id AS destination_location_id,

    entered_sp.salesperson_code AS entered_by_user_name,
    prim_sp.salesperson_code AS primary_sales_rep_user_name,
    sec_sp.salesperson_code AS secondary_sales_rep_user_name,

    COALESCE(booked_sp.salesperson_code, booked_u.user_name) AS booked_by_user_name,
    COALESCE(dispatch_sp.salesperson_code, dispatch_u.user_name) AS primary_carrier_rep_user_name,
    freight_sp.salesperson_code AS freight_assign_rep_user_name,

    TRIM(COALESCE(dispatch_ol.office_location_code, dispatch_ol2.office_location_code)) AS dispatch_office_location_code,

    CASE
      WHEN dispatch_ol.office_location_code IN ('XPOSPA')
           AND ol.company_code NOT IN ('COY MEX', 'COY US') THEN 1
      WHEN dispatch_ol.company_code IN ('COY MEX', 'COY US')
           AND ol.company_code NOT IN ('COY MEX', 'COY US') THEN 1
      WHEN dispatch_ol.office_location_code <> ol.office_location_code
           AND (dispatch_ol.company_code NOT IN ('COY MEX', 'COY US')) THEN 1
      ELSE 0
    END AS cross_booked,

    0 AS bounce_to_bounce,

    o.created_by AS created_by_user,
    o.is_hazmat AS is_hazmat,
    qty.quantity AS quantity,

    rl.origin_sched_late_arrival_time AS pickup_late,

    o.carrier_portal_posting_date AS carrier_portal_posting_date,
    o.is_drop_and_hook AS is_drop_and_hook,

    psf.service_failure_code_id AS pickup_service_failure_code_id,
    psf.reason_category_id AS pickup_service_failure_reason_category_id,
    dsf.service_failure_code_id AS delivery_service_failure_code_id,
    dsf.reason_category_id AS delivery_service_failure_reason_category_id,

    prim.percentage_of_commission AS percentage_of_commission_primary,

    SAFE_CAST(NULL AS INT64) AS company_id,

    o.value_of_goods AS value_of_goods,
    bz.bazooka_order_id AS bazooka_order_id,
    o.shipment_id AS shipment_id,

    rl.origin_scheduled_departure_time AS origin_scheduled_departure_time,
    rl.dest_scheduled_departure_time AS destination_stop_scheduled_departure_time,

    td.trip_id AS trip_id,
    td.trip_brokerage_status_id AS trip_brokerage_status_id,
    td.carrier_id AS carrier_id

  FROM cte_orders o
  LEFT JOIN resolved_locations rl ON rl.order_id = o.order_id
  LEFT JOIN trip_dim td ON td.order_id = o.order_id

  LEFT JOIN `sqlserver_to_bq_silver.fn_order_total_charge`() tc
    ON tc.order_id = o.order_id
  LEFT JOIN `sqlserver_to_bq_silver.fn_order_total_accessorial_charge`() tac
    ON tac.order_id = o.order_id
  LEFT JOIN `sqlserver_to_bq_silver.fn_order_total_linehaul_charge`() tlh
    ON tlh.order_id = o.order_id
  LEFT JOIN `sqlserver_to_bq_silver.fn_order_total_fuel_charge`() tfc
    ON tfc.order_id = o.order_id
  LEFT JOIN `sqlserver_to_bq_silver.fn_order_total_fuel_pay`() tfpay
    ON tfpay.order_id = o.order_id
  LEFT JOIN `sqlserver_to_bq_silver.fn_order_total_linehaul_pay`() tlhpay
    ON tlhpay.order_id = o.order_id
  LEFT JOIN `sqlserver_to_bq_silver.fn_order_detention_charges`() det
    ON det.order_id = o.order_id
  LEFT JOIN `sqlserver_to_bq_silver.fn_order_tonu_charges`() tonu
    ON tonu.order_id = o.order_id
  LEFT JOIN `sqlserver_to_bq_silver.fn_order_total_pay`() tpay
    ON tpay.order_id = o.order_id

  LEFT JOIN `sqlserver_to_bq_silver.fn_order_void_date`() ov
    ON ov.order_id = o.order_id
  LEFT JOIN `sqlserver_to_bq_silver.fn_trip_carrier_assign_date`() tca
    ON tca.trip_id = td.trip_id AND tca.carrier_id = td.carrier_id

  LEFT JOIN reschedule_reason pr ON pr.stop_id = o.origin_stop_id
  LEFT JOIN reschedule_reason dr ON dr.stop_id = o.destination_stop_id
  LEFT JOIN service_failure psf ON psf.stop_id = o.origin_stop_id
  LEFT JOIN service_failure dsf ON dsf.stop_id = o.destination_stop_id

  LEFT JOIN commodity_qty qty ON qty.order_id = o.order_id

  LEFT JOIN source_type src ON src.source_type_id = o.source_type_id
  LEFT JOIN reporting rep ON rep.reporting_code_id = o.reporting_code_id
  LEFT JOIN office_location ol ON ol.office_location_id = rep.office_location_id
  LEFT JOIN service_type svt ON svt.service_type_id = o.service_type_id
  LEFT JOIN order_status_type ost ON ost.order_status_type_id = o.order_status_type_id
  LEFT JOIN financial_status_type fst ON fst.financial_status_type_id = o.financial_status_type_id
  LEFT JOIN trip_brokerage_status bstat ON bstat.trip_brokerage_status_type_id = td.trip_brokerage_status_id

  LEFT JOIN customer_dim cust ON cust.customer_id = o.customer_id
  LEFT JOIN carrier_dim car ON car.carrier_id = td.carrier_id
  LEFT JOIN equipment_dim eq ON eq.equipment_type_id = o.requested_trailer_equipment_type_id

  LEFT JOIN sec_user entered_u ON entered_u.user_name = o.created_by
  LEFT JOIN salesperson entered_sp ON entered_sp.user_id = entered_u.user_id

  LEFT JOIN inside_sales_primary prim ON prim.order_id = o.order_id
  LEFT JOIN inside_sales_secondary sec ON sec.order_id = o.order_id
  LEFT JOIN salesperson prim_sp ON prim_sp.salesperson_id = prim.salesperson_id
  LEFT JOIN salesperson sec_sp ON sec_sp.salesperson_id = sec.salesperson_id

  LEFT JOIN sec_user booked_u ON booked_u.user_id = td.booked_by_user_id
  LEFT JOIN salesperson booked_sp ON booked_sp.user_id = td.booked_by_user_id

  LEFT JOIN sec_user dispatch_u ON dispatch_u.user_id = td.dispatcher_user_id
  LEFT JOIN salesperson dispatch_sp ON dispatch_sp.user_id = td.dispatcher_user_id
  LEFT JOIN office_location dispatch_ol ON dispatch_ol.office_location_id = dispatch_sp.office_location_id
  LEFT JOIN office_location dispatch_ol2 ON dispatch_ol2.office_location_id = dispatch_u.office_location_id

  LEFT JOIN salesperson freight_sp ON freight_sp.user_id = td.freight_assigned_actual_carrier_rep_id
  LEFT JOIN bazooka bz ON bz.order_id = o.order_id
  ;

  -- If table doesn't exist, create it from the current incremental set
  IF NOT table_exists THEN
    EXECUTE IMMEDIATE FORMAT("CREATE TABLE %s AS SELECT * FROM tmp_src", target_fqtn);
    RETURN;
  END IF;

  -- If exists, MERGE (upsert) by order_key
  EXECUTE IMMEDIATE FORMAT("""
    MERGE %s T
    USING tmp_src S
      ON T.order_key = S.order_key

    WHEN MATCHED THEN UPDATE SET
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

    WHEN NOT MATCHED THEN INSERT (
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
    )
  """, target_fqtn);

END;

Tengo dos preguntas:
tenien un problema de performance, puedes revisarlo?
y esta filtrando muchas order_key? hay un filtro adicional? o es solo la fecha?
