CUal es la diferencia entre esta funcion table, llamemosla FN1:

/* ---------------------------------------------------------------------------
   File Name      : fn_trip_carrier_assign_date.sql
   Object Type    : TABLE FUNCTION
   Object Name    : sqlserver_to_bq_silver.fn_trip_carrier_assign_date
 
   Description    : Return latest carrier assignment date per trip and carrier.
 
   Author         : Abhinav Chandna

   Version        : 1.0.0
   Created On     : 2026-01-08
   Last Modified  : 2026-01-08
 
   Change History :
      - 2026-01-08 | 1.0.0 | AC | Initial version
      - 2026-01-08 | 1.0.1 | OP | CAST to DATETIMEE

 --------------------------------------------------------------------------- */

CREATE OR REPLACE TABLE FUNCTION
`sqlserver_to_bq_silver.fn_trip_carrier_assign_date`()
AS (
  SELECT
    trip_id,
    carrier_id,
    CAST(created_date AS DATETIME) AS carrier_assign_date
  FROM (
    SELECT
      ah.trip_id,
      ah.carrier_id,
      ah.created_date,
      ROW_NUMBER() OVER (
        PARTITION BY ah.trip_id, ah.carrier_id
        ORDER BY ah.created_date DESC
      ) AS rn
    FROM `sqlserver_to_bq_silver.xpomaster_orders_assignhistory` ah
    WHERE ah.carrier_unassign_reason_type_id IS NULL
      AND ah.carrier_id IS NOT NULL
  )
  WHERE rn = 1
);


y esta llamemosla FN_ORIG:
/* ---------------------------------------------------------------------------
   File Name      : fn_trip_carrier_assign_date.sql
   Object Type    : TABLE FUNCTION
   Object Name    : sqlserver_to_bq_silver.fn_trip_carrier_assign_date
 
   Description    : Return latest carrier assignment date per trip and carrier.
 
   Author         : Abhinav Chandna

   Version        : 1.0.0
   Created On     : 2026-01-08
   Last Modified  : 2026-01-08
 
   Change History :
      - 2026-01-08 | 1.0.0 | AC | Initial version

 --------------------------------------------------------------------------- */

CREATE OR REPLACE TABLE FUNCTION `sqlserver_to_bq_silver.fn_trip_carrier_assign_date`() AS (
SELECT
    ah.trip_id AS trip_id,
    ah.carrier_id AS carrier_id,
    ah.created_date AS carrier_assign_date
  FROM `sqlserver_to_bq_silver.xpomaster_orders_assignhistory` AS ah
  WHERE
    ah.carrier_unassign_reason_type_id IS NULL
    AND ah.carrier_id IS NOT NULL
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY ah.trip_id, ah.carrier_id
      ORDER BY ah.created_date DESC
    ) = 1
);

