Tengo esta query en sqlserver:

SELECT 
    o.OrderID,
    ISNULL(charges.[Amount], 0) AS [OrderSalesAmount]
FROM [accounting].[AccountsPayable] AS o
OUTER APPLY [itvf].[OrderTotalCharge] AS charges
WHERE o.OrderId IN (
    11257068, 11544271, 11586289, 11589603, 11608930,
    11621472, 11635861, 11640210, 11755525, 11764666
);

Resultado:
OrderID	OrderSalesAmount
11257068	1538.8000
11257068	1538.8000
11544271	838.4000
11544271	838.4000
11586289	268.9300
11589603	319.1800
11589603	319.1800
11608930	255.0000
11608930	255.0000
11621472	1424.3400
11635861	2333.3500
11635861	2333.3500
11640210	2333.3500
11640210	2333.3500
11755525	257.1100
11755525	257.1100
11764666	1211.0000
11764666	1211.0000


Y en BQ:

-- BigQuery (Standard SQL)
SELECT
  o.order_id AS OrderID,
  IFNULL(tc.amount, 0) AS order_sales_amount
  FROM `rxo-dataeng-datalake-uat.sqlserver_to_bq_silver.xpomaster_accounting_accountspayable` AS o
 LEFT JOIN `sqlserver_to_bq_silver.fn_order_total_charge`() tc
    ON tc.order_id = o.order_id
WHERE o.order_id IN (
  11257068, 11544271, 11586289, 11589603, 11608930,
  11621472, 11635861, 11640210, 11755525, 11764666
) ORDER BY o.order_id ASC
Con salida:
OrderID	order_sales_amount
11257068	1538.8
11257068	1538.8
11544271	406.48
11544271	406.48
11586289	0
11589603	319.18
11589603	319.18
11608930	255
11608930	255
11621472	1424.34
11635861	2333.35
11635861	2333.35
11640210	2333.35
11640210	2333.35
11755525	162.11
11755525	162.11
11764666	996
11764666	996

Las funciones estan en el contexto, en caso quieras revisarla, Porque salen difernets los valores. Revisa las queries. Revisa las funciones en el contexto.
