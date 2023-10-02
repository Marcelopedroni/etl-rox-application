SELECT 
  SalesOrderID, 
  OrderDate, 
  TotalDue
FROM `etl-rox-application.datalake_tables.SalesOrderHeader` 

WHERE
  SAFE_CAST(OrderDate AS DATE) BETWEEN '2011-09-01' AND '2011-09-30'
  AND TotalDue > 1000
ORDER BY TotalDue DESC