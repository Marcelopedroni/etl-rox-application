WITH 
OrdersIDFilter AS (
  SELECT 
    SalesOrderID,
    COUNT(*) AS RowsQuantity
    
  FROM 
    etl-rox-application.datalake_tables.SalesOrderDetail
  GROUP BY SalesOrderID
  ORDER BY 2 DESC
)

SELECT * FROM OrdersIDFilter
WHERE RowsQuantity >= 3