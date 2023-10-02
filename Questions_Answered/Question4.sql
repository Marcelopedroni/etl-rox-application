SELECT
  Product.ProductID AS ProductID,
  SUM(OrderDetail.OrderQty) AS OrderQuantity,
  SAFE_CAST(Header.OrderDate AS DATE) AS OrderDate
FROM
  `etl-rox-application.datalake_tables.Product` Product

INNER JOIN `etl-rox-application.datalake_tables.SalesOrderDetail` OrderDetail
USING (ProductID)

INNER JOIN `etl-rox-application.datalake_tables.SalesOrderHeader` Header
USING (SalesOrderID)

WHERE ProductID = 864
GROUP BY
  ProductID,
  OrderDate

ORDER BY OrderQuantity DESC