WITH AllData AS (
  SELECT
  Product.Name              AS ProductName,
  SUM(OrderDetail.OrderQty) AS OrderQuantity,
  Product.DaysToManufacture AS DaysToManufacture

  FROM
    etl-rox-application.datalake_tables.Product Product

  INNER JOIN etl-rox-application.datalake_tables.SalesOrderDetail OrderDetail
  USING (ProductID)

  INNER JOIN etl-rox-application.datalake_tables.SalesSpecialOfferProduct Offer
  USING (SpecialOfferID, ProductID)

  GROUP BY DaysToManufacture, ProductName
  ORDER BY 2 DESC
),

TopThree AS (
    SELECT *, ROW_NUMBER() 
    OVER (
        PARTITION BY DaysToManufacture 
        order by OrderQuantity
    ) AS RowNo 
    FROM AllData
)
SELECT * FROM TopThree WHERE RowNo <=3
ORDER BY OrderQuantity DESC, DaysToManufacture