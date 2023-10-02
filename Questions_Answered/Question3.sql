WITH AllData AS (
    SELECT * FROM `etl-rox-application.datalake_tables.SalesOrderHeader` Header

    INNER JOIN `etl-rox-application.datalake_tables.SalesCustomer` Customer
    ON Customer.CustomerID = Header.CustomerID

    INNER JOIN `etl-rox-application.datalake_tables.Person` Person
    ON SAFE_CAST(Customer.PersonID AS INT64) = Person.BusinessEntityID
)

SELECT
  FirstName,
  MiddleName,
  LastName,
  COUNT(SalesOrderNumber) AS OrdersQuantity
FROM AllData
GROUP BY FirstName, MiddleName, LastName
ORDER BY OrdersQuantity DESC
