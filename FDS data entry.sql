CREATE DATABASE IF NOT EXISTS ElectronicaDW;

USE ElectronicaDW;

DROP TABLE IF EXISTS orders;

CREATE TABLE orders (
    orderID INT,
    orderDate DATETIME,
    productID INT,
    customerID INT,
    customerName VARCHAR(255),
    gender VARCHAR(10),
    quantityOrdered INT
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/transactions.csv'
INTO TABLE orders

FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
ESCAPED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(orderID, @var1, productID, customerID, customerName, gender, quantityOrdered)
SET orderDate = STR_TO_DATE(@var1, '%m/%d/%Y %H:%i');

-- SHOW VARIABLES LIKE "secure_file_priv";

-- select * from orders;