CREATE DATABASE IF NOT EXISTS ElectronicaDW;

USE ElectronicaDW;

DROP TABLE IF EXISTS products;

CREATE TABLE products (
    productID INT,
    productName VARCHAR(255),
    productPrice DECIMAL(10, 2),
    supplierID INT,
    supplierName VARCHAR(255),
    storeID INT,
    storeName VARCHAR(255)
);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/master_data.csv'
INTO TABLE products
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
ESCAPED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(productID, productName, @var1, supplierID, supplierName, storeID, storeName)
SET productPrice = replace(@var1, '$', '');

-- SHOW VARIABLES LIKE "secure_file_priv";

-- select * from products; 