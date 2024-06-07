CREATE DATABASE IF NOT EXISTS ElectronicaDW;

USE ElectronicaDW;

DROP TABLE IF EXISTS F_Sales;
DROP TABLE IF EXISTS D_Order;
DROP TABLE IF EXISTS D_Date;
DROP TABLE IF EXISTS D_Product;
DROP TABLE IF EXISTS D_Customer;
DROP TABLE IF EXISTS D_Supplier;
DROP TABLE IF EXISTS D_Store;

CREATE TABLE IF NOT EXISTS D_Order (
    OrderID INT PRIMARY KEY,
    QuantityOrdered INT
);

CREATE TABLE D_Date (
    DateID INT PRIMARY KEY auto_increment,
    Day INT,
    Month INT,
    Quarter INT,
    Year INT
);

CREATE TABLE D_Product (
    ProductID INT PRIMARY KEY,
    ProductName VARCHAR(255),
    ProductPrice Double(10, 2)
);

CREATE TABLE D_Customer (
    CustomerID INT PRIMARY KEY,
    CustomerName VARCHAR(255),
    Gender VARCHAR(255)
);

CREATE TABLE D_Supplier (
    SupplierID INT PRIMARY KEY,
    SupplierName VARCHAR(255)
);

CREATE TABLE D_Store (
    StoreID INT PRIMARY KEY,
    StoreName VARCHAR(255)
);

CREATE TABLE F_Sales (
    SaleID INT PRIMARY KEY auto_increment,
    OrderID INT,
    DateID INT,
    ProductID INT,
    CustomerID INT,
    SupplierID INT,
    StoreID INT,
    Total_Sale Double(10, 2),
    FOREIGN KEY (OrderID) REFERENCES D_Order(OrderID),
    FOREIGN KEY (DateID) REFERENCES D_Date(DateID),
    FOREIGN KEY (ProductID) REFERENCES D_Product(ProductID),
    FOREIGN KEY (CustomerID) REFERENCES D_Customer(CustomerID),
    FOREIGN KEY (SupplierID) REFERENCES D_Supplier(SupplierID),
    FOREIGN KEY (StoreID) REFERENCES D_Store(StoreID)
);

ALTER TABLE D_Order
ADD CONSTRAINT uc_OrderID UNIQUE (OrderID);

ALTER TABLE D_Date
ADD CONSTRAINT uc_DateID UNIQUE (DateID);

ALTER TABLE D_Product
ADD CONSTRAINT uc_ProductID UNIQUE (ProductID);

ALTER TABLE D_Customer
ADD CONSTRAINT uc_CustomerID UNIQUE (CustomerID);

ALTER TABLE D_Supplier
ADD CONSTRAINT uc_SupplierID UNIQUE (SupplierID);

ALTER TABLE D_Store
ADD CONSTRAINT uc_StoreID UNIQUE (StoreID);

ALTER TABLE F_Sales
ADD CONSTRAINT uc_SaleID UNIQUE (SaleID);