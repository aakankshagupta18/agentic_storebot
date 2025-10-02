CREATE DATABASE IF NOT EXISTS synthetic_store;
USE synthetic_store;

-- mirror tables in MySQL (we’ll place some here)
CREATE TABLE IF NOT EXISTS orders (
  `Row ID` VARCHAR(255),
  `Order ID` VARCHAR(255),
  `Order Date` DATE,
  `Ship Date` DATE,
  `Ship Mode` VARCHAR(255),
  `Customer ID` VARCHAR(255),
  `Customer Name` VARCHAR(255),
  `Segment` VARCHAR(255),
  `Country/Region` VARCHAR(255),
  `City` VARCHAR(255),
  `State/Province` VARCHAR(255),
  `Postal Code` VARCHAR(255),
  `Region` VARCHAR(255),
  `Product ID` VARCHAR(255),
  `Category` VARCHAR(255),
  `Sub-Category` VARCHAR(255),
  `Product Name` VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS returns (
  `Returned` VARCHAR(255),
  `ID` VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS regional_managers (
  `Regional Manager` VARCHAR(255),
  `Regions` VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS state_managers (
  `State/Province` VARCHAR(255),
  `Manager` VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS segment_managers (
  `Segment` VARCHAR(255),
  `Manager` VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS category_managers (
  `Category` VARCHAR(255),
  `Manager` VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS customer_succces_managers (
  `Regions` VARCHAR(255),
  `Manager` VARCHAR(255)
);

