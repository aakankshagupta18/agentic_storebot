CREATE SCHEMA IF NOT EXISTS sales;
CREATE SCHEMA IF NOT EXISTS ref;

-- Orders (goes into Postgres for variety)
CREATE TABLE IF NOT EXISTS sales.orders (
  "Row ID" TEXT,
  "Order ID" TEXT,
  "Order Date" DATE,
  "Ship Date" DATE,
  "Ship Mode" TEXT,
  "Customer ID" TEXT,
  "Customer Name" TEXT,
  "Segment" TEXT,
  "Country/Region" TEXT,
  "City" TEXT,
  "State/Province" TEXT,
  "Postal Code" TEXT,
  "Region" TEXT,
  "Product ID" TEXT,
  "Category" TEXT,
  "Sub-Category" TEXT,
  "Product Name" TEXT
);

-- Returns
CREATE TABLE IF NOT EXISTS ref.returns (
  "Returned" TEXT,
  "ID" TEXT
);

-- Regional Managers
CREATE TABLE IF NOT EXISTS ref.regional_managers (
  "Regional Manager" TEXT,
  "Regions" TEXT
);

-- State_Managers
CREATE TABLE IF NOT EXISTS ref.state_managers (
  "State/Province" TEXT,
  "Manager" TEXT
);

-- Segment_Managers
CREATE TABLE IF NOT EXISTS ref.segment_managers (
  "Segment" TEXT,
  "Manager" TEXT
);

-- Category_Managers
CREATE TABLE IF NOT EXISTS ref.category_managers (
  "Category" TEXT,
  "Manager" TEXT
);

-- Customer_Succces_Managers
CREATE TABLE IF NOT EXISTS ref.customer_succces_managers (
  "Regions" TEXT,
  "Manager" TEXT
);

