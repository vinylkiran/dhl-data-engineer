# Data Dictionary — DHL SKU Segmentation Warehouse
**Project:** 01 — SKU Segmentation Pipeline  
**Author:** Vinyl Kiran Anipe (Data Engineer)  
**Version:** 1.0 · 2024

---

## dim_date

| Column | Data Type | Business Definition | Source Field | Transformation | Example | Constraints |
|---|---|---|---|---|---|---|
| `date_key` | INTEGER | Surrogate key in YYYYMMDD format | Programmatic | `strftime('%Y%m%d', full_date)` cast to int | 20220115 | PK, NOT NULL |
| `full_date` | DATE | The calendar date | Programmatic | `pd.date_range('2022-01-01', '2023-12-31')` | 2022-01-15 | UNIQUE, NOT NULL |
| `day_of_week` | INTEGER | Day of week (1=Monday, 7=Sunday) | Programmatic | `dt.dayofweek + 1` | 6 | NOT NULL |
| `day_name` | VARCHAR | Full name of the day | Programmatic | `dt.day_name()` | Saturday | NOT NULL |
| `day_of_month` | INTEGER | Day number within the month | Programmatic | `dt.day` | 15 | NOT NULL |
| `day_of_year` | INTEGER | Day number within the year | Programmatic | `dt.dayofyear` | 15 | NOT NULL |
| `week_of_year` | INTEGER | ISO week number | Programmatic | `dt.isocalendar().week` | 2 | NOT NULL |
| `month_num` | INTEGER | Month number (1–12) | Programmatic | `dt.month` | 1 | NOT NULL |
| `month_name` | VARCHAR | Full name of the month | Programmatic | `dt.month_name()` | January | NOT NULL |
| `quarter` | INTEGER | Calendar quarter (1–4) | Programmatic | `dt.quarter` | 1 | NOT NULL, CHECK IN (1,2,3,4) |
| `year` | INTEGER | Calendar year | Programmatic | `dt.year` | 2022 | NOT NULL |
| `is_weekend` | BOOLEAN | True if Saturday or Sunday | Programmatic | `day_of_week IN (6,7)` | False | NOT NULL |
| `is_month_start` | BOOLEAN | True if first day of month | Programmatic | `dt.is_month_start` | False | NOT NULL |
| `is_month_end` | BOOLEAN | True if last day of month | Programmatic | `dt.is_month_end` | False | NOT NULL |
| `is_quarter_end` | BOOLEAN | True if last day of quarter | Programmatic | `dt.is_quarter_end` | False | NOT NULL |
| `season` | VARCHAR | Northern Hemisphere season | Programmatic | Spring(Mar-May), Summer(Jun-Aug), Autumn(Sep-Nov), Winter(Dec-Feb) | Winter | NOT NULL, CHECK IN ('Spring','Summer','Autumn','Winter') |
| `etl_loaded_at` | TIMESTAMP | UTC timestamp when this row was loaded | System | `datetime.utcnow()` | 2024-01-15T10:00:00 | NOT NULL |
| `etl_source_file` | VARCHAR | Source of this row | System | Hard-coded: "programmatic" | programmatic | NOT NULL |

---

## dim_warehouse

| Column | Data Type | Business Definition | Source Field | Transformation | Example | Constraints |
|---|---|---|---|---|---|---|
| `warehouse_key` | INTEGER | Surrogate key | Programmatic | Sequential integer from 1 | 1 | PK, NOT NULL |
| `warehouse_id` | VARCHAR | DHL warehouse identifier | Programmatic | Hard-coded from known site IDs | DHL-WH-IL02 | UNIQUE, NOT NULL |
| `warehouse_name` | VARCHAR | Full descriptive warehouse name | Programmatic | Hard-coded | DHL Warehouse Illinois 02 | NOT NULL |
| `city` | VARCHAR | City where warehouse is located | Programmatic | Hard-coded | Chicago | NOT NULL |
| `state` | VARCHAR | US state | Programmatic | Hard-coded | Illinois | NOT NULL |
| `region` | VARCHAR | US geographic region | Programmatic | Hard-coded (Midwest/Northeast/South) | Midwest | NOT NULL |
| `country` | VARCHAR | Country | Programmatic | Hard-coded: USA | USA | NOT NULL |
| `timezone` | VARCHAR | IANA timezone string | Programmatic | Hard-coded | America/Chicago | NOT NULL |
| `active_flag` | BOOLEAN | Whether warehouse is currently active | Programmatic | Hard-coded: True | True | NOT NULL |
| `etl_loaded_at` | TIMESTAMP | UTC load timestamp | System | `datetime.utcnow()` | 2024-01-15T10:00:00 | NOT NULL |
| `etl_source_file` | VARCHAR | Source of this row | System | Hard-coded: "programmatic" | programmatic | NOT NULL |

---

## dim_supplier

| Column | Data Type | Business Definition | Source Field | Transformation | Example | Constraints |
|---|---|---|---|---|---|---|
| `supplier_key` | INTEGER | Surrogate key | Programmatic | Sequential integer, sorted by Supplier_ID | 1 | PK, NOT NULL |
| `supplier_id` | VARCHAR | DHL supplier identifier | `Supplier_ID` | snake_case rename | SUP-0001 | UNIQUE, NOT NULL |
| `supplier_name` | VARCHAR | Full supplier company name | `Supplier_Name` | snake_case rename | GTO Logistics Ltd | NOT NULL |
| `country` | VARCHAR | Supplier country of origin | `Country` | snake_case rename | USA | NOT NULL |
| `category_focus` | VARCHAR | Primary product category the supplier serves | `Category_Focus` | snake_case rename | Industrial | — |
| `lead_time_avg_days` | DECIMAL(6,2) | Average supplier lead time in days | `Lead_Time_Avg_Days` | Cast to decimal | 12.00 | — |
| `lead_time_std_days` | DECIMAL(6,2) | Standard deviation of lead time | `Lead_Time_Std_Days` | Cast to decimal | 1.00 | — |
| `otif_rate` | DECIMAL(6,4) | On-time in-full delivery rate (0–1) | `OTIF_Rate` | Cast to decimal | 0.9370 | — |
| `fill_rate` | DECIMAL(6,4) | Proportion of orders fully filled (0–1) | `Fill_Rate` | Cast to decimal | 0.8780 | — |
| `defect_rate` | DECIMAL(6,4) | Proportion of units with defects (0–1) | `Defect_Rate` | Cast to decimal | 0.0144 | — |
| `active_flag` | BOOLEAN | Whether supplier is currently active | `Active_Flag` | Cast to boolean | True | NOT NULL |
| `etl_loaded_at` | TIMESTAMP | UTC load timestamp | System | `datetime.utcnow()` | 2024-01-15T10:00:00 | NOT NULL |
| `etl_source_file` | VARCHAR | Source CSV filename | System | Hard-coded: "suppliers.csv" | suppliers.csv | NOT NULL |

---

## dim_sku

| Column | Data Type | Business Definition | Source Field | Transformation | Example | Constraints |
|---|---|---|---|---|---|---|
| `sku_key` | INTEGER | Surrogate key | Programmatic | Sequential integer, sorted by SKU_ID | 1 | PK, NOT NULL |
| `sku_id` | VARCHAR | DHL SKU identifier | `SKU_ID` | snake_case rename | HLT-000001 | UNIQUE, NOT NULL |
| `category` | VARCHAR | Product category | `Category` | snake_case rename | Healthcare | NOT NULL |
| `abc_class` | VARCHAR(1) | ABC revenue classification (A/B/C) | `ABC_Class` | snake_case rename | C | NOT NULL, CHECK IN ('A','B','C') |
| `xyz_class` | VARCHAR(1) | XYZ demand variability class (X/Y/Z) | `XYZ_Class` | snake_case rename; NULL if not present in source | Y | CHECK IN ('X','Y','Z') |
| `unit_cost` | DECIMAL(12,4) | Unit procurement cost in USD | `Unit_Cost` | Cast to decimal | 18.3000 | NOT NULL |
| `unit_price` | DECIMAL(12,4) | Unit selling price in USD | `Unit_Price` | Cast to decimal | 31.6000 | NOT NULL |
| `weight_kg` | DECIMAL(10,4) | Unit weight in kilograms | `Weight_KG` | Cast to decimal | 1.0430 | — |
| `volume_cbm` | DECIMAL(10,4) | Unit volume in cubic metres | `Volume_CBM` | Cast to decimal | 0.1914 | — |
| `storage_type` | VARCHAR | Required storage type | `Storage_Type` | snake_case rename | Ambient | NOT NULL, CHECK IN ('Ambient','Bulk','Controlled','Hazmat') |
| `supplier_id` | VARCHAR | Reference to dim_supplier natural key | `Supplier_ID` | snake_case rename | SUP-0048 | — |
| `lead_time_days` | INTEGER | Days from order to receipt | `Lead_Time_Days` | Cast to Int64 | 13 | — |
| `min_order_qty` | INTEGER | Minimum order quantity (units) | `Min_Order_Qty` | Cast to Int64 | 1 | — |
| `safety_stock_qty` | INTEGER | Safety stock quantity (units) — BA/DA calculated | `Safety_Stock_Qty` | Cast to Int64 | 134 | — |
| `reorder_point_qty` | INTEGER | Reorder point quantity (units) — BA/DA calculated | `Reorder_Point_Qty` | Cast to Int64 | 202 | — |
| `primary_warehouse` | VARCHAR | Warehouse where this SKU is primarily stocked | `Primary_Warehouse` | snake_case rename | DHL-WH-IL02 | — |
| `active_flag` | BOOLEAN | Whether SKU is currently active | `Active_Flag` | Cast to boolean | False | NOT NULL |
| `etl_loaded_at` | TIMESTAMP | UTC load timestamp | System | `datetime.utcnow()` | 2024-01-15T10:00:00 | NOT NULL |
| `etl_source_file` | VARCHAR | Source CSV filename | System | Hard-coded: "sku_master.csv" | sku_master.csv | NOT NULL |

---

## fact_daily_demand

| Column | Data Type | Business Definition | Source Field | Transformation | Example | Constraints |
|---|---|---|---|---|---|---|
| `demand_key` | BIGINT | Surrogate key | Programmatic | Sequential integer | 1 | PK, NOT NULL |
| `date_key` | INTEGER | FK to dim_date | `Date` | Parse to date → YYYYMMDD integer join | 20220102 | FK → dim_date, NOT NULL |
| `sku_key` | INTEGER | FK to dim_sku | `SKU_ID` | Join to dim_sku on sku_id | 142 | FK → dim_sku, NOT NULL |
| `warehouse_key` | INTEGER | FK to dim_warehouse | `Warehouse_ID` | Join to dim_warehouse on warehouse_id | 2 | FK → dim_warehouse, NOT NULL |
| `abc_class` | VARCHAR(1) | ABC class at time of demand event | `ABC_Class` | Direct copy (degenerate dimension) | B | — |
| `xyz_class` | VARCHAR(1) | XYZ class at time of demand event | `XYZ_Class` | Direct copy (degenerate dimension) | X | — |
| `quantity_demanded` | INTEGER | Units demanded by customers | `Quantity_Demanded` | Cast to int, null → 0 | 16 | NOT NULL DEFAULT 0 |
| `quantity_fulfilled` | INTEGER | Units actually shipped | `Quantity_Fulfilled` | Cast to int, null → 0 | 16 | NOT NULL DEFAULT 0 |
| `quantity_unfulfilled` | INTEGER | Units not fulfilled (max 0) | Derived | `MAX(0, demanded - fulfilled)` | 0 | NOT NULL DEFAULT 0 |
| `stockout_flag` | BOOLEAN | True if any stockout occurred this day | `Stockout_Flag` | Cast to boolean | False | NOT NULL DEFAULT FALSE |
| `revenue` | DECIMAL(14,4) | Revenue generated in USD | `Revenue` | Cast to decimal; null → 0 (stockout business rule) | 102.4000 | NOT NULL DEFAULT 0 |
| `fill_rate` | DECIMAL(6,4) | fulfilled / demanded (null if demanded=0) | Derived | `fulfilled / demanded` where demanded > 0 | 1.0000 | — |
| `etl_loaded_at` | TIMESTAMP | UTC load timestamp | System | `datetime.utcnow()` | 2024-01-15T10:00:00 | NOT NULL |
| `etl_source_file` | VARCHAR | Source CSV filename | System | Hard-coded: "daily_demand.csv" | daily_demand.csv | NOT NULL |

---

## fact_inventory_snapshot

| Column | Data Type | Business Definition | Source Field | Transformation | Example | Constraints |
|---|---|---|---|---|---|---|
| `snapshot_key` | BIGINT | Surrogate key | Programmatic | Sequential integer | 1 | PK, NOT NULL |
| `date_key` | INTEGER | FK to dim_date (month-end date) | `Snapshot_Date` | Parse to date → YYYYMMDD integer join | 20220131 | FK → dim_date, NOT NULL |
| `sku_key` | INTEGER | FK to dim_sku | `SKU_ID` | Join to dim_sku on sku_id | 899 | FK → dim_sku, NOT NULL |
| `warehouse_key` | INTEGER | FK to dim_warehouse | `Warehouse_ID` | Join to dim_warehouse on warehouse_id | 3 | FK → dim_warehouse, NOT NULL |
| `on_hand_qty` | INTEGER | Total units physically present in warehouse | `On_Hand_Qty` | Cast to int, null → 0 | 535 | NOT NULL DEFAULT 0 |
| `in_transit_qty` | INTEGER | Units ordered but not yet received | `In_Transit_Qty` | Cast to int, null → 0 | 24 | NOT NULL DEFAULT 0 |
| `committed_qty` | INTEGER | Units reserved for open customer orders | `Committed_Qty` | Cast to int, null → 0 | 106 | NOT NULL DEFAULT 0 |
| `available_qty` | INTEGER | Units available for new orders (on_hand - committed) | `Available_Qty` | Cast to int, null → 0 | 429 | NOT NULL DEFAULT 0 |
| `inventory_value` | DECIMAL(14,4) | Total inventory value in USD (on_hand × unit_cost) | `Inventory_Value` | Cast to decimal, null → 0 | 3755.7000 | NOT NULL DEFAULT 0 |
| `inventory_record_accuracy` | DECIMAL(6,4) | available / on_hand (null if on_hand=0) | Derived | `available / on_hand` where on_hand > 0 | 0.8019 | — |
| `etl_loaded_at` | TIMESTAMP | UTC load timestamp | System | `datetime.utcnow()` | 2024-01-15T10:00:00 | NOT NULL |
| `etl_source_file` | VARCHAR | Source CSV filename | System | Hard-coded: "inventory_snapshot.csv" | inventory_snapshot.csv | NOT NULL |

---

## Audit Columns (all tables)

Every table in the warehouse includes two audit columns:

| Column | Purpose | Value |
|---|---|---|
| `etl_loaded_at` | Records the UTC timestamp of the ETL run that loaded this row. Enables point-in-time auditing and identification of stale data. | UTC datetime at pipeline start |
| `etl_source_file` | Records which source file this row originated from. Enables traceability from warehouse row back to source record. | CSV filename or "programmatic" for built dimensions |

---

*Data Dictionary v1.0 · Vinyl Kiran Anipe · DHL Data Engineer Portfolio · Project 01 · 2024*
