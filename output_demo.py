from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("PySpark DDL Commands") \
    .getOrCreate()
    
create_database_command = """
CREATE DATABASE IF NOT EXISTS DB_INVENTORY
"""
# Execute the command
spark.sql(create_database_command)
spark.sql("USE DB_INVENTORY")

# Create tables
spark.sql("""
    CREATE TABLE IF NOT EXISTS DB_INVENTORY.COSMA_FINAL_PER_DAY_CONS_MFN (
        id INT,
        name STRING,
        position STRING,
        salary FLOAT,
        hire_date DATE
    )
    USING parquet
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS DB_INVENTORY.COSMA_FLIGHT_SCHEDULE (
        dept_id INT,
        dept_name STRING
    )
    USING parquet
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS DB_INVENTORY.COSMA_PASSENGER_INFO (
        sale_id INT,
        product_id INT,
        amount DECIMAL(10, 2),
        sale_date DATE
    )
    USING parquet
    PARTITIONED BY (sale_date)
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS DB_INVENTORY.COSMA_BOOKING_DETAILS (
        product_id INT,
        product_name STRING,
        price DECIMAL(10, 2)
    )
    USING parquet
""")
try:
    spark.sql("""
        CREATE TABLE IF NOT EXISTS DB_INVENTORY.COSMA_AIRPORT_INFO (
            review_id INT,
            customer_id FLOAT,
            product_id INT,
            review_text STRING,
            rating INT
        )
        USING parquet
    """)
except Exception as e:
    print(e)
    # pass
try:
    spark.sql("""
        CREATE TABLE IF NOT EXISTS DB_INVENTORY.COSMA_FLIGHT_STATUS (
            order_id INT,
            customer_id INT,
            order_date DATE,
            total_amount DECIMAL(10, 2)
        )
        USING parquet
        PARTITIONED BY (order_date)
    """)
except Exception as e:
    print(e)

spark.sql("""
    CREATE TABLE IF NOT EXISTS DB_INVENTORY.COSMA_AIRLINE_INFO (
        supplier_id INT,
        supplier_name STRING,
        contact_name STRING
    )
    USING parquet
""")

# Create views
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW DB_INVENTORY.COSMA_AIRCRAFT_MAINTENANCE AS
    SELECT id, name, salary
    FROM DB_INVENTORY.COSMA_FINAL_PER_DAY_CONS_MFN
    WHERE salary > 50000
""")

spark.sql("""
    CREATE OR REPLACE GLOBAL TEMP VIEW global_temp_DB_INVENTORY.COSMA_PASSENGER_INFO_summary AS
    SELECT product_id, SUM(amount) AS total_DB_INVENTORY.COSMA_PASSENGER_INFO
    FROM DB_INVENTORY.COSMA_PASSENGER_INFO
    GROUP BY product_id
""")

spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW temp_DB_INVENTORY.COSMA_FLIGHT_SCHEDULE AS
    SELECT dept_id, dept_name
    FROM DB_INVENTORY.COSMA_FLIGHT_SCHEDULE
""")

# # Alter tables
# spark.sql("""
#     ALTER TABLE DB_INVENTORY.COSMA_FINAL_PER_DAY_CONS_MFN
#     ADD COLUMNS (department STRING)
# """)

# spark.sql("""
#     ALTER TABLE DB_INVENTORY.COSMA_FLIGHT_SCHEDULE
#     ADD COLUMNS (location STRING)
# """)

# spark.sql("""
#     ALTER TABLE DB_INVENTORY.COSMA_PASSENGER_INFO
#     ADD COLUMNS (discount DECIMAL(5, 2))
# """)

# spark.sql("""
#     ALTER TABLE DB_INVENTORY.COSMA_BOOKING_DETAILS
#     RENAME COLUMN price TO product_price
# """)

# spark.sql("""
#     ALTER TABLE DB_INVENTORY.COSMA_AIRPORT_INFO
#     ALTER COLUMN rating TYPE STRING
# """)

# Drop tables
spark.sql("""
    DROP TABLE IF EXISTS DB_INVENTORY.COSMA_FINAL_PER_DAY_CONS_MFN
""")

spark.sql("""
    DROP TABLE IF EXISTS DB_INVENTORY.COSMA_FLIGHT_SCHEDULE
""")

spark.sql("""
    DROP TABLE IF EXISTS DB_INVENTORY.COSMA_PASSENGER_INFO
""")

spark.sql("""
    DROP TABLE IF EXISTS DB_INVENTORY.COSMA_BOOKING_DETAILS
""")

spark.sql("""
    DROP TABLE IF EXISTS DB_INVENTORY.COSMA_AIRPORT_INFO
""")

spark.sql("""
    DROP TABLE IF EXISTS DB_INVENTORY.COSMA_FLIGHT_STATUS
""")

spark.sql("""
    DROP TABLE IF EXISTS DB_INVENTORY.COSMA_AIRLINE_INFO
""")

# Create more tables with variations
spark.sql("""
    CREATE TABLE IF NOT EXISTS DB_INVENTORY.COSMA_TRAVEL_AGENCY (
        emp_id INT,
        emp_name STRING,
        position STRING,
        department STRING
    )
    USING parquet
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS DB_INVENTORY.COSMA_SEAT_ALLOCATION (
        product_id INT,
        stock INT,
        warehouse_location STRING
    )
    USING parquet
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS DB_INVENTORY.COSMA_FOOD_SERVICE (
        order_id INT,
        product_id INT,
        quantity INT,
        unit_price DECIMAL(10, 2)
    )
    USING parquet
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS customer_DB_INVENTORY.COSMA_FLIGHT_STATUS (
        customer_id INT,
        order_id INT,
        order_date DATE,
        order_total DECIMAL(10, 2)
    )
    USING parquet
    PARTITIONED BY (order_date)
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS DB_INVENTORY.COSMA_VIP_LOUNGE_ACCESS (
        supplier_id INT,
        contact_name STRING,
        contact_email STRING
    )
    USING parquet
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS department_budget (
        dept_id INT,
        budget_amount DECIMAL(12, 2)
    )
    USING parquet
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS product_DB_INVENTORY.COSMA_PASSENGER_INFO (
        sale_id INT,
        product_id INT,
        sale_amount DECIMAL(10, 2),
        sale_date DATE
    )
    USING parquet
    PARTITIONED BY (sale_date)
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS customer_feedback (
        feedback_id INT,
        customer_id INT,
        feedback_text STRING
    )
    USING parquet
""")

# Create more views
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW DB_INVENTORY.COSMA_CREW_SCHEDULE AS
    SELECT order_id, SUM(quantity * unit_price) AS total_order_value
    FROM DB_INVENTORY.COSMA_FOOD_SERVICE
    GROUP BY order_id
""")

spark.sql("""
    CREATE OR REPLACE GLOBAL TEMP VIEW DB_INVENTORY.COSMA_PASSENGER_FEEDBACK AS
    SELECT product_id, COUNT(*) AS total_DB_INVENTORY.COSMA_PASSENGER_INFO, SUM(quantity) AS total_quantity_sold
    FROM DB_INVENTORY.COSMA_FOOD_SERVICE
    GROUP BY product_id
""")

spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW DB_INVENTORY.COSMA_BILLING_STATEMENTS AS
    SELECT supplier_id, COUNT(contact_name) AS total_contacts
    FROM DB_INVENTORY.COSMA_VIP_LOUNGE_ACCESS
    GROUP BY supplier_id
""")

spark.sql("""
    CREATE OR REPLACE GLOBAL TEMP VIEW global_temp_customer_DB_INVENTORY.COSMA_FLIGHT_STATUS_summary AS
    SELECT customer_id, COUNT(order_id) AS total_DB_INVENTORY.COSMA_FLIGHT_STATUS, SUM(order_total) AS total_amount_spent
    FROM customer_DB_INVENTORY.COSMA_FLIGHT_STATUS
    GROUP BY customer_id
""")

# Additional DDL commands
spark.sql("""
    CREATE TABLE IF NOT EXISTS DB_INVENTORY.COSMA_DUTY_FREE_PURCHASES (
        emp_id INT,
        pay_period STRING,
        gross_pay DECIMAL(10, 2),
        net_pay DECIMAL(10, 2)
    )
    USING parquet
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS DB_INVENTORY.COSMA_TRAVEL_INSURANCE (
        category_id INT,
        category_name STRING
    )
    USING parquet
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS DB_INVENTORY.COSMA_FLIGHT_ATTENDANT_LOGS (
        return_id INT,
        order_id INT,
        product_id INT,
        return_date DATE,
        return_amount DECIMAL(10, 2)
    )
    USING parquet
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS DB_INVENTORY.COSMA_CARGO_MANIFEST (
        customer_id INT,
        address STRING,
        city STRING,
        zip_code STRING
    )
    USING parquet
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS DB_INVENTORY.COSMA_VEHICLE_FLEET (
        invoice_id INT,
        supplier_id INT,
        invoice_date DATE,
        invoice_amount DECIMAL(12, 2)
    )
    USING parquet
    PARTITIONED BY (invoice_date)
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS department_DB_INVENTORY.COSMA_FINAL_PER_DAY_CONS_MFN (
        dept_id INT,
        emp_id INT,
        emp_name STRING
    )
    USING parquet
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS DB_INVENTORY.COSMA_PASSENGER_INFO_performance (
        performance_id INT,
        DB_INVENTORY.COSMA_PASSENGER_INFO_date DATE,
        total_DB_INVENTORY.COSMA_PASSENGER_INFO DECIMAL(10, 2),
        region STRING
    )
    USING parquet
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS DB_INVENTORY.COSMA_AIRLINE_TARIFFS (
        loyalty_id INT,
        customer_id INT,
        loyalty_points INT
    )
    USING parquet
""")

# # Continue with additional DDL operations
# spark.sql("""
#     ALTER TABLE DB_INVENTORY.COSMA_TRAVEL_AGENCY
#     ADD COLUMNS (employee_type STRING)
# """)

# spark.sql("""
#     ALTER TABLE DB_INVENTORY.COSMA_SEAT_ALLOCATION
#     ADD COLUMNS (reorder_level INT)
# """)

# spark.sql("""
#     ALTER TABLE DB_INVENTORY.COSMA_FOOD_SERVICE
#     RENAME COLUMN unit_price TO item_price
# """)

# spark.sql("""
#     ALTER TABLE customer_DB_INVENTORY.COSMA_FLIGHT_STATUS
#     ALTER COLUMN order_total TYPE DOUBLE
# """)

spark.sql("""
    DROP TABLE IF EXISTS DB_INVENTORY.COSMA_DUTY_FREE_PURCHASES
""")

spark.sql("""
    DROP TABLE IF EXISTS DB_INVENTORY.COSMA_TRAVEL_INSURANCE
""")

spark.sql("""
    DROP TABLE IF EXISTS DB_INVENTORY.COSMA_FLIGHT_ATTENDANT_LOGS
""")

spark.sql("""
    DROP TABLE IF EXISTS DB_INVENTORY.COSMA_CARGO_MANIFEST
""")

spark.sql("""
    DROP TABLE IF EXISTS DB_INVENTORY.COSMA_VEHICLE_FLEET
""")

spark.sql("""
    DROP TABLE IF EXISTS department_DB_INVENTORY.COSMA_FINAL_PER_DAY_CONS_MFN
""")

spark.sql("""
    DROP TABLE IF EXISTS DB_INVENTORY.COSMA_PASSENGER_INFO_performance
""")

spark.sql("""
    DROP TABLE IF EXISTS DB_INVENTORY.COSMA_AIRLINE_TARIFFS
""")

# Continue with additional tables and operations
spark.sql("""
    CREATE TABLE IF NOT EXISTS DB_INVENTORY.COSMA_CONTRACTED_SERVICES (
        warehouse_id INT,
        product_id INT,
        stock_quantity INT
    )
    USING parquet
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS DB_INVENTORY.COSMA_AIRLINE_PARTNERS (
        training_id INT,
        emp_id INT,
        training_date DATE,
        training_type STRING
    )
    USING parquet
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS DB_INVENTORY.COSMA_PASSENGER_INFO_team (
        team_id INT,
        team_name STRING,
        manager_id INT
    )
    USING parquet
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS DB_INVENTORY.COSMA_BOOKING_HISTORY (
        project_id INT,
        emp_id INT,
        role STRING,
        start_date DATE
    )
    USING parquet
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS DB_INVENTORY.COSMA_CHECKIN_COUNTERS (
        report_id INT,
        emp_id INT,
        report_date DATE,
        total_amount DECIMAL(10, 2)
    )
    USING parquet
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS DB_INVENTORY.COSMA_GATE_ASSIGNMENTS (
        ticket_id INT,
        customer_id INT,
        issue_description STRING,
        ticket_status STRING
    )
    USING parquet
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS DB_INVENTORY.COSMA_FLIGHT_ROUTINGS (
        return_id INT,
        order_id INT,
        product_id INT,
        return_reason STRING,
        return_date DATE
    )
    USING parquet
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS DB_INVENTORY.COSMA_PASSENGER_RANKINGS (
        contract_id INT,
        customer_id INT,
        service_type STRING,
        contract_start DATE,
        contract_end DATE
    )
    USING parquet
""")

# # Finalize with a few more operations
# spark.sql("""
#     ALTER TABLE DB_INVENTORY.COSMA_CONTRACTED_SERVICES
#     ADD COLUMNS (last_updated TIMESTAMP)
# """)

# spark.sql("""
#     ALTER TABLE DB_INVENTORY.COSMA_AIRLINE_PARTNERS
#     RENAME COLUMN training_type TO training_category
# """)

# spark.sql("""
#     ALTER TABLE DB_INVENTORY.COSMA_PASSENGER_INFO_team
#     ALTER COLUMN manager_id TYPE BIGINT
# """)

spark.sql("""
    DROP TABLE IF EXISTS DB_INVENTORY.COSMA_BOOKING_HISTORY
""")

spark.sql("""
    DROP TABLE IF EXISTS DB_INVENTORY.COSMA_CHECKIN_COUNTERS
""")

spark.sql("""
    DROP TABLE IF EXISTS DB_INVENTORY.COSMA_GATE_ASSIGNMENTS
""")

spark.sql("""
    DROP TABLE IF EXISTS DB_INVENTORY.COSMA_FLIGHT_ROUTINGS
""")

spark.sql("""
    DROP TABLE IF EXISTS DB_INVENTORY.COSMA_PASSENGER_RANKINGS
""")

# Clean up views
spark.sql("""
    DROP VIEW IF EXISTS DB_INVENTORY.COSMA_AIRCRAFT_MAINTENANCE
""")

spark.sql("""
    DROP VIEW IF EXISTS global_temp_DB_INVENTORY.COSMA_PASSENGER_INFO_summary
""")

spark.sql("""
    DROP VIEW IF EXISTS temp_DB_INVENTORY.COSMA_FLIGHT_SCHEDULE
""")

spark.sql("""
    DROP VIEW IF EXISTS DB_INVENTORY.COSMA_CREW_SCHEDULE
""")

spark.sql("""
    DROP VIEW IF EXISTS DB_INVENTORY.COSMA_PASSENGER_FEEDBACK
""")

spark.sql("""
    DROP VIEW IF EXISTS DB_INVENTORY.COSMA_BILLING_STATEMENTS
""")

spark.sql("""
    DROP VIEW IF EXISTS global_temp_customer_DB_INVENTORY.COSMA_FLIGHT_STATUS_summary
""")

tables_df = spark.sql("SHOW TABLES")
print("Created tables:")
for row in tables_df.collect():
    print(f"Table created: {row['tableName']}")
    