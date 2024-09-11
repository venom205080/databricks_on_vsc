from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("PySpark DDL Commands") \
    .getOrCreate()

# Create tables
spark.sql("""
    CREATE TABLE IF NOT EXISTS employees (
        id INT,
        name STRING,
        position STRING,
        salary FLOAT,
        hire_date DATE
    )
    USING parquet
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS departments (
        dept_id INT,
        dept_name STRING
    )
    USING parquet
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS sales (
        sale_id INT,
        product_id INT,
        amount DECIMAL(10, 2),
        sale_date DATE
    )
    USING parquet
    PARTITIONED BY (sale_date)
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS products (
        product_id INT,
        product_name STRING,
        price DECIMAL(10, 2)
    )
    USING parquet
""")
try:
    spark.sql("""
        CREATE TABLE IF NOT EXISTS customer_reviews (
            review_id INT,
            customer_id FLOAT,
            product_id INT,
            review_text VARCHAR,
            rating INT
        )
        USING parquet
    """)
except Exception as e:
    print(e)
    # pass
try:
    spark.sql("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id INT,
            customer_id INT,
            order_date DATETime,
            total_amount DECIMAL(10, 2)
        )
        USING parquet
        PARTITIONED BY (order_date)
    """)
except Exception as e:
    print(e)

spark.sql("""
    CREATE TABLE IF NOT EXISTS suppliers (
        supplier_id INT,
        supplier_name STRING,
        contact_name STRING
    )
    USING parquet
""")

# Create views
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW temp_employee_salaries AS
    SELECT id, name, salary
    FROM employees
    WHERE salary > 50000
""")

spark.sql("""
    CREATE OR REPLACE GLOBAL TEMP VIEW global_temp_sales_summary AS
    SELECT product_id, SUM(amount) AS total_sales
    FROM sales
    GROUP BY product_id
""")

spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW temp_departments AS
    SELECT dept_id, dept_name
    FROM departments
""")

# # Alter tables
# spark.sql("""
#     ALTER TABLE employees
#     ADD COLUMNS (department STRING)
# """)

# spark.sql("""
#     ALTER TABLE departments
#     ADD COLUMNS (location STRING)
# """)

# spark.sql("""
#     ALTER TABLE sales
#     ADD COLUMNS (discount DECIMAL(5, 2))
# """)

# spark.sql("""
#     ALTER TABLE products
#     RENAME COLUMN price TO product_price
# """)

# spark.sql("""
#     ALTER TABLE customer_reviews
#     ALTER COLUMN rating TYPE STRING
# """)

# Drop tables
spark.sql("""
    DROP TABLE IF EXISTS employees
""")

spark.sql("""
    DROP TABLE IF EXISTS departments
""")

spark.sql("""
    DROP TABLE IF EXISTS sales
""")

spark.sql("""
    DROP TABLE IF EXISTS products
""")

spark.sql("""
    DROP TABLE IF EXISTS customer_reviews
""")

spark.sql("""
    DROP TABLE IF EXISTS orders
""")

spark.sql("""
    DROP TABLE IF EXISTS suppliers
""")

# Create more tables with variations
spark.sql("""
    CREATE TABLE IF NOT EXISTS employee_details (
        emp_id INT,
        emp_name STRING,
        position STRING,
        department STRING
    )
    USING parquet
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS product_inventory (
        product_id INT,
        stock INT,
        warehouse_location STRING
    )
    USING parquet
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS order_details (
        order_id INT,
        product_id INT,
        quantity INT,
        unit_price DECIMAL(10, 2)
    )
    USING parquet
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS customer_orders (
        customer_id INT,
        order_id INT,
        order_date DATE,
        order_total DECIMAL(10, 2)
    )
    USING parquet
    PARTITIONED BY (order_date)
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS supplier_contacts (
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
    CREATE TABLE IF NOT EXISTS product_sales (
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
    CREATE OR REPLACE TEMPORARY VIEW temp_order_summary AS
    SELECT order_id, SUM(quantity * unit_price) AS total_order_value
    FROM order_details
    GROUP BY order_id
""")

spark.sql("""
    CREATE OR REPLACE GLOBAL TEMP VIEW global_temp_product_summary AS
    SELECT product_id, COUNT(*) AS total_sales, SUM(quantity) AS total_quantity_sold
    FROM order_details
    GROUP BY product_id
""")

spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW temp_supplier_summary AS
    SELECT supplier_id, COUNT(contact_name) AS total_contacts
    FROM supplier_contacts
    GROUP BY supplier_id
""")

spark.sql("""
    CREATE OR REPLACE GLOBAL TEMP VIEW global_temp_customer_orders_summary AS
    SELECT customer_id, COUNT(order_id) AS total_orders, SUM(order_total) AS total_amount_spent
    FROM customer_orders
    GROUP BY customer_id
""")

# Additional DDL commands
spark.sql("""
    CREATE TABLE IF NOT EXISTS employee_payroll (
        emp_id INT,
        pay_period STRING,
        gross_pay DECIMAL(10, 2),
        net_pay DECIMAL(10, 2)
    )
    USING parquet
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS product_categories (
        category_id INT,
        category_name STRING
    )
    USING parquet
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS order_returns (
        return_id INT,
        order_id INT,
        product_id INT,
        return_date DATE,
        return_amount DECIMAL(10, 2)
    )
    USING parquet
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS customer_addresses (
        customer_id INT,
        address STRING,
        city STRING,
        zip_code STRING
    )
    USING parquet
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS supplier_invoices (
        invoice_id INT,
        supplier_id INT,
        invoice_date DATE,
        invoice_amount DECIMAL(12, 2)
    )
    USING parquet
    PARTITIONED BY (invoice_date)
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS department_employees (
        dept_id INT,
        emp_id INT,
        emp_name STRING
    )
    USING parquet
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS sales_performance (
        performance_id INT,
        sales_date DATE,
        total_sales DECIMAL(10, 2),
        region STRING
    )
    USING parquet
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS customer_loyalty (
        loyalty_id INT,
        customer_id INT,
        loyalty_points INT
    )
    USING parquet
""")

# # Continue with additional DDL operations
# spark.sql("""
#     ALTER TABLE employee_details
#     ADD COLUMNS (employee_type STRING)
# """)

# spark.sql("""
#     ALTER TABLE product_inventory
#     ADD COLUMNS (reorder_level INT)
# """)

# spark.sql("""
#     ALTER TABLE order_details
#     RENAME COLUMN unit_price TO item_price
# """)

# spark.sql("""
#     ALTER TABLE customer_orders
#     ALTER COLUMN order_total TYPE DOUBLE
# """)

spark.sql("""
    DROP TABLE IF EXISTS employee_payroll
""")

spark.sql("""
    DROP TABLE IF EXISTS product_categories
""")

spark.sql("""
    DROP TABLE IF EXISTS order_returns
""")

spark.sql("""
    DROP TABLE IF EXISTS customer_addresses
""")

spark.sql("""
    DROP TABLE IF EXISTS supplier_invoices
""")

spark.sql("""
    DROP TABLE IF EXISTS department_employees
""")

spark.sql("""
    DROP TABLE IF EXISTS sales_performance
""")

spark.sql("""
    DROP TABLE IF EXISTS customer_loyalty
""")

# Continue with additional tables and operations
spark.sql("""
    CREATE TABLE IF NOT EXISTS warehouse_inventory (
        warehouse_id INT,
        product_id INT,
        stock_quantity INT
    )
    USING parquet
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS employee_training (
        training_id INT,
        emp_id INT,
        training_date DATE,
        training_type STRING
    )
    USING parquet
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS sales_team (
        team_id INT,
        team_name STRING,
        manager_id INT
    )
    USING parquet
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS project_assignments (
        project_id INT,
        emp_id INT,
        role STRING,
        start_date DATE
    )
    USING parquet
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS expense_reports (
        report_id INT,
        emp_id INT,
        report_date DATE,
        total_amount DECIMAL(10, 2)
    )
    USING parquet
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS customer_support_tickets (
        ticket_id INT,
        customer_id INT,
        issue_description STRING,
        ticket_status STRING
    )
    USING parquet
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS product_returns (
        return_id INT,
        order_id INT,
        product_id INT,
        return_reason STRING,
        return_date DATE
    )
    USING parquet
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS service_contracts (
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
#     ALTER TABLE warehouse_inventory
#     ADD COLUMNS (last_updated TIMESTAMP)
# """)

# spark.sql("""
#     ALTER TABLE employee_training
#     RENAME COLUMN training_type TO training_category
# """)

# spark.sql("""
#     ALTER TABLE sales_team
#     ALTER COLUMN manager_id TYPE BIGINT
# """)

spark.sql("""
    DROP TABLE IF EXISTS project_assignments
""")

spark.sql("""
    DROP TABLE IF EXISTS expense_reports
""")

spark.sql("""
    DROP TABLE IF EXISTS customer_support_tickets
""")

spark.sql("""
    DROP TABLE IF EXISTS product_returns
""")

spark.sql("""
    DROP TABLE IF EXISTS service_contracts
""")

# Clean up views
spark.sql("""
    DROP VIEW IF EXISTS temp_employee_salaries
""")

spark.sql("""
    DROP VIEW IF EXISTS global_temp_sales_summary
""")

spark.sql("""
    DROP VIEW IF EXISTS temp_departments
""")

spark.sql("""
    DROP VIEW IF EXISTS temp_order_summary
""")

spark.sql("""
    DROP VIEW IF EXISTS global_temp_product_summary
""")

spark.sql("""
    DROP VIEW IF EXISTS temp_supplier_summary
""")

spark.sql("""
    DROP VIEW IF EXISTS global_temp_customer_orders_summary
""")

tables_df = spark.sql("SHOW TABLES")
print("Created tables:")
for row in tables_df.collect():
    print(f"Table created: {row['tableName']}")
    