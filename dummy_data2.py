from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Spark SQL Example") \
    .getOrCreate()

# Drop databases if they exist
spark.sql("DROP DATABASE IF EXISTS retail_db CASCADE")
spark.sql("DROP DATABASE IF EXISTS hr_db CASCADE")

# Create databases
spark.sql("CREATE DATABASE IF NOT EXISTS retail_db")
spark.sql("CREATE DATABASE IF NOT EXISTS hr_db")

# Create tables in the retail database
spark.sql("""
    CREATE TABLE IF NOT EXISTS retail_db.products (
        product_id INT,
        product_name STRING,
        category STRING,
        price DECIMAL(10, 2),
        stock INT
    )
    USING delta
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS retail_db.sales (
        sale_id INT,
        product_id INT,
        sale_date DATE,
        quantity INT,
        total_amount DECIMAL(10, 2)
    )
    USING delta
""")

# Create tables in the HR database
spark.sql("""
    CREATE TABLE IF NOT EXISTS hr_db.employees (
        employee_id INT,
        first_name STRING,
        last_name STRING,
        department STRING,
        salary DECIMAL(10, 2),
        hire_date DATE
    )
    USING delta
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS hr_db.departments (
        department_id INT,
        department_name STRING
    )
    USING delta
""")

# Insert data into the retail database tables
spark.sql("""
    INSERT INTO retail_db.products VALUES
    (1, 'Laptop', 'Electronics', 999.99, 50),
    (2, 'Smartphone', 'Electronics', 499.99, 150),
    (3, 'Table', 'Furniture', 149.99, 75),
    (4, 'Chair', 'Furniture', 89.99, 200)
""")

spark.sql("""
    INSERT INTO retail_db.sales VALUES
    (1, 1, '2024-09-01', 2, 1999.98),
    (2, 2, '2024-09-01', 1, 499.99),
    (3, 3, '2024-09-02', 4, 599.96),
    (4, 4, '2024-09-03', 3, 269.97)
""")

# Insert data into the HR database tables
spark.sql("""
    INSERT INTO hr_db.employees VALUES
    (1, 'John', 'Doe', 'Sales', 55000.00, '2023-06-15'),
    (2, 'Jane', 'Smith', 'Engineering', 75000.00, '2022-03-01'),
    (3, 'Emily', 'Jones', 'HR', 65000.00, '2021-09-10'),
    (4, 'Michael', 'Brown', 'Marketing', 60000.00, '2024-01-20')
""")

spark.sql("""
    INSERT INTO hr_db.departments VALUES
    (1, 'Sales'),
    (2, 'Engineering'),
    (3, 'HR'),
    (4, 'Marketing')
""")

# Query data from the retail database
product_df = spark.sql("""
    SELECT * FROM retail_db.products
""")
product_df.show()

sales_df = spark.sql("""
    SELECT * FROM retail_db.sales
""")
sales_df.show()

# Query data from the HR database
employees_df = spark.sql("""
    SELECT * FROM hr_db.employees
""")
employees_df.show()

departments_df = spark.sql("""
    SELECT * FROM hr_db.departments
""")
departments_df.show()

# Join tables and perform transformations
product_sales_df = spark.sql("""
    SELECT p.product_name, s.sale_date, s.quantity, s.total_amount
    FROM retail_db.sales s
    JOIN retail_db.products p
    ON s.product_id = p.product_id
""")
product_sales_df.show()

salary_by_department_df = spark.sql("""
    SELECT e.department, SUM(e.salary) AS total_salary
    FROM hr_db.employees e
    GROUP BY e.department
""")
salary_by_department_df.show()

# Create a new table with aggregated data
spark.sql("""
    CREATE OR REPLACE TEMP VIEW department_salary AS
    SELECT e.department, SUM(e.salary) AS total_salary
    FROM hr_db.employees e
    GROUP BY e.department
""")

# Query the new table
spark.sql("""
    SELECT * FROM department_salary
""").show()

# Cleanup: Drop tables and databases if needed
spark.sql("DROP TABLE IF EXISTS retail_db.products")
spark.sql("DROP TABLE IF EXISTS retail_db.sales")
spark.sql("DROP TABLE IF EXISTS hr_db.employees")
spark.sql("DROP TABLE IF EXISTS hr_db.departments")
spark.sql("DROP DATABASE IF EXISTS retail_db CASCADE")
spark.sql("DROP DATABASE IF EXISTS hr_db CASCADE")

# Stop Spark session
spark.stop()




# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, avg, max as spark_max

# # Initialize a Spark session
# spark = SparkSession.builder \
#     .appName("Complex PySpark and Spark SQL Example") \
#     .getOrCreate()

# # Sample Data
# data = [
#     ("Alice", "HR", 34, 50000),
#     ("Bob", "Finance", 45, 70000),
#     ("Cathy", "HR", 29, 60000),
#     ("David", "Finance", 36, 80000),
#     ("Eva", "IT", 41, 85000),
#     ("Frank", "IT", 50, 95000),
#     ("Grace", "Finance", 33, 76000),
#     ("Hannah", "HR", 38, 54000)
# ]

# # Create a DataFrame
# columns = ["Name", "Department", "Age", "Salary"]
# df = spark.createDataFrame(data, columns)

# # Show the DataFrame
# df.show()

# # Calculate average salary by department using PySpark
# avg_salary_df = df.groupBy("Department").agg(avg("Salary").alias("Avg_Salary"))
# avg_salary_df.show()

# # Calculate maximum salary by department using PySpark
# max_salary_df = df.groupBy("Department").agg(spark_max("Salary").alias("Max_Salary"))
# max_salary_df.show()

# # Register the DataFrame as a temporary view for SQL queries
# df.createOrReplaceTempView("employee_data")

# # Calculate average age by department using Spark SQL
# avg_age_sql = spark.sql("""
# SELECT Department, AVG(Age) AS Avg_Age
# FROM employee_data
# GROUP BY Department
# """)
# avg_age_sql.show()

# # Find employees with salary greater than 75000 using Spark SQL
# high_salary_sql = spark.sql("""
# SELECT Name, Department, Salary
# FROM employee_data
# WHERE Salary > 75000
# ORDER BY Salary DESC
# """)
# high_salary_sql.show()

# # Add a new column "Seniority" based on age using PySpark
# df_with_seniority = df.withColumn("Seniority", col("Age") >= 40)
# df_with_seniority.show()

# # Register the modified DataFrame as another temporary view for SQL queries
# df_with_seniority.createOrReplaceTempView("employee_data_with_seniority")

# # Select only senior employees using Spark SQL
# senior_employees_sql = spark.sql("""
# SELECT Name, Department, Age, Salary
# FROM employee_data_with_seniority
# WHERE Seniority = TRUE
# """)
# senior_employees_sql.show()

# # Complex SQL Query: Join average salary and senior employees
# complex_query_sql = spark.sql("""
# SELECT e.Name, e.Department, e.Age, e.Salary, a.Avg_Salary
# FROM employee_data_with_seniority e
# JOIN (
#     SELECT Department, AVG(Salary) AS Avg_Salary
#     FROM employee_data
#     GROUP BY Department
# ) a
# ON e.Department = a.Department
# WHERE e.Salary > a.Avg_Salary AND e.Seniority = TRUE
# """)
# complex_query_sql.show()

# # Stop the Spark session
# spark.stop()

