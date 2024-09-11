from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("PySpark DDL Commands") \
    .getOrCreate()
    
    
create_database_command = """
CREATE DATABASE IF NOT EXISTS my_database
"""
# Execute the command
spark.sql(create_database_command)
spark.sql("USE my_database")

spark.sql("""
    CREATE TABLE IF NOT EXISTS my_database.employees (
        id INT,
        name STRING,
        position STRING,
        salary FLOAT,
        hire_date DATE
    )
    USING parquet
""")


print("Database 'my_database' created successfully.")
tables_df = spark.sql("SHOW TABLES")
print("Created tables:")
for row in tables_df.collect():
    print(f"Table created: {row['tableName']}")
    