from pyspark.sql import SparkSession

# Initialize a SparkSession
spark = SparkSession.builder \
    .appName("PySpark Example") \
    .enableHiveSupport().getOrCreate()

# Create a DataFrame to use in the table creation
data = [("John", 28), ("Doe", 32), ("Alice", 25), ("Bob", 45)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Register the DataFrame as a temporary view
df.createOrReplaceTempView("people_temp_view")

# Spark SQL commands

# 1. Create a table
spark.sql("""
CREATE TABLE IF NOT EXISTS people_table (
    Name STRING,
    Age INT
)
""")

# 2. Insert data into the table from the temp view
spark.sql("""
INSERT INTO people_table
SELECT Name, Age FROM people_temp_view
""")

# 3. Create a view from the table
spark.sql("""
CREATE VIEW IF NOT EXISTS people_view AS
SELECT * FROM people_table
""")

# 4. Alter the table by adding a new column
spark.sql("""
ALTER TABLE people_table
ADD COLUMNS (Address STRING)
""")

# 5. Update the new column with some data
# spark.sql("""
# UPDATE people_table
# SET Address = 'Unknown'
# """)

# 6. Drop the view
spark.sql("""
DROP VIEW IF EXISTS people_view
""")

# 7. Drop the table
spark.sql("""
DROP TABLE IF EXISTS people_table
""")

# Stop the Spark session
# spark.stop()
