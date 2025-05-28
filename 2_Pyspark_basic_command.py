from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("SimpleApp") \
    .getOrCreate()

# Sample data
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]

# Create a DataFrame from the data
df = spark.createDataFrame(data, ["Name", "Age"])

# Show the DataFrame content
df.show()

# Stop the Spark session
spark.stop()