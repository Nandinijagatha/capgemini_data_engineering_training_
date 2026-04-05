from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. EXTRACT (Create Spark Session + Load Data)
spark = SparkSession.builder \
    .appName("Phase3A_DataCleaning") \
    .getOrCreate()

data = [
    (1, "Ravi", "Hyderabad", 25),
    (2, None, "Chennai", 32),
    (None, "Arun", "Hyderabad", 28),
    (4, "Meena", None, 30),
    (4, "Meena", None, 30),
    (5, "John", "Bangalore", -5)
]

columns = ["customer_id", "name", "city", "age"]

df = spark.createDataFrame(data, columns)

print("RAW DATA")
df.show()

print("Row Count Before Cleaning:", df.count())


# 2. TRANSFORM - CLEANING (Step-by-step pipeline thinking)

# Step 1: Remove invalid keys (customer_id is mandatory)
df_clean = df.dropna(subset=["customer_id"])

# Step 2: Remove duplicate records (data quality fix)
df_clean = df_clean.dropDuplicates()

# Step 3: Handle missing attributes (business-friendly defaults)
df_clean = df_clean.fillna({
    "name": "Unknown",
    "city": "Unknown"
})

# Step 4: Remove invalid business values (age must be valid)
df_clean = df_clean.filter(col("age") > 0)

print("CLEANED DATA")
df_clean.show()

print("Row Count After Cleaning:", df_clean.count())


# 3. VALIDATION LAYER (check correctness)

print("Customers per City:")
df_clean.groupBy("city").count().show()


# 4. LOAD (end of pipeline)
spark.stop()
