from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark session
spark = SparkSession.builder.appName("OptimizationExample").getOrCreate()

# ===========================
# Sample Data
# ===========================
# Let's assume we have two CSV files:
# 1. data.csv: Contains employee data
#    id,name,age,department
#    1,Alice,30,HR
#    2,Bob,35,Engineering
#    3,Charlie,25,Engineering
#    4,David,28,HR
#    5,Eve,40,Marketing
#    6,Frank,33,Engineering
# 
# 2. small_data.csv: Contains additional info
#    id,salary
#    1,70000
#    2,80000
#    3,60000
#    4,65000

# Load data from CSV files into DataFrames
df = spark.read.csv("data.csv", header=True, inferSchema=True)
small_df = spark.read.csv("small_data.csv", header=True, inferSchema=True)

# ===========================
# 1. Using the Catalyst Optimizer
# ===========================
df.createOrReplaceTempView("data_view")

# Optimized SQL query
optimized_query = spark.sql("""
SELECT department, COUNT(*) AS count, AVG(age) AS avg_age
FROM data_view
GROUP BY department
HAVING avg_age > 30
""")
print("===== Optimized Query Result =====")
optimized_query.show()

# ===========================
# 2. Broadcast Joins
# ===========================
# Broadcasting the smaller DataFrame
result_broadcast_join = df.join(F.broadcast(small_df), on="id", how="inner")
print("===== Broadcast Join Result =====")
result_broadcast_join.show()

# ===========================
# 3. Caching DataFrames
# ===========================
# Caching the DataFrame for reuse
df.cache()

# Performing some operations
count_over_30 = df.filter(df.age > 30).count()
print(f"Number of employees over 30: {count_over_30}")

# Grouping and showing average age by department
avg_age_by_department = df.groupBy("department").agg(F.avg("age"))
print("===== Average Age by Department =====")
avg_age_by_department.show()

# Unpersist the DataFrame when done
df.unpersist()

# ===========================
# 4. Partitioning DataFrames
# ===========================
# Repartition the DataFrame based on the 'department' column
df_repartitioned = df.repartition("department")

# Show the number of partitions
print(f"Number of partitions after repartitioning: {df_repartitioned.rdd.getNumPartitions()}")

# ===========================
# 5. Predicate Pus

