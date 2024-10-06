from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("EmployeeAnalysis").getOrCreate()

# Load the CSV file into a DataFrame
df = spark.read.csv("employees.csv", header=True, inferSchema=True)

# Show the DataFrame
df.show()

# Filter employees in Engineering
engineering_employees = df.filter(df.department == "Engineering")
engineering_employees.show()

# Calculate the average age of employees in each department
avg_age_by_department = df.groupBy("department").avg("age")
avg_age_by_department.show()

# Stop the Spark session
spark.stop()
