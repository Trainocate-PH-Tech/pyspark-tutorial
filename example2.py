# Import the SparkSession class from the pyspark.sql module
from pyspark.sql import SparkSession

# Create a SparkSession, which is the entry point for Spark functionality
spark = SparkSession.builder \
    .appName('Example 2') \  # Set the application name to 'Example 2'
    .getOrCreate()            # Create a new SparkSession or retrieve an existing one

# Read a CSV file into a DataFrame, specifying that the first row contains headers
df = spark.read.option('header', 'true').csv('data/test.csv')

# Display the contents of the DataFrame
df.show()

# Print the schema of the DataFrame to understand its structure
df.printSchema()

# Read the CSV file again, inferring the schema automatically
df = spark.read.csv('data/test.csv', header=True, inferSchema=True)

# Display the contents of the newly read DataFrame
df.show()

# Print the schema of the DataFrame to check data types
df.printSchema()

# Print the list of columns in the DataFrame
print(df.columns)

# Select and display the 'Name' column from the DataFrame
df.select('Name').show()

# Select and display the 'Name' and 'Experience' columns from the DataFrame
df.select(['Name', 'Experience']).show()

# Generate summary statistics for the DataFrame and display them
df.describe().show()

# Create a new DataFrame by adding 2 years to the 'Experience' column
df_result = df.withColumn('Experience After 2 years', df['Experience'] + 2)

# Display the original DataFrame
df.show()

# Display the DataFrame with the new 'Experience After 2 years' column
df_result.show()

# Drop the 'Experience After 2 years' column from the DataFrame
df_dropped_column = df_result.drop('Experience After 2 years')

# Display the DataFrame after dropping the column
df_dropped_column.show()

# Rename the 'Name' column to 'Employee'
df_renamed_columns = df.withColumnRenamed('Name', 'Employee')

# Display the DataFrame with the renamed column
df_renamed_columns.show()
