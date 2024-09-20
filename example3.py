# Importing the SparkSession from PySpark
from pyspark.sql import SparkSession

# Creating a Spark session with a specific application name
spark = SparkSession.builder.appName('Example 3').getOrCreate()

# Reading a CSV file into a DataFrame, inferring the schema and using the first row as headers
df = spark.read.csv('data/test_with_null.csv', header=True, inferSchema=True)

# Displaying the DataFrame to inspect the data
df.show()

# Dropping the 'Name' column and showing the resulting DataFrame
df.drop('Name').show()

# Dropping rows with any null values and showing the resulting DataFrame
df.na.drop().show()

# Dropping rows that contain all null values and showing the resulting DataFrame
df.na.drop(how='all').show()

# Dropping rows with less than 3 non-null values and showing the resulting DataFrame
df.na.drop(how='any', thresh=3).show()

# Displaying the original DataFrame again for reference
df.show()

# Dropping rows with null values in the 'Experience' column and showing the resulting DataFrame
df.na.drop(how="any", subset=['Experience']).show()

# Filling null values with the string 'Missing Values' and showing the resulting DataFrame
df.na.fill('Missing Values').show()

# Filling null values in the 'Experience' and 'age' columns with 'Missing Values' and showing the resulting DataFrame
df.na.fill('Missing Values', ['Experience', 'age']).show()

# Importing the Imputer class from the PySpark ML library
from pyspark.ml.feature import Imputer

# Creating an Imputer instance to fill missing values with the mean for specified columns
imputer = Imputer(
    inputCols=['age', 'Experience', 'Salary'],
    outputCols=[f'{c}_imputed' for c in ['age', 'Experience', 'Salary']]
).setStrategy('mean')

# Fitting the imputer to the DataFrame and transforming it, then displaying the resulting DataFrame
imputer.fit(df).transform(df).show()

