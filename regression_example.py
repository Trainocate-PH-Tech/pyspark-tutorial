from pyspark.sql import SparkSession

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

spark = SparkSession.builder.appName('Regression Example').getOrCreate()

df = spark.read.csv('data/regression_example.csv', header=True, inferSchema=True)

df.printSchema()

independent_features = ['Age', 'Experience']

assembler = VectorAssembler(inputCols=independent_features, outputCol="Independent Features")

output = assembler.transform(df)

output.show()

sanitized_df = output.select("Independent Features", "Salary")

sanitized_df.show()

train_data, test_data = sanitized_df.randomSplit([0.75, 0.25])

regressor = LinearRegression(featuresCol='Independent Features', labelCol='Salary')
regressor = regressor.fit(train_data)

print(regressor.coefficients)

print(regressor.intercept)

result = regressor.evaluate(test_data)

result.predictions.show()

print(f"Mean Absolute Error: {result.meanAbsoluteError}")
print(f"Mean Squared Error: {result.meanSquaredError}")
