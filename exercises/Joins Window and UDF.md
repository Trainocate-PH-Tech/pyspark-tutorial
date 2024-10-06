# Joins Windows and UDF

## Exercise 1

You are given two datasets, `employees` and `salaries`. You will join the datasets to analyze employee salary trends, use a user-defined function (UDF) to calculate bonus percentages, and apply window functions to rank employees based on their salaries.

### Instructions

1. Load Data

2. Perform a join: Join the `employees` DataFrame with the `salaries` DataFrame on `employee_id`.

3. Use a UDF to Calculate the Bonus Amount:

* Define a UDF that calculates the bonus amount (`salary * bonus_percentage / 100`).
* Apply this UDF to a new column `bonus_amount`.

4. Apply window functions:

* Use a window function to rank employees by salary within their departments.
* Create a column `salary_rank` that shows the rank of each employee’s salary within their department.

5. Filter Data:

* Filter out employees who were hired after 2020-01-01

6. Output:

Show the final DataFrame with `employee_id`, `name`, `department`, `salary`, `bonus_amount`, and `salary_rank`.

### Code (Solution)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import FloatType
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("EmployeeSalaryAnalysis").getOrCreate()

# Sample data
employees_data = [
    (1, 'Alice', 'Sales', '2020-01-15'),
    (2, 'Bob', 'HR', '2019-03-22'),
    (3, 'Charlie', 'IT', '2021-07-09'),
    (4, 'David', 'Sales', '2018-11-23'),
    (5, 'Eve', 'IT', '2020-12-05')
]

salaries_data = [
    (1, 60000, 10),
    (2, 50000, 12),
    (3, 70000, 8),
    (4, 55000, 15),
    (5, 65000, 5)
]

# Create DataFrames
employees_df = spark.createDataFrame(employees_data, ["employee_id", "name", "department", "hire_date"])
salaries_df = spark.createDataFrame(salaries_data, ["employee_id", "salary", "bonus_percentage"])

# Join DataFrames
df = employees_df.join(salaries_df, "employee_id")

# Define UDF to calculate bonus amount
def calculate_bonus(salary, bonus_percentage):
    return salary * (bonus_percentage / 100)

bonus_udf = udf(calculate_bonus, FloatType())

# Apply UDF to create a bonus_amount column
df = df.withColumn("bonus_amount", bonus_udf(col("salary"), col("bonus_percentage")))

# Create a window specification to rank salaries within departments
window_spec = Window.partitionBy("department").orderBy(col("salary").desc())

# Apply window function to create salary_rank
df = df.withColumn("salary_rank", row_number().over(window_spec))

# Filter employees hired after 2020-01-01
df = df.filter(df["hire_date"] > '2020-01-01')

# Show final result
df.select("employee_id", "name", "department", "salary", "bonus_amount", "salary_rank").show()
```

## Exercise 2: Customer Purchase Data Analysis with Joins, UDFs, and Window Functions

You have two datasets, `customers` and `transactions`. You will join these datasets to analyze customer purchase behavior, use a UDF to categorize customers based on their total spending, and apply window functions to rank customers based on their purchase frequency.

### Instructions
1. Load Data:

Read the customers and transactions CSV files into PySpark DataFrames.

2. Perform a Join:

Join the customers DataFrame with the transactions DataFrame on `customer_id`.

3. Use a UDF to Categorize Customers:

* Define a UDF that categorizes customers based on their total spending:
```
High Spender: total spending > 400
Medium Spender: 200 < total spending ≤ 400
Low Spender: total spending ≤ 200
```

* Apply this UDF to categorize customers.

4. Apply Window Functions:

* Use a window function to rank customers based on the number of transactions.
* Create a column transaction_rank that shows the rank of customers based on transaction count.

5. Filter Data:

Filter customers who signed up before 2021.

6. Output:

Show the final DataFrame with `customer_id`, `name`, `total_spent`, `category`, and `transaction_rank`.

### Code (Solution)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, sum as spark_sum
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("CustomerPurchaseAnalysis").getOrCreate()

# Sample data
customers_data = [
    (101, 'John', '2021-02-14'),
    (102, 'Jane', '2020-11-05'),
    (103, 'Mike', '2019-08-12'),
    (104, 'Sarah', '2021-04-20'),
    (105, 'Peter', '2020-01-19')
]

transactions_data = [
    (1001, 101, 150.75, '2021-08-10'),
    (1002, 102, 200.00, '2021-01-15'),
    (1003, 103, 50.00, '2020-07-01'),
    (1004, 101, 300.00, '2021-12-01'),
    (1005, 104, 500.00, '2021-11-30'),
    (1006, 105, 100.00, '2020-05-21')
]

# Create DataFrames
customers_df = spark.createDataFrame(customers_data, ["customer_id", "name", "signup_date"])
transactions_df = spark.createDataFrame(transactions_data, ["transaction_id", "customer_id", "amount", "date"])

# Join DataFrames
df = customers_df.join(transactions_df, "customer_id")

# Calculate total spending by customer
total_spent_df = df.groupBy("customer_id", "name
```
