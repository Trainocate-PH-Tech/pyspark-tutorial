# Dealing with Data

## Exercise 1: Read CSV, Process and Save Parquet

**Instructions**

1. Read the csv file `employees.csv`

```
id,name,age,department,salary
1,Alice,30,HR,50000
2,Bob,35,Engineering,120000
3,Charlie,25,Sales,70000
4,David,45,Engineering,130000
5,Eva,40,HR,80000
6,Frank,33,Sales,60000
```

### Code

```python
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("CSV_to_Parquet").getOrCreate()

# Read CSV file
df = spark.read.csv("path/to/employees.csv", header=True, inferSchema=True)

# Show data
df.show()
```

2. Process the data to find out the following:

* Filter out employees with salary greater than 70,000
* Group by department and calculate the average salary for each department.

### Code

```python
# Filter employees with salary > 70000
df_filtered = df.filter(df["salary"] > 70000)

# Group by department and calculate average salary
df_grouped = df_filtered.groupBy("department").avg("salary")

# Show the processed data
df_grouped.show()
```

3. Write results to the parque file

### Code

```python
# Write the result to a Parquet file
df_grouped.write.parquet("path/to/filtered_departments.parquet")
```

4. Verify the Parquet file:

```python
# Read the parquet file to verify
df_parquet = spark.read.parquet("path/to/filtered_departments.parquet")
df_parquet.show()
```

## Exercise 2: Read JSON, Process and Save to Parquet

### Data (`products.json`)

```
[
    {
        "id": 1,
        "product_name": "Laptop",
        "category": {
            "main": "Electronics",
            "sub": "Computers"
        },
        "price": 1200,
        "stock": {
            "quantity": 100,
            "warehouse": "A1"
        }
    },
    {
        "id": 2,
        "product_name": "Phone",
        "category": {
            "main": "Electronics",
            "sub": "Mobile"
        },
        "price": 800,
        "stock": {
            "quantity": 200,
            "warehouse": "B2"
        }
    },
    {
        "id": 3,
        "product_name": "Desk",
        "category": {
            "main": "Furniture",
            "sub": "Office"
        },
        "price": 250,
        "stock": {
            "quantity": 50,
            "warehouse": "C3"
        }
    }
]
```

1. Read the JSON file

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("Nested_JSON_to_Parquet").getOrCreate()

# Read the JSON file with nested structure
df = spark.read.json("path/to/products_nested.json")

# Show the data (nested structure)
df.show(truncate=False)
df.printSchema()
```

2. Flatten the Nested JSON Structure

```python
# Flatten the nested attributes
df_flat = df.select(
    col("id"),
    col("product_name"),
    col("category.main").alias("category_main"),
    col("category.sub").alias("category_sub"),
    col("price"),
    col("stock.quantity").alias("stock_quantity"),
    col("stock.warehouse").alias("warehouse")
)

# Show the flattened DataFrame
df_flat.show(truncate=False)
```

3. Process the data by calculating the total stock per product. To do this create new columns:
* `total_stock_value`
* `price`
* `stock_quantity`

```python
# Calculate total value of stock per product
df_processed = df_flat.withColumn("total_stock_value", col("price") * col("stock_quantity"))

# Show the processed data
df_processed.show(truncate=False)
```

4. Write the result to a parquet file.

```python
# Write the processed data to a Parquet file
df_processed.write.parquet("path/to/products_processed.parquet")
```

5. Verify the Written Parquet Filter

```python
# Read the Parquet file
df_parquet = spark.read.parquet("path/to/products_processed.parquet")

# Show the data from the Parquet file
df_parquet.show(truncate=False)
```
