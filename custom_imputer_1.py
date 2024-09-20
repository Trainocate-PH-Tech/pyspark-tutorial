from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Transformer
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable

class SimpleImputer(Transformer, DefaultParamsReadable, DefaultParamsWritable):
    def __init__(self, input_cols=None, fill_value=None):
        super(SimpleImputer, self).__init__()
        self.input_cols = input_cols
        self.fill_value = fill_value

    def _transform(self, df: DataFrame) -> DataFrame:
        if self.input_cols is None:
            raise ValueError("Input columns must be specified")
        
        for col in self.input_cols:
            df = df.withColumn(col, F.when(F.col(col).isNull(), self.fill_value).otherwise(F.col(col)))
        
        return df

# Example usage
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Simple Imputer Example") \
        .getOrCreate()

    # Sample DataFrame
    data = [(1, None), (2, "B"), (3, None)]
    columns = ["id", "value"]
    df = spark.createDataFrame(data, columns)

    # Create and apply the imputer
    imputer = SimpleImputer(input_cols=["value"], fill_value="A")
    imputed_df = imputer.transform(df)

    # Show results
    imputed_df.show()

    spark.stop()

