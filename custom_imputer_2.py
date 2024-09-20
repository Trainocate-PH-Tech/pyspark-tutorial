from pyspark.ml.feature import Imputer
from pyspark.ml import Estimator
from pyspark.sql import DataFrame, functions as F

class CustomImputer(Imputer):
    def __init__(self, input_cols=None, output_cols=None, strategy="mean"):
        super(CustomImputer, self).__init__()
        self.setInputCols(input_cols)
        self.setOutputCols(output_cols)
        self.setStrategy(strategy)

    def setStrategy(self, strategy):
        """Set the imputation strategy (e.g., mean, median, mode, max)."""
        if strategy not in ["mean", "median", "mode", "max"]:
            raise ValueError("Strategy must be one of 'mean', 'median', 'mode', or 'max'.")
        return super().setStrategy(strategy)

    def fit(self, dataset: DataFrame) -> 'CustomImputerModel':
        """Fit the model to the dataset based on the strategy."""
        if self.getStrategy() == "max":
            # Calculate max for each input column
            max_values = {col: dataset.agg(F.max(col)).collect()[0][0] for col in self.getInputCols()}
            self.max_values = max_values
        else:
            super().fit(dataset)
        
        return self

    def transform(self, dataset: DataFrame) -> DataFrame:
        """Transform the dataset based on the fitted model."""
        if self.getStrategy() == "max":
            for col in self.getInputCols():
                max_value = self.max_values[col]
                dataset = dataset.withColumn(col, F.when(F.col(col).isNull(), max_value).otherwise(F.col(col)))
        else:
            return super().transform(dataset)

        return dataset

# Example usage
if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .appName("Custom Imputer with Max Strategy") \
        .getOrCreate()

    # Sample DataFrame
    data = [(1, None), (2, 2.0), (3, None), (4, 4.0)]
    columns = ["id", "value"]
    df = spark.createDataFrame(data, columns)

    df.show()

    # Create and apply the custom imputer
    custom_imputer = CustomImputer(input_cols=["value"], output_cols=["value_imputed"], strategy="max")
    model = custom_imputer.fit(df)
    imputed_df = model.transform(df)

    # Show results
    imputed_df.show()

    spark.stop()

