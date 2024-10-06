from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("WordCountExample").getOrCreate()

# Step 1: Read the text file into an RDD
text_rdd = spark.sparkContext.textFile("sample.txt")

# Step 2: Transformations
# Split each line into words
words_rdd = text_rdd.flatMap(lambda line: line.split(" "))

# Create a key-value pair with word and count of 1
word_pairs_rdd = words_rdd.map(lambda word: (word, 1))

# Reduce by key to count occurrences of each word
word_counts_rdd = word_pairs_rdd.reduceByKey(lambda a, b: a + b)

# Step 3: Actions
# Collect the results and print them
results = word_counts_rdd.collect()

# Print the word counts
for word, count in results:
    print(f"{word}: {count}")

# Stop the Spark session
spark.stop()
