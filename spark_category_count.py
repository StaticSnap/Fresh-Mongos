# Counts the number of videos in each category from the cleaned dataset.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import argparse
import time

def main(input_file, output_file):
    start_time = time.time()

    # Initialize Spark session
    spark = SparkSession.builder.appName("CategoryCount").getOrCreate()
    print(spark.version)

    # Input: cleaned JSONL dataset
    df = spark.read.json(input_file)

    # Computing operations: group by category and count videos
    category_counts = df.groupBy("category").count().orderBy(col("count").desc())

    # Output: save results to JSON
    category_counts.coalesce(1).write.json(output_file, mode="overwrite")

    category_counts.show(10, truncate=False)  # Display top 10 categories

    end_time = time.time()
    print(f"Execution time: {end_time - start_time:.2f} seconds")

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Count videos per category using PySpark")
    parser.add_argument("input_file")
    parser.add_argument("output_file")
    args = parser.parse_args()
    main(args.input_file, args.output_file)


"""
Pseudocode:

Input:
    input_file  - JSONL file with cleaned video data
    output_file - path to save the category counts in JSON format
Output:
    Writes category counts to output_file
    
Function CountVideosPerCategory(input_file, output_file):
    # Start Spark session
    spark <- SparkSession.builder.appName("CategoryCount").getOrCreate()

    # Read input JSONL file into DataFrame
    df <- spark.read.json(input_file)

    # Group by 'category' and count videos
    category_counts  <- df.groupBy("category").count()

    # Sort by count in descending order
    category_counts <- category_counts.orderBy(count descending)

    # Save results to JSON
    category_counts.coalesce(1).write.json(output_file, mode="overwrite")

    # Display top 10 categories
    category_counts.show(10, truncate=False)

    # Stop Spark session
    spark.stop()
    
End Function
"""