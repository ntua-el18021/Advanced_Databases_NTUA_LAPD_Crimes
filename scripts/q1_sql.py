from utilities_functions import read_saved_dataframe, setup_output_path, close_stdout
from utilities_config import *
from pyspark.sql import SparkSession
import time
import os


def process_data(spark, final_df):
    """
    Processes the data.
    """
    # Register the DataFrame as a SQL temporary view
    final_df.createOrReplaceTempView("final_df")

    # Execute SQL query to extract year and month from 'DATE OCC', count the occurrences, and rank within each year
    result = spark.sql("""
        SELECT Year, Month, count, Rank
        FROM (
            SELECT Year, Month, count, RANK() OVER (PARTITION BY Year ORDER BY count DESC) as Rank
            FROM (
                SELECT YEAR(`DATE OCC`) as Year, MONTH(`DATE OCC`) as Month, COUNT(*) as count
                FROM final_df
                GROUP BY YEAR(`DATE OCC`), MONTH(`DATE OCC`)
            )
        ) tmp
        WHERE Rank <= 3
        ORDER BY Year, Rank
    """)

    return result



def main():
    """
    Main function.
    """
    # Set up output path
    script_name = os.path.basename(os.path.abspath(__file__))
    script_name = setup_output_path(DEFAULT_OUTPUT_PATH, script_name)

    # Initialize Spark session
    spark = SparkSession.builder.appName(script_name).config("spark.executor.instances", "4").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Load the combined dataset
    final_df = read_saved_dataframe(spark, SAVED_DATAFRAME_PATH, SAVED_DATAFRAME_NAME)

    # Process data
    start_time = time.time()
    result = process_data(spark, final_df)
    end_time = time.time()

    # Requested information!
    result.show(result.count(), truncate=False)
    print("\nExecution time: ", end_time - start_time)

    # Reset stdout and stop Spark session
    close_stdout()
    spark.stop()


if __name__ == "__main__":
    main()