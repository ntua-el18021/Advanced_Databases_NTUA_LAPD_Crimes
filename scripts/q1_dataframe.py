from utilities_functions import read_saved_dataframe, setup_output_path, close_stdout
from utilities_config import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, col, desc, rank
from pyspark.sql.window import Window
import time
import os


def process_data(final_df):
    """
    Processes the data.
    """
    # Extract year and month from 'DATE OCC'
    final_df_with_month_year = final_df.withColumn("Year", year("DATE OCC")) \
                                    .withColumn("Month", month("DATE OCC"))
    
    # Group by year and month and count the occurrences
    monthly_crime_counts = final_df_with_month_year.groupBy("Year", "Month").count()

    # Window specification to rank by crime count within each year
    windowSpec = Window.partitionBy("Year").orderBy(desc("count"))

    # Rank the months within each year by crime count and filter top 3
    ranked_monthly_crime_counts = monthly_crime_counts.withColumn("Rank", rank().over(windowSpec)) \
                                                    .filter(col("Rank") <= 3)

    # Sort the result by year and rank
    sorted_ranked_monthly_crime_counts = ranked_monthly_crime_counts.orderBy("Year", "Rank")

    return sorted_ranked_monthly_crime_counts


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
    sorted_ranked_monthly_crime_counts = process_data(final_df)
    end_time = time.time()

    # Requested information!
    sorted_ranked_monthly_crime_counts.show(sorted_ranked_monthly_crime_counts.count(), truncate=False)
    print("\nExecution time: ", end_time - start_time)

    # Reset stdout and stop Spark session
    close_stdout()
    spark.stop()


if __name__ == "__main__":
    main()