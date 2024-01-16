from utilities_functions import setup_output_path, close_stdout, read_saved_dataframe, query_3_parse_arguments, query_3_print_descendants
from utilities_config import *
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import year, col, desc
import time, os
from pyspark.sql.functions import regexp_replace


def find_the_zips(spark, df_with_zips):
        """
        Finds the top and bottom 3 ZIP Codes by median income.
        """
        df_with_zips = df_with_zips.withColumn("Estimated Median Income", regexp_replace(col("Estimated Median Income"), '[$,]', ''))
        df_with_zips = df_with_zips.withColumn("Estimated Median Income", col("Estimated Median Income").cast("int"))
        df_with_zips = df_with_zips.filter(col("Estimated Median Income").isNotNull() & col("ZIPcode").isNotNull() & col("Vict Descent").isNotNull())

        sorted_medians_df = df_with_zips.select("ZIPcode", "Estimated Median Income").dropDuplicates(["ZIPcode"]).orderBy(desc("Estimated Median Income"))
        
        # print("\n\n\n-------------------------------------------------------------")
        # sorted_medians_df.show()
        top_zips = sorted_medians_df.limit(3)
        bottom_zips = sorted_medians_df.collect()[-3:]
        bottom_zips = spark.createDataFrame(bottom_zips)
        total_zips = top_zips.union(bottom_zips)

        return total_zips

def process_data(spark, final_df):
    """
    Processes the data.
    """
    df_filtered_2015 = final_df.filter(year(col("DATE OCC")) == 2015)
    df_filtered = df_filtered_2015.select("Vict Descent", "ZIPcode", "Estimated Median Income")
    
    total_median = find_the_zips(spark, df_filtered)

    result_total = df_filtered.join(total_median.select(col("ZIPcode")), "ZIPcode")
    df_descents = result_total.groupBy("Vict Descent").count().orderBy(col("count").desc())
   
    return df_descents



def main():
    """
    Main function.
    """
    with_desccriptions = query_3_parse_arguments()
    script_name = os.path.basename(os.path.abspath(__file__))
    script_name = setup_output_path(DEFAULT_OUTPUT_PATH, script_name, with_desccriptions)

    # df_descents_executors = []
    executor_times = []
    print_results = True

    print(f"Requested information: ")
    for executors_count in SPARK_EXECUTORS:
        # Initialize Spark session
        spark = SparkSession.builder.appName(script_name+"_exec_"+str(executors_count)).config("spark.executor.instances", executors_count).getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        # Load the combined dataset
        final_df = read_saved_dataframe(spark, SAVED_DATAFRAME_PATH, SAVED_DATAFRAME_NAME)

        # Process data
        try:
            start_time = time.time()
            df_descents = process_data(spark, final_df)
            end_time = time.time()
            executor_times.append(end_time - start_time)

            if(with_desccriptions):
                print(f"Results for {executors_count} executors: ")
                query_3_print_descendants(df_descents)
            elif(print_results):
                query_3_print_descendants(df_descents)
                print_results = False
            print(f"Execution time for {executors_count} executors: {end_time - start_time} seconds")

        except Exception as e:
            print(e)
        finally:
            spark.stop()

    close_stdout()


if __name__ == '__main__':
    main()