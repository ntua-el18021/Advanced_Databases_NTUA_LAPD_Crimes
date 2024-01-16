from utilities_functions import dataframe_print_additional_info, dataframe_print_requested_information, save_dataframe_to_csv, setup_output_path, dataframe_parse_arguments, close_stdout
from utilities_config import *
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql.functions import col, to_date
import os


def trim_columns(df):
    """
    Strips whitespace from column names of a DataFrame.
    """
    for field in df.schema.fields:
        df = df.withColumnRenamed(field.name, field.name.strip())
    return df


def process_data(spark, datasets_path):
    """
    Loads, processes, and combines datasets.
    """
    df1 = spark.read.csv(datasets_path + "Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True)
    df2 = spark.read.csv(datasets_path + "Crime_Data_from_2020_to_Present.csv", header=True, inferSchema=True)
    rev_geo_df = spark.read.csv(datasets_path + "revgecoding.csv", header=True, inferSchema=True)
    income_2015_df = spark.read.csv(datasets_path + "LA_income_2015.csv", header=True, inferSchema=True)

    df1 = trim_columns(df1)
    df2 = trim_columns(df2)

    combined_df_original = df1.unionByName(df2, allowMissingColumns=True)

    # Adjust DataFrame Types
    dateFormat = "M/d/yyyy hh:mm:ss a"
    combined_df = combined_df_original.withColumn("Date Rptd", to_date(col("Date Rptd"), dateFormat)) \
                                    .withColumn("DATE OCC", to_date(col("DATE OCC"), dateFormat)) \
                                    .withColumn("Vict Age", col("Vict Age").cast(IntegerType())) \
                                    .withColumn("LAT", col("LAT").cast(DoubleType())) \
                                    .withColumn("LON", col("LON").cast(DoubleType()))

    # Join combined_df with corresponding ZIP codes and median income
    combined_with_zip_df = combined_df.join(rev_geo_df, ['LAT', 'LON'], 'left')
    final_combined_df = combined_with_zip_df.join(income_2015_df, combined_with_zip_df['ZIPcode'] == income_2015_df['Zip Code'], 'left')

    # Reorder columns and remove duplicates
    final_df = final_combined_df.select(
        combined_df["*"],
        col("ZIPcode"),
        col("Community"),
        col("Estimated Median Income")
    )

    return combined_df_original, rev_geo_df, income_2015_df, final_df, combined_df, combined_with_zip_df, final_combined_df, df1, df2



def main():
    """
    Main function.
    """
    # Parse arguments and setup output path
    with_desccriptions, save_to_csv, overwrite_csv, output_path = dataframe_parse_arguments(DEFAULT_OUTPUT_PATH)
    script_name = os.path.basename(os.path.abspath(__file__))
    script_name = setup_output_path(output_path, script_name, with_desccriptions)

    # Initialize Spark session
    spark = SparkSession.builder.appName(script_name).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Process data
    combined_df_original, rev_geo_df, income_2015_df, final_df, combined_df, combined_with_zip_df, final_combined_df, df1, df2  = process_data(spark, DATASETS_PATH)

    # Descriptive output
    dataframe_print_additional_info(with_desccriptions, rev_geo_df, income_2015_df, final_df, combined_df_original, combined_df, combined_with_zip_df, final_combined_df, df1, df2)

    # Requested information!
    dataframe_print_requested_information(final_df)
    
    # Save final DataFrame to CSV
    save_dataframe_to_csv(overwrite_csv, save_to_csv, SAVED_DATAFRAME_NAME, final_df)

    # Reset stdout and stop Spark session
    close_stdout()
    spark.stop()


if __name__ == "__main__":
    main()