from utilities_functions import close_stdout
from utilities_config import *
from q3_dataframe import find_the_zips
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col, year, to_date
import os, sys


def make_base_dataframe(spark, datasets_path):
    """
    Loads, processes, and combines datasets.
    """
    df1 = spark.read.csv(datasets_path + "Crime_Data_from_2010_to_2019.csv", header=True, inferSchema=True).withColumnRenamed("AREA ", "AREA")
    df2 = spark.read.csv(datasets_path + "Crime_Data_from_2020_to_Present.csv", header=True, inferSchema=True)
    rev_geo_df = spark.read.csv(datasets_path + "revgecoding.csv", header=True, inferSchema=True)
    income_2015_df = spark.read.csv(datasets_path + "LA_income_2015.csv", header=True, inferSchema=True)

    combined_df = df1.unionByName(df2, allowMissingColumns=True)
    dateFormat = "M/d/yyyy hh:mm:ss a"
    combined_df = combined_df.withColumn("DATE OCC", to_date(col("DATE OCC"), dateFormat)) \
                                    .withColumn("LAT", col("LAT").cast(DoubleType())) \
                                    .withColumn("LON", col("LON").cast(DoubleType()))

    # combined_with_zip_df = combined_df.join(rev_geo_df, ['LAT', 'LON'], 'left')
    # final_df = combined_with_zip_df.join(income_2015_df, combined_with_zip_df['ZIPcode'] == income_2015_df['Zip Code'], 'left')

    return combined_df, rev_geo_df, income_2015_df

def q3_short(spark, final_df):
    """
    Necessary operations from Q3
    """
    df_filtered_2015 = final_df.filter(year(col("DATE OCC")) == 2015)
    df_filtered = df_filtered_2015.select("Vict Descent", "ZIPcode", "Estimated Median Income")
    total_median = find_the_zips(spark, df_filtered)

    # result_total = df_filtered.join(total_median.select(col("ZIPcode")), "ZIPcode")

    return df_filtered, total_median

def q4_short(spark, final_df):
    """
    Necessary operations from Q4
    """
    final_df = final_df.withColumn("Year", year(col("DATE OCC")))
    final_df = final_df.filter(col("Year").isNotNull())
    final_df = final_df.filter((col("LAT")!=0) & (col("LON")!=0))
    final_df = final_df.filter(col("Weapon Used Cd").rlike("^1"))

    lapd_stations_df = spark.read.csv(DATASET_PATH+"LAPD_Police_Stations.csv", header=True, inferSchema=True)

    # combined_df = final_df.join(lapd_stations_df, final_df["AREA"] == lapd_stations_df["PREC"])
    
    return final_df, lapd_stations_df

def print_to_file(type, operation, flag="a", output_path=DEFAULT_OUTPUT_PATH, script_name="analysis_compare_joins"):
    if type == "show":
        sys.stdout = open(output_path + script_name + "_show_df.out", flag)
        operation()
    elif type == "explain":
        sys.stdout = open(output_path + script_name + "_explain.out", flag)
        operation()   
    close_stdout()


def main():
    """
    Main function.
    """
    script_name = os.path.basename(os.path.abspath(__file__)).split(".")[0]

    print_to_file("explain", lambda: None, "w")
    print_to_file("show", lambda: None, "w")

    join_strategies = ['broadcast', 'merge', 'shuffle_hash', 'shuffle_replicate_nl']
    for strategy in join_strategies:
        print_to_file("explain", lambda: print("Showing explain() results for strategy: ", strategy), "a")
        print_to_file("show", lambda: print("Showing show() results for strategy: ", strategy), "a")

        spark = SparkSession.builder.appName(script_name+"_"+strategy).getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        
        combined_df, rev_geo_df, income_2015_df = make_base_dataframe(spark, DATASETS_PATH)

        combined_with_zip_df = combined_df.join(rev_geo_df.hint(strategy), ['LAT', 'LON'], 'left')
        final_df = combined_with_zip_df.join(income_2015_df.hint(strategy), combined_with_zip_df['ZIPcode'] == income_2015_df['Zip Code'], 'left')

        print_to_file("explain", lambda: combined_with_zip_df.explain())
        print_to_file("explain", lambda: final_df.explain())

        print_to_file("show", lambda: combined_with_zip_df.show())
        print_to_file("show", lambda: final_df.show())


        df_filtered, total_median = q3_short(spark, final_df)

        result_total = df_filtered.join(total_median.select(col("ZIPcode")).hint(strategy), "ZIPcode")

        print_to_file("explain", lambda: result_total.explain())
        print_to_file("show", lambda: result_total.show())


        final_df, lapd_stations_df = q4_short(spark, final_df)

        combined_df = final_df.join(lapd_stations_df.hint(strategy), final_df["AREA"] == lapd_stations_df["PREC"])

        print_to_file("explain", lambda: combined_df.explain())
        print_to_file("show", lambda: combined_df.show())

        
        close_stdout()
        spark.stop()


if __name__ == "__main__":
    main()