from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, split, input_file_name, regexp_extract, upper,
    lower, trim, coalesce, when
)

# 1. Parameters
INPUT_GLOB = "./*.csv"  # Location of .csv files
PARQUET_OUTPUT = "./cleaned_stops.parquet"
CSV_OUTPUT_DIR = "./cleaned_stops_csv"

# region mapping
WEST   = ["CA", "WA", "OR", "AZ", "TX"]
EAST   = ["NC", "RI", "VT", "NY", "MA"]
STATES = WEST + EAST

# Spark session ── make the driver JAR visible
spark = (
    SparkSession.builder
        .appName("CleanStops")
        #.config("spark.driver.extraClassPath", MYSQL_JAR_PATH)
        #.config("spark.executor.extraClassPath", MYSQL_JAR_PATH)
        .getOrCreate()
)

df = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(INPUT_GLOB)
)

# Clean
candidates = []
if "stop_hour"  in df.columns:  candidates.append(col("stop_hour").cast("int"))
if "time"       in df.columns:  candidates.append(split(col("time"), ":")[0].cast("int"))
if "stop_time"  in df.columns:  candidates.append(split(col("stop_time"), ":")[0].cast("int"))

df = df.withColumn("stop_hour",  coalesce(*candidates))

df = df.withColumn(
        "state",
        upper(regexp_extract(input_file_name(), r".*/([A-Za-z]{2})_.*\.csv$", 1))
     )

# trim now so MySQL gets tidy values
df = (df
        .withColumn("subject_race", trim(lower(col("subject_race"))))
        .withColumn("subject_sex",  trim(lower(col("subject_sex")))))

df = (df.filter(col("state").isin(*STATES))
        .na.drop(subset=["stop_hour", "subject_race", "subject_sex"])
        .filter(~col("subject_race").isin("n/a","na","unknown"))
        .filter(~col("subject_sex").isin("n/a","na","unknown"))
)

#Swap mislabeled columns and then map to buckets
df = (df
    .withColumn(                    #some CSVs put codes in subject_sex - bug fix
        "race_raw",
        when(col("subject_race").rlike("^[0-9]+$"), col("subject_sex"))
        .otherwise(col("subject_race"))
    )
    .withColumn(
        "sex_raw",
        when(col("subject_race").rlike("^[0-9]+$"), col("subject_race"))
        .otherwise(col("subject_sex"))
    )
)

race_norm = (
    when(col("race_raw").rlike("asian|pacific|pi|^a$|^ap$|^19$|^20$"),
         "asian/pacific islander")
   .when(col("race_raw").rlike("black|african|^b$|^23$|^24$"),         "black")
   .when(col("race_raw").rlike("hisp|latino|mex|^h$|^l$|^25$|^26$"),   "hispanic")
   .when(col("race_raw").rlike("white|^w$|^21$|^22$"),                 "white")
   .otherwise("other")
)

sex_norm  = (
    when(col("sex_raw").isin( "f","female", "0"),   "female")
   .when(col("sex_raw").isin( "m","male", "1"),     "male")
   .otherwise("other")
)

df_clean = (df
    .withColumn("race_norm", race_norm)
    .withColumn("sex_norm",  sex_norm)
    .select("stop_hour","state","race_norm","sex_norm")      # final schema
)

# 6. Write output

# 6a) Single CSV with header
df_clean.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(CSV_OUTPUT_DIR)

# 6b) Optional: Parquet version
df_clean.write \
    .mode("overwrite") \
    .parquet(PARQUET_OUTPUT)

# 7. Done
spark.stop()



