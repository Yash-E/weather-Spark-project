from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, min, to_date, year

spark = SparkSession.builder.appName("Weather Data Analysis").getOrCreate()

df = spark.read.csv("data/weatherHistory.csv", header=True, inferSchema=True)

df = df.withColumn("Date", to_date(col("Formatted Date"), "yyyy-MM-dd"))

df.show(5)


df_clean = df.dropna(subset=["Temperature (C)", "Humidity", "Precip Type"])

df_clean = df_clean.withColumn("Year", year(col("Date")))

yearly_avg = df_clean.groupBy("Year").agg(
    avg("Temperature (C)").alias("AvgTemp"),
    avg("Humidity").alias("AvgHumidity")
)

print("Average Temperature and Humidity by Year:")
yearly_avg.show()

summary_temp = df_clean.groupBy("Summary").agg(
    avg("Temperature (C)").alias("AvgTemp")
)

print("Average Temperature by Weather Summary:")
summary_temp.orderBy(col("AvgTemp").desc()).show()

precip_visibility = df_clean.groupBy("Precip Type").agg(
    avg("Visibility (km)").alias("AvgVisibility"),
    avg("Wind Speed (km/h)").alias("AvgWindSpeed")
)

print("Average Visibility and Wind Speed by Precipitation Type:")
precip_visibility.show()

yearly_avg.write.csv("output/yearly_avg", header=True)
summary_temp.write.csv("output/summary_temp", header=True)
precip_visibility.write.csv("output/precip_visibility", header=True)

spark.stop()