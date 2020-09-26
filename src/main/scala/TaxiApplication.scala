import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TaxiApplication extends App {

  val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("taxiApplication")
    .getOrCreate()

  val taxiDF = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")

  val taxiZonesDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/taxi_zones.csv")

  val pickupsByTaxiZoneDF = taxiDF.groupBy("PULocationID")
    .agg(count("*").as("totalTrips"))
    .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
    .drop("LocationID", "service_zone")
    .orderBy(col("totalTrips").desc_nulls_last)
    .groupBy(col("Borough"))
    .agg(sum(col("totalTrips")).as("totalTrips"))
    .orderBy(col("totalTrips").desc_nulls_last)

  val dropOffByTaxiZoneDF = taxiDF.groupBy("DOLocationID")
    .agg(count("*").as("totalTrips"))
    .join(taxiZonesDF, col("DOLocationID") === col("LocationID"))
    .drop("LocationID", "service_zone")
    .orderBy(col("totalTrips").desc_nulls_last)
    .groupBy(col("Borough"))
    .agg(sum(col("totalTrips")).as("totalTrips"))
    .orderBy(col("totalTrips").desc_nulls_last)

  val pickupsByHourDF = taxiDF
    .withColumn("hour_of_day", hour(col("tpep_pickup_datetime")))
    .groupBy("hour_of_day")
    .agg(count("*").as("totalTrips"))
    .orderBy(col("totalTrips").desc_nulls_last)



}


