import org.apache.spark.sql.SparkSession

object Test extends App {

  println("Hello")

  val spark = SparkSession.builder()
    .appName("appName")
    .config("spark.master", "local")
    .getOrCreate()


}
