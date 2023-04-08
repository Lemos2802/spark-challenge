import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.desc

object Parte2  {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
          .config("spark.hadoop.fs.native.lib", "false")
          .appName("Parte2")
          .master("local[*]")
          .getOrCreate()

        // Read googleplaystore.csv as a DataFrame
        val df = spark.read.format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load("src/resources/googleplaystore.csv")

        // Filter by rating >= 4.0 and sort in descending order
        val bestApps = df.filter(col("Rating") >= 4.0).sort(desc("Rating"))

        // Save as CSV (delimiter: "§")
        bestApps
          .coalesce(1)
          .write
          .format("csv")
          .option("header", "true")
          .option("delimiter", "§")
          .mode("overwrite")
          .save("src/output/best_apps.csv")

        spark.stop()

    }
}

