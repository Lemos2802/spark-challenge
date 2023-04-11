import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.desc

object Part2  {
    def to_csv(spark: SparkSession): Unit ={

        val df = spark.read.format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load("src/resources/googleplaystore.csv")

        // Filter by rating >= 4.0 and sort in descending order
        val bestApps = df.filter(col("Rating") >= 4.0).sort(desc("Rating"))

        bestApps
          .coalesce(1)
          .write
          .format("csv")
          .option("header", "true")
          .option("delimiter", "§")
          .mode("overwrite")
          .save("src/output/best_apps.csv")


    }
}

