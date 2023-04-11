import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}


object Part1 {

  def processUserReviews(spark: SparkSession): DataFrame = {

    val dfUserReviews: DataFrame = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/resources/googleplaystore_user_reviews.csv")
      .select("App", "Sentiment_Polarity")

    dfUserReviews.groupBy("App")
      .agg(avg("Sentiment_Polarity").cast("double").alias("Average_Sentiment_Polarity"))
      .na.fill(0.0, Seq("Average_Sentiment_Polarity"))
  }
}


