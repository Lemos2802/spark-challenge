import Part4._
import org.apache.spark
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.functions.{col, collect_set, desc}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}


object Part5 {

  def df_4(spark:SparkSession):Unit = {


    val dfUserReviews: DataFrame = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/resources/googleplaystore_user_reviews.csv")
      .select("App", "Sentiment_Polarity")

    val df_joined = Part3.df_3(spark)
      .join(dfUserReviews, Seq("App"), "left")


    // Seleciona a coluna 'Genres', divide em novas linhas e seleciona as colunas 'Rating' e 'Sentiment_Polarity'
    // agrupa o DataFrame pelo valor da coluna 'Genres' e  faz o count, a média da coluna 'Rating' e a média da coluna 'Sentiment_Polarity'
    val df_grouped = df_joined
      .select(explode(col("Genres")).as("Genres"),
        col("Rating"),
        col("Sentiment_Polarity"))
      .groupBy("Genres")
      .agg(count("Genres").alias("Count"),
        avg("Rating").alias("Average_Rating"),
        avg("Sentiment_Polarity").alias("Average_Sentiment_Polarity"))



    df_grouped
    .repartition(1)
    .write
    .format("parquet")
    .option("compression", "gzip")
      .mode("overwrite")
    .save("src/output/googleplaystore_metrics.parquet")

  }

}
