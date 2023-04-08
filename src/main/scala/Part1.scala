import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object Part1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Part1")
      .setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()


    val df_user_reviews = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/resources/googleplaystore_user_reviews.csv")
      .select("App", "Sentiment_Polarity")

    //Dataframe : agrupar pelo nome da App e criar coluna com as medias
    val df_1 = df_user_reviews.groupBy("App")
      .agg(avg("Sentiment_Polarity").alias("Average_Sentiment_Polarity"))
      .na.fill(0.0, Seq("Average_Sentiment_Polarity"))

    df_1.show()
  }
}