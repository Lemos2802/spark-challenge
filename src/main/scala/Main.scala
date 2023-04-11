import org.apache.spark.sql.SparkSession

object Main {
  def main(args:Array[String]) : Unit={

    val spark = SparkSession.builder()
      .config("spark.hadoop.fs.native.lib", "false")
      .appName("Spark-challenge")
      .master("local[*]")
      .getOrCreate()

    val df_1 = Part1.processUserReviews(spark)
    Part2.to_csv(spark)
    val df_2 = Part3.df_3(spark)
    Part4.to_parquet(spark)
    Part5.df_4(spark)

    df_1.show()
    df_2.show()

  }

}