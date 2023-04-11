import Part1._
import Part3._
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, collect_set, desc}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}

object Part4 {

  def to_parquet(spark:SparkSession): Unit = {
    val df_1 = Part1.processUserReviews(spark)
    val df_3 = Part3.df_3(spark)

    val final_df = df_3
      .join(df_1, Seq("App"), "left")
      .na.fill(0.0)

    final_df.write.format("parquet")
      .option("compression", "gzip")
      .mode("overwrite")
      .save("src/output/googleplaystore_cleaned")
  }
}
