import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, collect_set, desc}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
// GroupBy por APP -> Coluna categorias resultante é um Set(nao te repetidos) contendo as categorias todas
//                 -> Restantes colunas ficam com o valor da coluna que possuia o numero maximo de reviews

object Part3 {

  def df_3(spark: SparkSession): DataFrame = {

    // Read googleplaystore.csv as a DataFrame
    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("src/resources/googleplaystore.csv")

    // Coluna das categorias organizada em um Set
    val df_categories = df.groupBy("App").agg(collect_set("Category").as("Categories"))

    //Dataframe agrupado por App e ordenado por numero max reviews
    val window_spec = Window.partitionBy("App").orderBy(desc("Reviews"))
    val ordered_df = df.withColumn("max_reviews", max("Reviews").over(window_spec))
      .orderBy(desc("max_reviews"))
      .drop("max_reviews")

    //dataset filtrado (apenas os que tem rank 1) ou seja as linhas com maior numero de reviews
    val row_number_spec = Window.partitionBy("App").orderBy(desc("Reviews"))
    val ranked_df = ordered_df.withColumn("rank", row_number.over(row_number_spec))
      .filter(col("rank") === 1)
      .distinct()
      .drop("rank")

    val final_df = ranked_df
      .join(df_categories, Seq("App"), "left")
      .withColumnRenamed("Category", "Old_Category")
      .withColumnRenamed("Categories", "Categories")
      .drop("Old_Category")

    val df_3 = final_df
      .withColumn("Size", regexp_extract(col("Size"), "(\\d+\\.?\\d*)", 1).cast("double")) //SIZE
      .withColumn("Price", regexp_extract(col("Price"), "\\$?(\\d+\\.?\\d*)", 1).cast("double") * 0.9)
      .withColumnRenamed("Content Rating", "Content_Rating")
      .withColumn("Genres", split(col("Genres"), ";").cast("array<string>"))
      .withColumnRenamed("Last Updated", "Last_Updated")
      .withColumn("Last_Updated", to_date(col("Last_Updated"), "MMMM d, yyyy"))
      .withColumnRenamed("Current Ver", "Current_Ver")
      .withColumnRenamed("Android Ver", "Minimum_Android_Version")
      .withColumn("Reviews", col("Reviews").cast("long"))

    df_3

  }


}
