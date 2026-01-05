package de.xixi.sparkcourse

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("spark-course1")
      .master("local[*]")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val df: DataFrame = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv("data/AAPL.csv")

    df.show()
    df.printSchema()

    df.select("Date","Open","Close").show()
    val column = df("Date")
    col("Date")
    import spark.implicits._
    $"Date"

    df.select(col("Date"), $"Open", df("Close")).show()

  }
}

