package de.xixi.sparkcourse


import org.apache.hadoop.shaded.org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.functions.{col, current_timestamp, expr, lit,year}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
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

    val renameColumn = List(
      col("Date").as("date"),
      col("Open").as("open"),
      col("High").as("high"),
      col("Low").as("low"),
      col("Close").as("close"),
      col("Adj Close").as("adjClose"),
      col("Volume").as("volume")
    )

   val stockData =  df.select(renameColumn: _*)


    stockData
      .groupBy(year(col("date")))
      .agg(functions.max(col("close")).as("maxClose"), functions.avg(col("close")).as("avgClose"))
      .sort(col("maxClose").desc)
      .show()

    stockData
      .groupBy(year(col("date")).as("year"))
      .max("close", "high")
      .show()

  }
}

