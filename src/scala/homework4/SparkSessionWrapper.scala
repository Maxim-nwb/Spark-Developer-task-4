package src.scala.homework4

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {

  lazy val spark: SparkSession = SparkSession
    .builder()
    .appName("SparkDataFrameAPI")
    .master("local[*]")
    .getOrCreate
}
