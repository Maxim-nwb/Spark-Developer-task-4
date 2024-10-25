package ru.otus.spark

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import src.scala.homework4.{ActualDataTrips, LocationStatistic, ReferenceDataTrips, SparkSessionWrapper}

import java.sql.Timestamp
import java.text.SimpleDateFormat

object Main extends App
 with SparkSessionWrapper {

  import spark.implicits._

  var path_to_cars = "src/resources/data/cars.json"
  var path_to_cars_dates = "src/resources/data/cars_dates.json"
  val path_to_taxi_zones = "src/resources/data/taxi_zones.csv"
  val path_to_yellow_taxi = "src/resources/data/yellow_taxi_jan_25_2018/part-00004-5ca10efc-1651-4c8f-896a-3d7d3cc0e925-c000.snappy.parquet"

  val schema_taxi_zones = Encoders.product[ReferenceDataTrips].schema
  val schema_yellow_taxi = Encoders.product[ActualDataTrips].schema

//Task 1
  //Загрузить данные в первый DataFrame из файла с фактическими данными поездок в Parquet
  val task1_taxi = spark.read.schema(schema_yellow_taxi)
    .parquet(path_to_yellow_taxi)
    .as[ActualDataTrips]
    .toDF()

  //Загрузить данные во второй DataFrame из файла со справочными данными поездок в csv (src/main/resources/data/taxi_zones.csv)
  val taxi_zones = spark.read.option("header", "true")
    .schema(schema_taxi_zones)
    .csv(path_to_taxi_zones)
    .as[ReferenceDataTrips]
    .toDF()

  //С помощью DSL построить таблицу, которая покажет какие районы самые популярные для заказов.
    val task1_rez = task1_taxi.joinWith(taxi_zones, task1_taxi.col("PULocationID") === taxi_zones.col("LocationID"))
  .map{case(taxi, taxiZone) =>
      LocationStatistic(taxiZone.getAs("LocationID"), taxiZone.getAs("Zone"))
    }
    .groupByKey(_.Zone)
    .reduceGroups((a,b) => {
      a.count += b.count
      a})
    .map(_._2)

  //Результат вывести на экран
  task1_rez.show()

  //и записать в файл Parquet.
  task1_rez
    .write
    .format("parquet")
    .mode(SaveMode.Overwrite)
    .saveAsTable("task1")

  //Task 2

  //Загрузить данные в RDD из файла с фактическими данными поездок в Parquet (src/main/resources/data/yellow_taxi_jan_25_2018).
  val task2_taxi = spark.read.schema(schema_yellow_taxi)
    .parquet(path_to_yellow_taxi)
    .as[ActualDataTrips]
    .rdd

  //С помощью lambda построить таблицу, которая покажет в какое время происходит больше всего вызовов.
  val lambda = (a: ActualDataTrips) => new SimpleDateFormat("HH").format(a.tpep_pickup_datetime)

  val task2_rez = task2_taxi.map{i => (lambda(i), 1)}
    .groupBy(_._1)
    .map(i => (i._1, i._2.size))
    .sortBy(_._2, false).collect()
    .head

  //Результат вывести на экран
  println(task2_rez)

  // и в txt файл c пробелами.
  val out_2 = new java.io.PrintWriter("src/resources/data/rez/task2.txt")
  out_2.println(task2_rez._1, " ", task2_rez._2)
  out_2.close()

  //Task 3

  //Загрузить данные в DataSet из файла с фактическими данными поездок в Parquet (src/main/resources/data/yellow_taxi_jan_25_2018).
  val task3_taxi = spark.read.schema(schema_yellow_taxi)
    .parquet(path_to_yellow_taxi)
    .as[ActualDataTrips]

  //С помощью DSL и lambda построить таблицу, которая покажет. Как происходит распределение поездок по дистанции.
  val task3_rez = task3_taxi.describe("trip_distance")

  // Результат вывести на экран
  task3_rez.show()

  // и записать в файл Parquet.
  task3_rez.write
    .format("parquet")
    .mode(SaveMode.Overwrite)
    .saveAsTable("task2")

  spark.stop()
}
