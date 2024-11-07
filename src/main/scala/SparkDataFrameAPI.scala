import org.apache.spark.api.java.JavaRDD.fromRDD
import org.apache.spark.sql._

import java.text.SimpleDateFormat

object Main extends App
 with SparkSessionWrapper {

  import spark.implicits._

  val conf = spark.sparkContext.hadoopConfiguration
  val fs = org.apache.hadoop.fs.FileSystem.get(conf)

  var path_to_cars = "src/resources/data/cars.json"
  var path_to_cars_dates = "src/resources/data/cars_dates.json"
  val path_to_taxi_zones = "src/resources/data/taxi_zones.csv"
  val path_to_yellow_taxi = "src/resources/data/yellow_taxi_jan_25_2018"

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
    .option("inferSchema" , "true")
    .csv(path_to_taxi_zones)
    .as[ReferenceDataTrips]
    .toDF()

  //С помощью DSL построить таблицу, которая покажет какие районы самые популярные для заказов.
    val task1_rez = task1_taxi.joinWith(taxi_zones, task1_taxi.col("PULocationID") === taxi_zones.col("LocationID"))
      .select($"_2.LocationID", $"_2.Zone")
      .groupBy($"LocationID", $"Zone")
      .count()

  //Результат вывести на экран
  task1_rez.orderBy($"LocationID").show()

  //и записать в файл Parquet.
  task1_rez
    .write
    .format("parquet")
    .mode(SaveMode.Overwrite)
    .save("src/resources/data/rez/task1")

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
    .sortBy(_._2, false)
    .map( i => i.toString())

  //Результат вывести на экран
  task2_rez.foreach(println(_))

  // и в txt файл c пробелами.
  if(!fs.exists(new org.apache.hadoop.fs.Path("src/resources/data/rez/task2"))) {
    task2_rez.saveAsTextFile("src/resources/data/rez/task2")
  }

  //Task 3

  //Загрузить данные в DataSet из файла с фактическими данными поездок в Parquet (src/main/resources/data/yellow_taxi_jan_25_2018).
  val task3_taxi = spark.read.schema(schema_yellow_taxi)
    .parquet(path_to_yellow_taxi)
    .as[ActualDataTrips]

  //С помощью DSL и lambda построить таблицу, которая покажет. Как происходит распределение поездок по дистанции.
  val task3_rez = task3_taxi.map{i =>
    ActualDataTripsShort(i.trip_distance.round)
  }
    .groupByKey(_.trip_distance)
    .mapGroups{ (k,v) =>
      v.reduce{(a,b) => a.count += b.count
        a}
    }.describe()


  // Результат вывести на экран
  task3_rez.show()

  // и записать в файл Parquet.
  task3_rez.write
    .format("parquet")
    .mode(SaveMode.Overwrite)
    .save("src/resources/data/rez/task3")

  spark.stop()
}
