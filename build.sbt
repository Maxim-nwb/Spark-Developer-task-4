ThisBuild / version := "1.0"

ThisBuild / scalaVersion := "2.12.15"

val sparkVersion = "3.2.1"

lazy val sparkDependencies = Seq(
  "org.apache.spark"         %% "spark-sql"            % sparkVersion,
  "org.apache.logging.log4j" % "log4j-core"            % "2.20.0",
  "io.netty"          % "netty-all"                   % "4.1.97.Final"
)
//
lazy val root = (project in file("."))
  .settings(
    name := "SparkTask4",
    libraryDependencies ++= sparkDependencies,
    javacOptions ++= Seq("-source", "16"),
    javaOptions ++= Seq( // Spark-specific JVM options
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    ),
    compileOrder := CompileOrder.JavaThenScala
  )