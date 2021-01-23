name := "huditest"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.7"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-avro" % sparkVersion
)

libraryDependencies += "org.apache.hudi" %% "hudi-spark" % "0.6.0"
libraryDependencies += "org.apache.parquet" % "parquet-avro" % "1.8.1"

