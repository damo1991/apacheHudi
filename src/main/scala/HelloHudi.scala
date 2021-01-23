import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}
import org.apache.hudi.config.HoodieWriteConfig

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import java.io.File
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import scala.reflect.io.Directory

case class Album(albumId: Long, title: String, tracks: Array[String], updateDate: Long)
object HelloHudi {

  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val basePath = "C:\\Users\\user\\Documents\\hudidata"


  private def dateToLong(dateString: String): Long = LocalDate.parse(dateString, formatter).toEpochDay

  private val INITIAL_ALBUM_DATA = Seq(
    Album(800, "6 String Theory", Array("Lay it down", "Am I Wrong", "68"), dateToLong("2019-12-01")),
    Album(801, "Hail to the Thief", Array("2+2=5", "Backdrifts"), dateToLong("2019-12-01")),
    Album(801, "Hail to the Thief", Array("2+3=5", "Backdrifts", "Go to sleep"), dateToLong("2019-12-03"))
  )

  private val UPSERT_ALBUM_DATA = Seq(
    Album(800, "6 String Theory - Special", Array("Jumpin' the blues", "Bluesnote", "Birth of blues"), dateToLong("2020-01-03")),
    Album(802, "Best Of Jazz Blues", Array("Jumpin' the blues", "Bluesnote", "Birth of blues"), dateToLong("2020-01-04")),
    Album(803, "Birth of Cool", Array("Move", "Jeru", "Moon Dreams"), dateToLong("2020-02-03"))
  )

  private def upsert(albumDf: DataFrame, tableName: String, key: String, combineKey: String): Unit = {
    albumDf.write
      .format("hudi")
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, key) //primary key, supports multiple columns/nested fields  by a.b.c
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, combineKey) //When two records have the same key value, the record with the largest value from the field specified will be chosen.
      .option(HoodieWriteConfig.TABLE_NAME, tableName)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .mode(SaveMode.Append)
      .save(s"$basePath/$tableName/")
  }

  def main(args: Array[String]) {
    clearDirectory()

    val spark: SparkSession = SparkSession.builder().appName("hudi-datalake")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.hive.convertMetastoreParquet", "false")
      .config("spark.testing.memory", "2147480000")
      .getOrCreate()

    import spark.implicits._
    val tableName = "Album"
    upsert(INITIAL_ALBUM_DATA.toDF(), tableName, "albumId", "updateDate")
    spark.read.format("hudi").load(s"$basePath/$tableName/*").show() //sanpshot-1

    upsert(UPSERT_ALBUM_DATA.toDF(), tableName, "albumId", "updateDate")
    spark.read.format("hudi").load(s"$basePath/$tableName/*").show() //snapshot -2


    incrementalQuery(spark, basePath, tableName)
    spark.stop()
  }

  private def incrementalQuery(sparkSession: SparkSession, basePath: String, tableName: String): Unit = {
    sparkSession.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, "20210122233606")
      .load(s"$basePath/$tableName/*").show()
  }

  private def clearDirectory(): Unit = {
    val directory = new Directory(new File(basePath))
    directory.deleteRecursively()
  }
}
