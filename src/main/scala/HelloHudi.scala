import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}
import org.apache.hudi.config.HoodieWriteConfig

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import java.io.File
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import scala.reflect.io.Directory

object HelloHudi {

  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  val basePath = "F:\\Hudi\\Data"


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
  private val UPSERT_ALBUM_DATA_2 = Seq(
    Album(801, "801 String Theory - Special", Array("Jumpin' the blues", "Bluesnote", "Birth of blues"), dateToLong("2020-01-03")),
    Album(803, "803 Best Of Jazz Blues", Array("Jumpin' the blues", "Bluesnote", "Birth of blues"), dateToLong("2020-01-04")),
    Album(804, "804 Birth of Cool", Array("Move", "Jeru", "Moon Dreams"), dateToLong("2020-02-03"))
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
    //clearDirectory()

    val spark: SparkSession = SparkSession.builder().appName("hudi-datalake")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.hive.convertMetastoreParquet", "false")
      .config("spark.testing.memory", "2147480000")
      .getOrCreate()

    import spark.implicits._
    val tableName = "Album"
    //upsert(INITIAL_ALBUM_DATA.toDF(), tableName, "albumId", "updateDate")
    //spark.read.format("hudi").load(s"$basePath/$tableName/*").show() //sanpshot-1

   //upsert(UPSERT_ALBUM_DATA.toDF(), tableName, "albumId", "updateDate")
    //spark.read.format("hudi").load(s"$basePath/$tableName/*").show() //snapshot -2

    //upsert(UPSERT_ALBUM_DATA_2.toDF(), tableName, "albumId", "updateDate")
    //spark.read.format("hudi").load(s"$basePath/$tableName/*").show() //snapshot -3


  //  incrementalQuery(spark, basePath, tableName)
   pointInTimeQuery(spark, basePath, tableName)
    //deleteQuery(spark, basePath, tableName)
    spark.stop()
  }

  private def deleteQuery(spark: SparkSession, basePath: String, tableName: String): Unit = {
    val deleteKeys = Seq(
      Album(803, "", null, 0l),
      Album(802, "", null, 0l)
    )

    import spark.implicits._

    val df = deleteKeys.toDF()

    df.write.format("hudi")
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "albumId")
      .option(HoodieWriteConfig.TABLE_NAME, tableName)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL)
      .mode(SaveMode.Append) // Only Append Mode is supported for Delete.
      .save(s"$basePath/$tableName/")

    spark.read.format("hudi").load(s"$basePath/$tableName/*").show()
  }

  private def incrementalQuery(sparkSession: SparkSession, basePath: String, tableName: String): Unit = {
    import sparkSession.implicits._
    sparkSession.read.format("hudi").load(s"$basePath/$tableName/*").createOrReplaceTempView("hudi_Album_snapshot")
    val commits = sparkSession.sql("select distinct(_hoodie_commit_time) as commitTime from  hudi_Album_snapshot order by commitTime").map(k => k.getString(0)).take(50)
    print("********commit_length******"+commits.length)
    val beginTime = commits(commits.length-2) // commit time we are interested in
    print("********commit time*******"+beginTime)
    sparkSession.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, beginTime)
      .load(s"$basePath/$tableName/*").show()
  }

  private def pointInTimeQuery(sparkSession: SparkSession, basePath: String, tableName: String): Unit = {
    import sparkSession.implicits._
    sparkSession.read.format("hudi").load(s"$basePath/$tableName/*").createOrReplaceTempView("hudi_Album_snapshot")
    val commits = sparkSession.sql("select distinct(_hoodie_commit_time) as commitTime from  hudi_Album_snapshot order by commitTime").map(k => k.getString(0)).take(50)
    print("********commit_length******"+commits.length)
    val beginTime = "000"
    val endtime = commits(commits.length-2) // commit time we are interested in
    print("********commit time*******"+endtime)
    sparkSession.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, beginTime)
      .option(DataSourceReadOptions.END_INSTANTTIME_OPT_KEY, endtime)
      .load(s"$basePath/$tableName/*").show()
  }

  private def clearDirectory(): Unit = {
    val directory = new Directory(new File(basePath))
    directory.deleteRecursively()
  }
}
