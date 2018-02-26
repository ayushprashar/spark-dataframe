package Operations

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

case class FileAccessor(pathToCSV: String) {

  private val config: SparkConf = new SparkConf()
    .setMaster("local")
    .setAppName("spark-dataframes")

  private val spark: SparkSession = SparkSession
    .builder()
    .config(config)
    .getOrCreate()

  //val pathToCSV: String = "src/main/resources/D1.csv"
  private val file: DataFrame = spark.read.option("header","true").option("inferSchema","true").csv(pathToCSV)

  file.createOrReplaceTempView("FOOTBALLSTATS")

  def displayHomeMatches(): Unit = {
    val query: DataFrame = spark.sql("SELECT COUNT(*) as MatchesPlayed,HomeTeam FROM FOOTBALLSTATS GROUP BY HomeTeam")
    query.show()
  }
}
