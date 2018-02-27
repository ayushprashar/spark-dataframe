package Operations

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
case class Record (HomeTeam: String, AwayTeam: String,FTHG: Int,FTAG: Int,FTR: String)
case class TotalGames(Team: String, GamesPlayed: Int)
case class TotalWins(Team: String,GamesWon: Int)
case class FileAccessor(pathToCSV: String) {

  private val config: SparkConf = new SparkConf()
    .setMaster("local")
    .setAppName("spark-dataframes")

  private val spark: SparkSession = SparkSession
    .builder()
    .config(config)
    .getOrCreate()


  private val file: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(pathToCSV)
  private val add: UserDefinedFunction = udf((first: Int, second: Int) => first + second)
  file.createOrReplaceTempView("FOOTBALLSTATS")

  def displayHomeMatches(): Unit = {
    val query: DataFrame = spark.sql("SELECT COUNT(*) as MatchesPlayed,HomeTeam FROM FOOTBALLSTATS GROUP BY HomeTeam")
    query.show()
  }

  def displayWinPercent(): Unit = {
    val homeWins: DataFrame = file.where("FTR == 'H'").groupBy("HomeTeam").count.toDF("Team", "Wins")
    val awayWins: DataFrame = file.where("FTR == 'A'").groupBy("AwayTeam").count.toDF("Team", "Wins")
    val homeGames: DataFrame = file.groupBy("HomeTeam").count.toDF("Team", "Games")
    val awayGames: DataFrame = file.groupBy("AwayTeam").count.toDF("Team", "Games")



    val percent: UserDefinedFunction = udf((dividend: Int, divisor: Int) => (dividend * 100) / divisor)
    val gamesTotal: DataFrame = awayGames.join(homeGames, "Team").toDF("Team", "Home", "Away").withColumn("GameSum", add(col("Home"), col("Away")))

    val winsTotal: DataFrame = awayWins.join(homeWins, "Team")
      .toDF("Team", "Home", "Away")
      .withColumn("WinSum", add(col("Home"), col("Away")))

    val winPercent = gamesTotal.join(winsTotal, "Team")
      .toDF("Team", "Home Games", "Away Games", "Total Played", "Home Wins", "Away Wins", "Total Wins")
      .withColumn("Percent", percent(col("Total Wins"), col("Total Played")))
    winPercent.orderBy(desc("Percent")).select("Team", "Percent").show(10)

  }
  import spark.implicits._

  private val recordDataSet: Dataset[Record] = file.select("HomeTeam","AwayTeam","FTHG","FTAG","FTR").as[Record]
  def displayDataSet: Unit = recordDataSet.show()

  def totalMatchesPlayed: Unit = {
    val homeGames: DataFrame = recordDataSet.groupBy("HomeTeam").count.toDF("Team","Games")
    val awayGames: DataFrame = recordDataSet.groupBy("AwayTeam").count.toDF("Team","Games")
    val totalGames = homeGames.join(awayGames,"Team")
      .toDF("Team","Home","Away")
      .withColumn("GamesPlayed", add(col("Home"), col("Away")))

    val gamesDataSet = totalGames.select("Team","GamesPlayed").as[TotalGames]
    gamesDataSet.show()
  }

  def highestWins: Unit = {
    val homeWins: DataFrame = recordDataSet.where("FTR == 'H'").groupBy("HomeTeam").count.toDF("Team", "Wins")
    val awayWins: DataFrame = recordDataSet.where("FTR == 'A'").groupBy("AwayTeam").count.toDF("Team", "Wins")
    val totalWins: DataFrame = homeWins.join(awayWins,"Team")
      .toDF("Team","Home","Away")
      .withColumn("GamesWon",add(col("Home"),col("Away")))

    val winsDataset = totalWins.select("Team","GamesWon").as[TotalWins]
    winsDataset.sort(desc("GamesWon")).show(10)

  }
}
