import Operations.FileAccessor

object MainApp extends App {
    val path: String = "src/main/resources/D1.csv"
    val file = FileAccessor(path)
    file.displayHomeMatches
    file.displayWinPercent

    file.displayDataSet
    file.totalMatchesPlayed
    file.highestWins
}
