import Operations.FileAccessor

object MainApp extends App {
//  def main(args: Array[String]): Unit = {
    val path: String = "src/main/resources/D1.csv"
    val file = FileAccessor(path)
    file.displayHomeMatches()
}
