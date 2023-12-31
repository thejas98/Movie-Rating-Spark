import org.apache.spark.sql.{SparkSession, DataFrame}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.functions._
import MovieRatingAnalyzer.calculateMeanAndStdDev

class MovieRatingAnalyzerSpec extends AnyFlatSpec with Matchers {

  private val spark: SparkSession = SparkSession.builder()
    .appName("MovieRatingAnalyzerTest")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  "calculateMeanAndStdDev" should "return an empty DataFrame if 'imdb_score' column is not present" in {
    import spark.implicits._

    // Test data without 'imdb_score' column
    val testData = Seq(
      (1, "Avengers"),
      (2, "Thor"),
      (3, "A beautiful mind")
    )

    val columns = Seq("id", "title")
    val movieDataWithoutImdbScore: DataFrame = testData.toDF(columns: _*)

    val result = calculateMeanAndStdDev(movieDataWithoutImdbScore)


    //result should be 0 since imdb_score column in not present
    result.count() shouldBe 0
  }

  "calculateMeanAndStdDev" should "return the correct mean and standard deviation" in {
    import spark.implicits._

    // Test data - To test if the function(calculateMeanAndStdDev) i created works properly
    val testData = Seq(
      (1, "Avengers", 7.5),
      (2, "Thor", 8.0),
      (3, "A beautiful mind", 6.5),
    )

    val columns = Seq("id", "title", "imdb_score")
    val movieRatingsDF: DataFrame = testData.toDF(columns: _*)

    val result = calculateMeanAndStdDev(movieRatingsDF)

    val expectedMean = testData.map(_._3).sum / testData.length.toDouble
    val expectedStdDev = math.sqrt(testData.map(score => math.pow(score._3 - expectedMean, 2)).sum / (testData.length - 1).toDouble)

    val resultRow = result.head()
    val resultMean = resultRow.getAs[Double]("mean_rating")
    val resultStdDev = resultRow.getAs[Double]("std_dev_rating")

    resultMean shouldEqual expectedMean +- 0.001
    resultStdDev shouldEqual expectedStdDev +- 0.001
  }
}
