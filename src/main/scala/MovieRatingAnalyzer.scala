import org.apache.spark.sql.{SparkSession,DataFrame}
import org.apache.spark.sql.functions._




object MovieRatingAnalyzer {

  def main(args: Array[String]): Unit = {
    val path = "src/main/resources/movie_metadata.csv"

    val spark = SparkSession
      .builder()
      .appName("Movie Rating Spark")
      .master("local")
      .getOrCreate()

    val movieRatingsDF: DataFrame = spark.read.option("header", "true").csv(path)

    val result:DataFrame = calculateMeanAndStdDev(movieRatingsDF)

    result.show(truncate = false)

    spark.stop()

  }


  def calculateMeanAndStdDev(movieData: DataFrame): DataFrame = {
    if (movieData.columns.contains("imdb_score")) {
      movieData.select(mean("imdb_score").as("mean_rating"), stddev("imdb_score").as("std_dev_rating"))
    } else {
      println("Error: 'imdb_score' column not found in the CSV file. Upload a proper csv file.")

      //returning an empty dataframe if the column is not present in the csv
      movieData.limit(0)
    }
  }
}


