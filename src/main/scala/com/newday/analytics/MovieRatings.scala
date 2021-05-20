package com.newday.analytics

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{max, _}
import org.slf4j.{Logger, LoggerFactory}

object MovieRatings extends Utilities {
  def main(args: Array[String]): Unit = {
    val logger: Logger = LoggerFactory.getLogger(getClass)

    val inputPath = args(0)
    val outputPath = args(1)

    logger.info(s"input path = ${inputPath}")
    logger.info(s"input path = ${outputPath}")

//    val sparkConfig = new SparkConf()
//    sparkConfig.setIfMissing("spark.master", "local[4]")

    val spark = SparkSession
      .builder()
//      .config(sparkConfig)
      .appName("movie_ratings_job")
      .getOrCreate()

    logger.info(s"start reading movies data")
    val moviesDF = csvReader(spark, inputPath, "movies").persist()

    logger.info(s"start reading ratings data")
    val ratingsDF = csvReader(spark, inputPath, "ratings").persist()

    logger.info(s"create spark temp view for ratings dataframe")
    ratingsDF.createOrReplaceTempView("ratings_view")

    val ratingsUpdatedDF = spark.sql("SELECT movie_id, " +
                                               "MAX(rating) as max_rating, MIN(rating) as min_Rating, AVG(rating) as avg_Rating " +
                                               "FROM ratings_view GROUP BY movie_id")

    val movieRatingsDF = moviesDF.join(ratingsUpdatedDF, moviesDF("movie_id") === ratingsUpdatedDF("movie_id"), "left")
      .select(moviesDF("movie_id")
        , moviesDF("title")
        , moviesDF("genres")
        , ratingsUpdatedDF("max_rating")
        , ratingsUpdatedDF("min_rating")
        , ratingsUpdatedDF("avg_rating")
      )

    val userWindow  = Window.partitionBy(col("user_id")).orderBy(col("rating").desc)
    val rankedMoviesPerUserDF = ratingsDF.withColumn("movie_rank", row_number().over(userWindow))

    val usersTop3MoviesDF = rankedMoviesPerUserDF.where(col("movie_rank") <= 3).join(moviesDF, usingColumn = "movie_id")
    val usersTop3MoviesNamesDF = usersTop3MoviesDF.select(
      rankedMoviesPerUserDF("user_id")
      , rankedMoviesPerUserDF("movie_id")
      , rankedMoviesPerUserDF("movie_rank")
      , moviesDF("title")
    ).orderBy("user_id", "movie_rank")

    logger.info(s"write moviesRatingDF as parquet files in the output location")
    writeDF(movieRatingsDF, outputPath, "movie_ratings")

    logger.info(s"write usersTop3MoviesNamesDF as parquet files in the output location")
    writeDF(usersTop3MoviesNamesDF, outputPath, "top_3_movies_per_user")

  }

}
