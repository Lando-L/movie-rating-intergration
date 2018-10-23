package de.hpi.movie

import de.hpi.movie.wrapper.imdb.ImdbWrapper
import de.hpi.movie.wrapper.kaggle.KaggleWrapper
import org.apache.spark.sql.SparkSession

object Main {
	def main(args: Array[String]): Unit = {
		val spark = SparkSession
			.builder()
			.appName("Movie Ratings Integration")
			.config("spark.master", "local")
			.getOrCreate()

		/*ImdbWrapper
			.load(spark)(Map("ratingPath" -> "../imdb/title.ratings-10.tsv", "basicPath" -> "../imdb/title.basics-10.tsv"))
			.foreach(_.show(10))*/

		KaggleWrapper
  		.load(spark)(Map("ratingPath" -> "../kaggle/ratings_small.csv", "basicPath" -> "../kaggle/movies_metadata.csv"))
  		.foreach(_.show(10))
	}
}
