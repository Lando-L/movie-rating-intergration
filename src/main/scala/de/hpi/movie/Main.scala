package de.hpi.movie

import de.hpi.movie.core.{Integration, Movie}
import de.hpi.movie.source.imdb.{Imdb, ImdbInstances}
import de.hpi.movie.source.kaggle.{Kaggle, KaggleInstances}
import org.apache.spark.sql.SparkSession

object Main {
	def main(args: Array[String]): Unit = {
		import ImdbInstances._
		import KaggleInstances._

		val spark = SparkSession
			.builder()
			.appName("Movie Ratings Integration")
			.config("spark.master", "local")
			.getOrCreate()

		import spark.implicits._

		val movies = for {
			optImdb <- Integration.run[Imdb](Map("ratingPath" -> "../imdb/title.ratings.tsv", "basicPath" -> "../imdb/title.basics.tsv"))
			optKaggle <- Integration.run[Kaggle](Map("ratingPath" -> "../kaggle/ratings_small.csv", "basicPath" -> "../kaggle/movies_metadata.csv"))
		} yield List(optImdb, optKaggle).flatten.fold(spark.emptyDataset[Movie])(_ union _)

		movies.run(spark).select("title", "rating").write.csv("../merged/")
	}
}
