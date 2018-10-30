package de.hpi.movie

import de.hpi.movie.core.{Integration, Movie}
import de.hpi.movie.source.imdb.{Imdb, ImdbInstances}
import de.hpi.movie.source.kaggle.{Kaggle, KaggleInstances}
import de.hpi.movie.source.movielens.{MovieLens, MovieLensInstances}
import org.apache.spark.sql.SparkSession

object Main {
	def main(args: Array[String]): Unit = {
		import ImdbInstances._
		import KaggleInstances._
		import MovieLensInstances._

		val spark = SparkSession
			.builder()
			.appName("Movie Ratings Integration")
			.config("spark.master", "local")
			.getOrCreate()

		import spark.implicits._

		val movies = for {
			optImdb <- Integration.run[Imdb](Map("ratingPath" -> "../imdb/title.ratings.tsv", "basicPath" -> "../imdb/title.basics.tsv"))
			optKaggle <- Integration.run[Kaggle](Map("ratingPath" -> "../kaggle/ratings_small.csv", "basicPath" -> "../kaggle/movies_metadata.csv"))
			optMovieLens <- Integration.run[MovieLens](Map("moviesPath" -> "../movie_lens/movies.csv", "ratingsPath" -> "../movie_lens/ratings.csv", "linksPath" -> "../movie_lens/links.csv"))
			merged = (List(optImdb, optKaggle, optMovieLens).flatten fold spark.emptyDataset[Movie])(_ union _)
		} yield merged

		movies.run(spark).select("title", "rating").write.csv("../merged/")
	}
}
