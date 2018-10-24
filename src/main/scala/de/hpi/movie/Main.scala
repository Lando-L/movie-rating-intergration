package de.hpi.movie

import de.hpi.movie.core.Integration
import de.hpi.movie.source.imdb.{Imdb, ImdbInstances}
import org.apache.spark.sql.SparkSession

object Main {
	def main(args: Array[String]): Unit = {
		import ImdbInstances._

		val spark = SparkSession
			.builder()
			.appName("Movie Ratings Integration")
			.config("spark.master", "local")
			.getOrCreate()

		val is = Integration
			.setup[Imdb](Map("ratingPath" -> "../imdb/title.ratings-10.tsv", "basicPath" -> "../imdb/title.basics-10.tsv"))
			.run(spark)

		is.foreach(_.show())

		/*KaggleWrapper
  		.load(spark)(Map("ratingPath" -> "../kaggle/ratings_small.csv", "basicPath" -> "../kaggle/movies_metadata.csv"))
  		.foreach(_.show(10))*/
	}
}
