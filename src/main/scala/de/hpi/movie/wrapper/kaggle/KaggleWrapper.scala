package de.hpi.movie.wrapper.kaggle

import de.hpi.movie.{Movie, Rating}
import de.hpi.movie.wrapper.Wrapper
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object KaggleWrapper extends Wrapper[Kaggle] {
	override def load(spark: SparkSession)(config: Map[String, String]): Option[Dataset[Kaggle]] = {
		import org.apache.spark.sql.functions._
		import spark.implicits._

		for {
			ratingPath <- config get "ratingPath"
			basicPath <- config get "basicPath"
			ratingDf = parseSource(spark)(ratingPath).select("movieId", "rating").groupBy("movieId").agg(avg("rating"))
			basicDf = parseSource(spark)(basicPath).select("id", "imdb_id", "title")
			joined = ratingDf.join(basicDf, ratingDf("movieId") === basicDf("id"))
		} yield joined.select("id", "imdb_id", "title", "avg(rating)").as[Kaggle]
	}

	override def asMovie(a: Kaggle): Option[Movie] = {
		for {
			title <- a.title
			rating <- a.rating flatMap Rating(0,5)
			fk = (a.imdb_id fold Map("kaggle" -> a.id))(id => Map("kaggle" -> a.id, "imdb" -> id))
		} yield Movie(title, rating, fk)
	}

	def parseSource(spark: SparkSession)(path: String): DataFrame = {
		spark
			.read
			.format("csv")
			.option("inferSchema", "true")
			.option("header", "true")
			.load(path)
	}
}
