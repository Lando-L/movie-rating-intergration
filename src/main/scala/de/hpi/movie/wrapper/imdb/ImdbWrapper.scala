package de.hpi.movie.wrapper.imdb

import de.hpi.movie.{Movie, Rating}
import de.hpi.movie.wrapper.Wrapper
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object ImdbWrapper extends Wrapper[Imdb] {
	override def load(spark: SparkSession)(config: Map[String, String]): Option[Dataset[Imdb]] = {
		import spark.implicits._
		for {
			ratingPath <- config get "ratingPath"
			basicPath <- config get "basicPath"
			ratingDf = parseSource(spark)(ratingPath).select("tconst", "averageRating")
			basicDf = parseSource(spark)(basicPath).select("tconst", "primaryTitle")
			joined = ratingDf.join(basicDf, "tconst")
		} yield joined.as[Imdb]
	}

	override def asMovie(imdb: Imdb): Option[Movie] = {
		for {
			title <- imdb.primaryTitle
			rating <- imdb.averageRating flatMap Rating.apply(0, 10)
			fk = Map("imdb" -> imdb.tconst)
		} yield Movie(title, rating, fk)
	}

	def parseSource(spark: SparkSession)(path: String): DataFrame = {
		spark
			.read
			.format("csv")
			.option("sep", "\t")
			.option("inferSchema", "true")
			.option("header", "true")
			.load(path)
	}
}
