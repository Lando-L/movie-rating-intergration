/*package de.hpi.movie.source.kaggle

import de.hpi.movie.core.{Integration, Movie, Normalization, Wrapper}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object KaggleInstances {
	implicit def kaggleIntegration: Integration[Kaggle] =
		new Integration[Kaggle] {
			override protected def normalize: Movie => Movie = {
				Normalization.normalizeRating(0, 5)
			}
		}

	implicit def kaggleWrapper: Wrapper[Kaggle] =
		new Wrapper[Kaggle] {
			override def load(config: Map[String, String])(sparkSession: SparkSession): Option[Dataset[Kaggle]] = {
				import org.apache.spark.sql.functions._
				import sparkSession.implicits._

				for {
					ratingPath <- config get "ratingPath"
					basicPath <- config get "basicPath"
					ratingDf = parseSource(ratingPath)(sparkSession).select("movieId", "rating").groupBy("movieId").agg(avg("rating"))
					basicDf = parseSource(basicPath)(sparkSession).select("id", "imdb_id", "title")
					joined = ratingDf.join(basicDf, ratingDf("movieId") === basicDf("id"))
				} yield joined.select("id", "imdb_id", "title", "avg(rating)").as[Kaggle]
			}

			override def asMovie(a: Kaggle): Option[Movie] = {
				for {
					title <- a.title
					rating <- a.rating
					fk = (a.imdb_id fold Map("kaggle" -> a.id))(id => Map("kaggle" -> a.id, "imdb" -> id))
				} yield Movie(title, rating, fk)
			}

			def parseSource(path: String)(sparkSession: SparkSession): DataFrame = {
				sparkSession
					.read
					.format("csv")
					.option("inferSchema", "true")
					.option("header", "true")
					.load(path)
			}
		}
}*/
