package de.hpi.movie.source.kaggle

import cats.data.Reader
import de.hpi.movie.core.{Preparation, Movie, Wrapper}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object KaggleInstances {
	implicit def kaggleIntegration: Preparation[Kaggle] with Serializable =
		new Preparation[Kaggle] with Serializable {
			override def normalize: Movie => Movie = {
				Preparation.normalizeRating(1, 5)
			}
		}

	implicit def kaggleWrapper: Wrapper[Kaggle] with Serializable =
		new Wrapper[Kaggle] with Serializable {
			override def parse(config: Map[String, String]): Reader[SparkSession, Option[Dataset[Kaggle]]] =
				Reader {
					sparkSession =>
						import org.apache.spark.sql.functions._
						import sparkSession.implicits._

						for {
							ratingPath <- config get "ratingPath"
							basicPath <- config get "basicPath"
							ratingDf = parseSource(ratingPath)(sparkSession)
								.select("movieId", "rating")
								.groupBy("movieId")
								.agg(avg("rating"))
								.withColumnRenamed("avg(rating)", "rating")
							basicDf = parseSource(basicPath)(sparkSession)
								.select("id", "imdb_id", "title")
							joined = ratingDf.join(basicDf, ratingDf("movieId") === basicDf("id"))
						} yield joined.select("id", "imdb_id", "title", "rating").as[Kaggle]
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
}
