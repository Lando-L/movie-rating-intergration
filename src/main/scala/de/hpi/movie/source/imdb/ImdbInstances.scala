package de.hpi.movie.source.imdb

import cats.data.Reader
import de.hpi.movie.core.{Preparation, Movie, Wrapper}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object ImdbInstances {
	implicit val imdbIntegration: Preparation[Imdb] with Serializable =
		new Preparation[Imdb] with Serializable {
			override def normalize: Movie => Movie = {
				Preparation.normalizeRating(1, 10)
			}
		}

	implicit val imdbWrapper: Wrapper[Imdb] with Serializable =
		new Wrapper[Imdb] with Serializable {
			override def parse(config: Map[String, String]): Reader[SparkSession, Option[Dataset[Imdb]]] =
				Reader {
					sparkSession =>
						import sparkSession.implicits._
						for {
							ratingPath <- config get "ratingPath"
							basicPath <- config get "basicPath"
							ratingDf = parseSource(ratingPath)(sparkSession)
								.select("tconst", "averageRating")
							basicDf = parseSource(basicPath)(sparkSession)
								.select("tconst", "primaryTitle")
							joined = ratingDf.join(basicDf, "tconst")
						} yield joined.as[Imdb]
			}

			override def asMovie(imdb: Imdb): Option[Movie] = {
				for {
					title <- imdb.primaryTitle
					rating <- imdb.averageRating
					fk = Map("imdb" -> imdb.tconst)
				} yield Movie(title, rating, fk)
			}

			def parseSource(path: String)(sparkSession: SparkSession): DataFrame = {
				sparkSession
					.read
					.format("csv")
					.option("sep", "\t")
					.option("inferSchema", "true")
					.option("header", "true")
					.load(path)
			}
		}
}
