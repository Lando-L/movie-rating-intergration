package de.hpi.movie.source.movielens

import cats.data.Reader
import de.hpi.movie.core.{Preparation, Movie, Wrapper}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object MovieLensInstances {
	implicit def movieLensPreparation: Preparation[MovieLens] with Serializable =
		new Preparation[MovieLens] with Serializable {
			override def normalize: Movie => Movie = {
				Preparation.normalizeRating(1,5)
			}
		}

	implicit def movieLensWrapper: Wrapper[MovieLens] with Serializable =
		new Wrapper[MovieLens] with Serializable {
			override protected def parse(config: Map[String, String]): Reader[SparkSession, Option[Dataset[MovieLens]]] =
				Reader {
					sparkSession =>
						import org.apache.spark.sql.functions._
						import sparkSession.implicits._

						for {
							moviesPath <- config get "moviesPath"
							ratingsPath <- config get "ratingsPath"
							linksPath <- config get "linksPath"
							movieDf = parseSource(moviesPath)(sparkSession).select("movieId", "title")
							ratingDf = parseSource(ratingsPath)(sparkSession)
								.select("movieId", "rating")
  							.groupBy("movieId")
  							.agg(avg("rating"))
  							.withColumnRenamed("avg(rating)", "rating")
							linksDf = parseSource(linksPath)(sparkSession).select("movieId", "imdbId")
							joined = movieDf.join(ratingDf, "movieId").join(linksDf, "movieId").as[MovieLens]
						} yield joined
				}

			override protected def asMovie(a: MovieLens): Option[Movie] = {
				for {
					title <- a.title
					rating <- a.rating
					fk = (a.imdbId fold Map("movie_lens" -> a.movieId))(id => Map("movie_lens" -> a.movieId, "imdb" -> id))
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
