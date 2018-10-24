package de.hpi.movie.core

import cats.data.Reader
import org.apache.spark.sql.SparkSession

trait Integration[A] {
	protected def normalize: Movie => Movie
	//def cluster[A]: Movie => A
	//def deduplicate: (Movie, Movie) => Double

	def setup(config: Map[String, String])(implicit w: Wrapper[A]): Reader[SparkSession, Unit] =
		Reader {
			sparkSession =>
				import sparkSession.implicits._

				w
					.load(config)(sparkSession)
					.map(_.flatMap(w.asMovie).map(normalize).count())
					.foreach(println)
		}
}

object Integration {
	def setup[A](config: Map[String, String])(implicit i: Integration[A], w: Wrapper[A]): Reader[SparkSession, Unit] =
		i.setup(config)
}
