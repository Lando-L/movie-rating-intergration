package de.hpi.movie.wrapper

import de.hpi.movie.Movie
import org.apache.spark.sql.{Dataset, SparkSession}

trait Wrapper[A] {
	def load(spark: SparkSession)(config: Map[String, String]): Option[Dataset[A]]
	def asMovie(a: A): Option[Movie]
}
