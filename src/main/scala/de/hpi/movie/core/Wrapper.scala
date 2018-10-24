package de.hpi.movie.core

import org.apache.spark.sql.{Dataset, SparkSession}

trait Wrapper[A] {
	def load(config: Map[String, String])(sparkSession: SparkSession): Option[Dataset[A]]
	def asMovie(a: A): Option[Movie]
}

object Wrapper {
	def load[A](config: Map[String, String])(sparkSession: SparkSession)(implicit w: Wrapper[A]): Option[Dataset[A]] =
		w.load(config)(sparkSession)

	def asMovie[A](a: A)(implicit w: Wrapper[A]): Option[Movie] =
		w.asMovie(a)
}
