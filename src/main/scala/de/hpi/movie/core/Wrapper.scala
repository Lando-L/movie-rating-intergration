package de.hpi.movie.core

import cats.data.Reader
import org.apache.spark.sql.{Dataset, SparkSession}

trait Wrapper[A] {
	protected def parse(config: Map[String, String]): Reader[SparkSession, Option[Dataset[A]]]
	protected def asMovie(a: A): Option[Movie]

	def load(config: Map[String, String]): Reader[SparkSession, Option[Dataset[Movie]]] =
		Reader {
			sparkSession =>
				import sparkSession.implicits._
				parse(config)(sparkSession).map(_.flatMap(asMovie))
		}
}

object Wrapper {
	def load[A](config: Map[String, String])(implicit w: Wrapper[A]): Reader[SparkSession, Option[Dataset[Movie]]] =
		w.load(config)
}
