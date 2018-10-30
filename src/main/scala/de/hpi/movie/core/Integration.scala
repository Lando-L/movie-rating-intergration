package de.hpi.movie.core

import cats.data.Reader
import org.apache.spark.sql.{Dataset, Encoder, SparkSession}

object Integration {
	def run[A](config: Map[String, String])(implicit w: Wrapper[A], p: Preparation[A], e: Encoder[Movie]): Reader[SparkSession, Option[Dataset[Movie]]] = {
		w.load(config).map(_.map(_.map(p.normalize)))
	}
}
