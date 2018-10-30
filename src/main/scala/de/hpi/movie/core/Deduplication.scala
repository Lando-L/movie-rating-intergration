package de.hpi.movie.core

trait Deduplication[A] {
	import Deduplication._

	def score(weights: List[(Score, Double)]): Score =
		{ case (m1, m2) => (weights foldLeft 0.0){ case (sum, (f, w)) => sum + f(m1, m2) * w }}
}

object Deduplication {
	type Score = (Movie, Movie) => Double

	def idMatch(field: String): Score = {
		case (m1, m2) =>
			val score = for {
				id1 <- m1.fk get field
				id2 <- m2.fk get field
				if id1 == id2
			} yield 1.0

			score.getOrElse(0.0)
	}

	def nameMatch(similarityMeasure: (String, String) => Double): Score = {
		case (m1, m2) => similarityMeasure(m1.title, m2.title)
	}
}
