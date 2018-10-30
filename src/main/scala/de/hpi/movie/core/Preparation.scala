package de.hpi.movie.core

trait Preparation[A] {
	import Preparation._

	def normalize: Normalize
}

object Preparation {
	type Normalize = Movie => Movie

	def normalize[A](implicit p: Preparation[A]): Normalize =
		p.normalize

	def normalizeRating(min: Double, max: Double): Normalize = {
		movie => movie.copy(rating = (movie.rating - min) / (max - min))
	}
}
