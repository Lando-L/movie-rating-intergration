package de.hpi.movie.core

object Normalization {
	type Normalize = Movie => Movie

	def normalizeRating(min: Double, max: Double): Normalize = {
		movie => movie.copy(rating = movie.rating - min / (max - min))
	}
}
