package de.hpi.movie

case class Rating(value: Double) extends AnyVal

object Rating {
	def apply(min: Double, max: Double)(rating: Double): Option[Rating] =
		Some(rating)
			.filter(x => min <= x && x <= max)
			.map(rating => new Rating(rating / (max - min)))
}
