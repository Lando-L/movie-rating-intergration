package de.hpi.movie.core

trait Preparation[A] {
	def normalize: Movie => Movie
}

object Preparation {
	def normalize[A](implicit p: Preparation[A]): Movie => Movie =
		p.normalize
}
