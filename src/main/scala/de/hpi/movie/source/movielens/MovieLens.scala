package de.hpi.movie.source.movielens

case class MovieLens(movieId: String, imdbId: Option[String], title: Option[String], rating: Option[Double])
