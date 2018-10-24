package de.hpi.movie.core

case class Movie(title: String, rating: Double, fk: Map[String, String] = Map())
