package de.hpi.movie

case class Movie(title: String, rating: Rating, fk: Map[String, String] = Map())
