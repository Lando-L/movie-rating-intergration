package de.hpi.movie.wrapper.imdb

import de.hpi.movie.{Movie, Rating}
import org.scalatest.{FlatSpec, Matchers}

class ImdbWrapperSpec extends FlatSpec with Matchers {
	"asMovie" should "translate an imdb instance to a movie instance" in {
		import ImdbWrapper._

		val movies = List(
			Imdb("1", Some(5.0), Some("Title 1")),
			Imdb("2", Some(3.0), None),
			Imdb("3", None, Some("Title 3"))
		).flatMap(asMovie)

		val expected = List(
			Movie("Title 1", Rating(0,10)(5.0).get, Map("imdb" -> "1"))
		)

		movies shouldEqual expected
	}
}
