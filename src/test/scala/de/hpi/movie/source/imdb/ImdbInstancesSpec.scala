package de.hpi.movie.source.imdb

import de.hpi.movie.core.{Movie, Preparation}
import org.scalatest.{FlatSpec, Matchers}

class ImdbInstancesSpec extends FlatSpec with Matchers {
	"preparation" should "normalize the movie rating" in {
		import ImdbInstances._

		val movies = Set(Movie("1", 1.0), Movie("2", 4.0), Movie("3", 5.0), Movie("4", 7.0), Movie("5", 10.0))
		val expected = Set(Movie("1", 0), Movie("2", 3.0/9), Movie("3", 4.0/9), Movie("4", 6.0/9), Movie("5", 1.0))

		movies.map(Preparation.normalize) shouldEqual expected
	}
}
