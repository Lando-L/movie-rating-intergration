package de.hpi.movie.source.movielens

import de.hpi.movie.core.{Movie, Preparation}
import org.scalatest.{FlatSpec, Matchers}

class MovieLensInstancesSpec extends FlatSpec with Matchers {
	"preparation" should "normalize the movie rating" in {
		import MovieLensInstances._

		val movies = Set(Movie("1", 1.0), Movie("2", 2.0), Movie("3", 3.0), Movie("4", 4.0), Movie("5", 5.0))
		val expected = Set(Movie("1", 0), Movie("2", 0.25), Movie("3", 0.5), Movie("4", 0.75), Movie("5", 1.0))

		movies.map(Preparation.normalize) shouldEqual expected
	}
}
