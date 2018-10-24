import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.example",
      scalaVersion := "2.11.12",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "movie-rating-integration",
    scalacOptions += "-Ypartial-unification",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.2",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.2",
    libraryDependencies += "org.typelevel" %% "cats-core" % "1.4.0"
  )
