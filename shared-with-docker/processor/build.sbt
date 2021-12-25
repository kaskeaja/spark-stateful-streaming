name := "DeltaLakeProcessor"
version := "0.1"
scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2"
libraryDependencies += "io.delta" %% "delta-core" % "1.0.0"

//libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.3.5"

// scala test
libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.10"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.10" % "test"

