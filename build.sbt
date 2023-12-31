name := "Movie Rating Spark"

version := "0.1"
val sparkVersion = {
  "3.2.4"
}
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.scala-lang" % "scala-library" % "2.13.8",
  "org.scalatest" %% "scalatest" % "3.2.3" % "test"

)