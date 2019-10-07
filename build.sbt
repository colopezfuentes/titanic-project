name := "option-project"

version := "0.1"

scalaVersion := "2.11.12"
val sparkVersion = "2.4.3"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
