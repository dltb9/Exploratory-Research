name :="SFFS Project"

version:="1.0"

organization := "orgg.idaslab"

scalaVersion :="2.10.2"

libraryDependencies+="org.apache.spark" % "spark-core_2.10" % "1.6.0" % "provided"
libraryDependencies+="org.apache.spark" % "spark-sql_2.10" % "1.6.0" % "provided" 

assemblyJarName in assembly := "myproject-assembly-1.0.jar"
test in assembly := {}
