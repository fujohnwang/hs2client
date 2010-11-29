import sbt._

class HelloWorldProject(info: ProjectInfo) extends DefaultProject(info)
{
	/* use local maven repository with auto dependency management*/
	val mavenLocalRepo         = "Local Maven Repository" at "file://" + Path.userHome + "/.m2/repository"
	val sonatypeNexusSnapshots = "Sonatype Nexus Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
	val sonatypeNexusReleases  = "Sonatype Nexus Releases" at "https://oss.sonatype.org/content/repositories/releases"
	val jbossRepository        = "Jboss Repository" at "http://repository.jboss.org/nexus/content/groups/public"
	val jbossRepository2       = "jboss Maven2 repo" at "http://repository.jboss.com/maven2"
	val scalatools             = "scala tools release" at "http://scala-tools.org/repo-releases"
	val nexusScalaToolsRepo    = "Nexus Scala tools Repo" at "http://nexus.scala-tools.org/content/repositories/releases/"
	val fuseSourceRepo         = "HawtDispatch Repo" at "http://repo.fusesource.com/nexus/content/repositories/public"
	
	val netty = "org.jboss.netty" % "netty" % "3.2.2.Final"
	val slf4jApi = "org.slf4j" % "slf4j-api" % "1.6.1"
	val logback = "ch.qos.logback" % "logback-core" % "0.9.21"
	val logbackClassic = "ch.qos.logback" % "logback-classic" % "0.9.21"
	
}