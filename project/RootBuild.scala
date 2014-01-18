import sbt._
import sbt.Classpaths.publishTask
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._
//import twirl.sbt.TwirlPlugin._
import scala.sys.process._
// For Sonatype publishing
//import com.jsuereth.pgp.sbtplugin.PgpKeys._



object RootBuild extends Build {

	//////// Project definitions/configs ///////
	val SPARK_VERSION = "0.7.3-adatao"
	val SHARK_VERSION = "0.7.1"
	
	val theScalaVersion = "2.10.0"
	val targetDir = "target/scala-" + theScalaVersion // to help mvn and sbt share the same target dir

	val rootOrganization = "com.adatao"
	val projectName = "DDF"
	val rootProjectName = projectName
	val rootVersion = "1.0"

	val projectOrganization = rootOrganization + "." + projectName

	val coreProjectName = projectName + "_core"
	val coreVersion = rootVersion
	//val coreJarName = coreProjectName + "_" + theScalaVersion + "-" + coreVersion + ".jar"
	//val coreTestJarName = coreProjectName + "_" + theScalaVersion + "-" + coreVersion + "-tests.jar"
	val coreJarName = coreProjectName + "-" + coreVersion + ".jar"
	val coreTestJarName = coreProjectName + "-" + coreVersion + "-tests.jar"
	
	//lazy val root = Project("root", file("."), settings = rootSettings) aggregate(core, enterprise, community, examples)
	//lazy val root = Project("root", file("."), settings = rootSettings) aggregate(core, examples)
	lazy val root = Project("root", file("."), settings = rootSettings) aggregate(core)
	lazy val core = Project("core", file("core"), settings = coreSettings)
	//lazy val enterprise = Project("enterprise", file("enterprise"), settings = enterpriseSettings) dependsOn (core)
	//lazy val community = Project("community", file("community"), settings = communitySettings) dependsOn (core)
	lazy val examples = Project("examples", file("examples"), settings = examplesSettings) dependsOn (core) dependsOn (core)

	// A configuration to set an alternative publishLocalConfiguration
	lazy val MavenCompile = config("m2r") extend(Compile)
	lazy val publishLocalBoth = TaskKey[Unit]("publish-local", "publish local for m2 and ivy")


	//////// Variables/flags ////////

	// Hadoop version to build against. For example, "0.20.2", "0.20.205.0", or
	// "1.0.4" for Apache releases, or "0.20.2-cdh3u5" for Cloudera Hadoop.
	//val HADOOP_VERSION = "1.0.4"
	//val HADOOP_MAJOR_VERSION = "1"
	//val HADOOP_VERSION = "1.0.4"
	//val HADOOP_MAJOR_VERSION = "0"

	// For Hadoop 2 versions such as "2.0.0-mr1-cdh4.1.1", set the HADOOP_MAJOR_VERSION to "2"
	val HADOOP_VERSION = "2.0.0-mr1-cdh4.1.1"
	val HADOOP_MAJOR_VERSION = "2"

	val slf4jVersion = "1.7.2"
	val excludeJacksonCore = ExclusionRule(organization = "org.codehaus.jackson", name = "jackson-core-asl")
	val excludeJacksonMapper = ExclusionRule(organization = "org.codehaus.jackson", name = "jackson-mapper-asl")
	val excludeNetty = ExclusionRule(organization = "org.jboss.netty", name = "netty")
	val excludeScala = ExclusionRule(organization = "org.scala-lang", name = "scala-library")
	val excludeGuava = ExclusionRule(organization = "com.google.guava", name = "guava-parent")
	val excludeSpark = ExclusionRule(organization = "org.spark-project", name = "spark-core_2.10")
	val excludeEverthing = ExclusionRule(organization = "*", name = "*")
	val excludeEverythingHackForMakePom = ExclusionRule(organization = "_MAKE_POM_EXCLUDE_ALL_", name = "_MAKE_POM_EXCLUDE_ALL_")

	// We define this explicitly rather than via unmanagedJars, so that make-pom will generate it in pom.xml as well
	// org % package % version
	val adatao_unmanaged = Seq(
		//"adatao.unmanaged.edu.berkeley.amplab" % "hive_builtins" % "0.9.0.patched",
		//"adatao.unmanaged.edu.berkeley.amplab" % "hive_cli" % "0.9.0.patched",
		//"adatao.unmanaged.edu.berkeley.amplab" % "hive_common" % "0.9.0.patched",
		//"adatao.unmanaged.edu.berkeley.amplab" % "hive_contrib" % "0.9.0.patched",
		//"adatao.unmanaged.edu.berkeley.amplab" % "hive_exec" % "0.9.0.patched",
		//"adatao.unmanaged.edu.berkeley.amplab" % "hive_hbase_handler" % "0.9.0.patched",
		//"adatao.unmanaged.edu.berkeley.amplab" % "hive_hwi" % "0.9.0.patched",
		//"adatao.unmanaged.edu.berkeley.amplab" % "hive_jdbc" % "0.9.0.patched",
		//"adatao.unmanaged.edu.berkeley.amplab" % "hive_metastore" % "0.9.0.patched",
		//"adatao.unmanaged.edu.berkeley.amplab" % "hive_pdk" % "0.9.0.patched",
		//"adatao.unmanaged.edu.berkeley.amplab" % "hive_serde" % "0.9.0.patched",
		//"adatao.unmanaged.edu.berkeley.amplab" % "hive_service" % "0.9.0.patched",
		//"adatao.unmanaged.edu.berkeley.amplab" % "hive_shims" % "0.9.0.patched",
		//"adatao.unmanaged.net.rforge" % "REngine" % "1.7.2.compiled",
		//"adatao.unmanaged.net.rforge" % "Rserve" % "1.7.2.compiled"
	)

	/////// Common/Shared project settings ///////

	def commonSettings = Defaults.defaultSettings ++ Seq(
		//organization := projectOrganization,
		organization := rootOrganization,
		version := rootVersion,
		scalaVersion := theScalaVersion,
		scalacOptions := Seq("-unchecked", "-optimize", "-deprecation"),
		// See adatao_unmanaged: unmanagedJars in Compile <<= baseDirectory map { base => (base / "lib" ** "*.jar").classpath },
		//retrieveManaged := false, // Do not create a lib_managed, leave dependencies in ~/.ivy2
		retrieveManaged := true, // Do create a lib_managed, so we have one place for all the dependency jars to copy to slaves, if needed
		retrievePattern := "[type]s/[artifact](-[revision])(-[classifier]).[ext]",
		transitiveClassifiers in Scope.GlobalScope := Seq("sources"),
		////testListeners <<= target.map(t => Seq(new eu.henkelmann.sbt.JUnitXmlTestsListener(t.getAbsolutePath))),

		// Fork new JVMs for tests and set Java options for those
		fork in Test := true,
		javaOptions in Test ++= Seq("-Xmx2g"),

		// Only allow one test at a time, even across projects, since they run in the same JVM
		concurrentRestrictions in Global += Tags.limit(Tags.Test, 1),
		
		conflictManager := ConflictManager.strict,
		
		/*
		// For Sonatype publishing
		resolvers ++= Seq(
			"sonatype-snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
			"sonatype-staging" at "https://oss.sonatype.org/service/local/staging/deploy/maven2/"
		),
		*/
		
		// This goes first for fastest resolution. We need this for adatao_unmanaged.
		// Now, sometimes missing .jars in ~/.m2 can lead to sbt compile errors.
		// In that case, clean up the ~/.m2 local repository using bin/clean-m2-repository.sh
		// @aht: needs this to get Rserve jars, I don't know how to publish to adatao/mvnrepos
		resolvers ++= Seq("Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"),
		//resolvers ++= Seq("Adatao Mvnrepos Snapshots" at "https://raw.github.com/adatao/mvnrepos/master/snapshots",
		//		"Adatao Mvnrepos Releases" at "https://raw.github.com/adatao/mvnrepos/master/releases"),
		//resolvers ++= Seq("Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"),
		resolvers ++= Seq(
			//"BetaDriven Repository" at "http://nexus.bedatadriven.com/content/groups/public/",
			"Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
			"scala-tools.org" at "https://oss.sonatype.org/content/groups/scala-tools/"
//			"Spark-project" at "https://oss.sonatype.org/"
//			"Akka Repository" at "http://repo.akka.io/releases/"
		),
		
		

		publishMavenStyle := true, // generate pom.xml with "sbt make-pom"

		//useGpg in Global := true,

		pomExtra := (
			<!--
			**************************************************************************************************
			IMPORTANT: This file is generated by "sbt make-pom" (bin/make-poms.sh). Edits will be overwritten!
			**************************************************************************************************
			-->
			<parent>
				<groupId>{rootOrganization}</groupId>
				<artifactId>{rootProjectName}</artifactId>
				<version>{rootVersion}</version>
			</parent>
			<build>
				<directory>${{basedir}}/{targetDir}</directory>
				<plugins>
					<plugin>
						<!-- Let SureFire know where the jars are -->
						<groupId>org.apache.maven.plugins</groupId>
						<artifactId>maven-surefire-plugin</artifactId>
						<version>2.15</version>
						<configuration>			
							<reuseForks>false</reuseForks>
							<environmentVariables>
								<RSERVER_JAR>${{basedir}}/{targetDir}/{coreJarName},${{basedir}}/{targetDir}/{coreTestJarName}</RSERVER_JAR>
							</environmentVariables>
							<systemPropertyVariables>
								<spark.serializer>spark.KryoSerializer</spark.serializer>
								<spark.kryo.registrator>shark.KryoRegistrator</spark.kryo.registrator>
								<spark.ui.port>8085</spark.ui.port>
								<log4j.configuration>{projectName}-log4j.properties</log4j.configuration>							
							</systemPropertyVariables>
						</configuration>
					</plugin>
					<plugin>
						<groupId>org.apache.maven.plugins</groupId>
						<artifactId>maven-jar-plugin</artifactId>
						<version>2.2</version>
						<executions>
							<execution>
								<goals><goal>test-jar</goal></goals>
							</execution>
						 </executions>
					</plugin>
					<plugin>
						<groupId>net.alchim31.maven</groupId>
						<artifactId>scala-maven-plugin</artifactId>
						<version>3.1.5</version>
						<configuration>
							<recompileMode>incremental</recompileMode>
						</configuration>
					</plugin>
				</plugins>
			</build>		
			<profiles>

				<profile>
					<id>local</id>
					<activation><property><name>!dist</name></property></activation>
					<build>
						<directory>${{basedir}}/{targetDir}</directory>
						<plugins>
							<plugin>
								<groupId>org.apache.maven.plugins</groupId>
								<artifactId>maven-surefire-plugin</artifactId>
								<version>2.15</version>
								<configuration>
									<additionalClasspathElements>
										<additionalClasspathElement>${{basedir}}/conf/local</additionalClasspathElement>
									</additionalClasspathElements>						
								</configuration>
							</plugin>
						</plugins>
					</build>
				</profile>

				<profile>
					<id>nospark</id>
					<activation><property><name>nospark</name></property></activation>
					<build>
						<directory>${{basedir}}/{targetDir}</directory>
						<plugins>
							<plugin>
								<!-- Let SureFire know where the jars are -->
								<groupId>org.apache.maven.plugins</groupId>
								<artifactId>maven-surefire-plugin</artifactId>
								<version>2.15</version>
								<configuration>
									<additionalClasspathElements>
										<additionalClasspathElement>${{basedir}}/conf/local</additionalClasspathElement>
									</additionalClasspathElements>						
									<includes><include>**</include></includes>
									<excludes><exclude>**/spark/**</exclude></excludes>
								</configuration>
							</plugin>
						</plugins>
					</build>
				</profile>

				<profile>
					<id>package</id>
					<activation><property><name>package</name></property></activation>
					<build>
						<directory>${{basedir}}/{targetDir}</directory>
						<plugins>
							<plugin>
								<!-- Let SureFire know where the jars are -->
								<groupId>org.apache.maven.plugins</groupId>
								<artifactId>maven-surefire-plugin</artifactId>
								<version>2.15</version>
								<configuration>
									<additionalClasspathElements>
										<additionalClasspathElement>${{basedir}}/conf/local</additionalClasspathElement>
									</additionalClasspathElements>						
									<includes><include>**/${{path}}/**</include></includes>
								</configuration>
							</plugin>
						</plugins>
					</build>
				</profile>
			</profiles>				
		),

		/*
		publishTo <<= version { (v: String) =>
			val nexus = "https://oss.sonatype.org/"
			if (v.trim.endsWith("SNAPSHOT"))
				Some("sonatype-snapshots" at nexus + "content/repositories/snapshots")
			else
				Some("sonatype-staging"  at nexus + "service/local/staging/deploy/maven2")
		},
		*/

		libraryDependencies ++= Seq(
			//"commons-configuration" % "commons-configuration" % "1.6",
			//"com.google.code.gson"% "gson" % "2.2.2",
			//"org.spark-project" % "spark-core_2.10" % SPARK_VERSION, 
			//"edu.berkeley.cs.amplab" % "shark_2.10" % SHARK_VERSION excludeAll(excludeSpark), //use Adatao version of spark
			//"javax.jdo" % "jdo2-api" % "2.3-eb",
			//"org.eclipse.jetty" % "jetty-server" % "7.6.8.v20121106",
			//"org.eclipse.jetty" % "jetty-util" % "7.6.8.v20121106",
			"org.scalatest" % "scalatest_2.10" % "2.0", // depends on scala-lang 2.10.0
			//"org.scalacheck" % "scalacheck_2.10" % "1.10.0", // depends on scala-lang 2.10.0
			//"com.novocode" % "junit-interface" % "0.9" % "test",
			"org.easymock" % "easymock" % "3.1" % "test"
			
		),

		// for fast linear algebra
		libraryDependencies += "org.jblas" % "jblas" % "1.2.3",

		/* Already in plugins.sbt
		addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.8.5"),
		addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "2.1.1"),
		addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.2.0"),
		addSbtPlugin("io.spray" %% "sbt-twirl" % "0.6.1"),
		*/

		otherResolvers := Seq(Resolver.file("dotM2", file(Path.userHome + "/.m2/repository"))),
		publishLocalConfiguration in MavenCompile <<= (packagedArtifacts, deliverLocal, ivyLoggingLevel) map {
			(arts, _, level) => new PublishConfiguration(None, "dotM2", arts, Seq(), level)
		},
		publishMavenStyle in MavenCompile := true,
		publishLocal in MavenCompile <<= publishTask(publishLocalConfiguration in MavenCompile, deliverLocal),
		publishLocalBoth <<= Seq(publishLocal in MavenCompile, publishLocal).dependOn,
		
		//dependencyOverrides += "org.scala-lang" % "scala-library" % theScalaVersion,
		//dependencyOverrides += "commons-configuration" % "commons-configuration" % "1.6",
		//dependencyOverrides += "commons-logging" % "commons-logging" % "1.1.1",
		//dependencyOverrides += "commons-lang" % "commons-lang" % "2.6",
		//dependencyOverrides += "it.unimi.dsi" % "fastutil" % "6.4.4",
		dependencyOverrides += "log4j" % "log4j" % "1.2.17",
		dependencyOverrides += "org.slf4j" % "slf4j-api" % slf4jVersion force(),
		dependencyOverrides += "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
		//dependencyOverrides += "commons-io" % "commons-io" % "2.4", //tachyon 0.2.1
		//dependencyOverrides += "org.apache.thrift" % "libthrift" % "0.9.0", //bigr
		//dependencyOverrides += "org.apache.httpcomponents" % "httpclient" % "4.1.3", //libthrift
		//dependencyOverrides += "org.apache.commons" % "commons-math" % "2.1", //hadoop-core, renjin newer use a newer version but we prioritize hadoop
		//dependencyOverrides += "com.google.guava" % "guava" % "11.0.1", //spark-core, renjin use a newer version but we prioritize spark
		//dependencyOverrides += "asm" % "asm" % "3.3.1", //org.datanucleus#datanucleus-enhancer's
		//dependencyOverrides += "org.codehaus.jackson" % "jackson-core-asl" % "1.8.8",
		//dependencyOverrides += "org.codehaus.jackson" % "jackson-mapper-asl" % "1.8.8",
		//dependencyOverrides += "org.codehaus.jackson" % "jackson-xc" % "1.8.8",
		//dependencyOverrides += "org.codehaus.jackson" % "jackson-jaxrs" % "1.8.8",
		//dependencyOverrides += "org.eclipse.jetty" % "jetty-util" % "7.6.8.v20121106",
		dependencyOverrides += "org.apache.httpcomponents" % "httpcore" % "4.1.4"
	) // end of commonSettings


	/////// Individual project settings ///////

	def rootSettings = commonSettings ++ Seq(publish := {})


	def coreSettings = commonSettings ++ Seq(
		name := coreProjectName,

		javaOptions in Test <+= baseDirectory map {dir => "-Dspark.classpath=" + dir + "/../lib_managed/jars/*"},
		javaOptions in Test <+= baseDirectory map {dir => "-Drserver.jar=" + dir + "/" + targetDir + "/" + coreJarName},

		// Add post-compile activities: touch the maven timestamp files so mvn doesn't have to compile again
		compile in Compile <<= compile in Compile andFinally { List("sh", "-c", "touch core/" + targetDir + "/*timestamp") },

		//unmanagedSourceDirectories in Compile <+= baseDirectory{ _ / ("src/hadoop" + HADOOP_MAJOR_VERSION + "/scala") }

		resolvers ++= Seq(
			//"JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
			//"Spray Repository" at "http://repo.spray.cc/",
			//"Twitter4J Repository" at "http://twitter4j.org/maven2/"
			//"Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
		),

		libraryDependencies ++= adatao_unmanaged ++ Seq(
			//"org.antlr" % "antlr" % "3.0.1", // needed by shark.SharkDriver.compile
			//"org.renjin" % "renjin-script-engine" % "0.7.0-RC6" excludeAll(ExclusionRule(organization="org.renjin", name="gcc-bridge-plugin")),
			//"com.google.code.findbugs" % "jsr305" % "1.3.9", // to avoid [error] error while loading Optional, Missing dependency 'class javax.annotation.Nullable', required by /home/ctn/src/adatao/BigR/lib_managed/jars/guava-13.0.jar(com/google/common/base/Optional.class)			
			//"mysql" % "mysql-connector-java" % "5.1.25"
			
		) ++ (
			//if (HADOOP_MAJOR_VERSION == "2")
			//	Some("org.apache.hadoop" % "hadoop-client" % HADOOP_VERSION)
			//else
				None
		).toSeq ++ Seq(
			// Per @ngon: [you need these Hive dependencies, else] though you can compile, but are not able to run.
			//"org.apache.avro" % "avro-mapred" % "1.5.3",
			//"commons-dbcp" % "commons-dbcp" % "1.4",
			//"org.datanucleus" % "datanucleus-rdbms" % "2.0.3",
			//"org.datanucleus" % "datanucleus-enhancer" % "2.0.3",
			//"org.datanucleus" % "datanucleus-connectionpool" % "2.0.3",
			//"org.datanucleus" % "datanucleus-core" % "2.0.3",
			//"org.apache.derby" % "derby" % "10.4.2.0",
			//"org.apache.hbase" % "hbase" % "0.92.0" excludeAll(excludeEverthing, excludeEverythingHackForMakePom),
			// Now under adatao_unmanaged "javax.jdo" % "jdo2-api" % "2.3-ec",
			//"org.mortbay.jetty" % "jetty-util" % "6.1.26",
			//"jline" % "jline" % "0.9.94",
			//"org.json" % "json" % "20090211",
			//"org.apache.thrift" % "libfb303" % "0.9.0"
		)
	) ++ assemblySettings ++ extraAssemblySettings //++ Twirl.settings


	//def communitySettings = commonSettings ++ Seq(
	//	name := projectName + "-community",
	//	libraryDependencies <+= scalaVersion("org.scala-lang" % "scala-compiler" % _)
	//)


	//def enterpriseSettings = commonSettings ++ Seq(
	//	name := projectName + "-enterprise",
	//	libraryDependencies <+= scalaVersion("org.scala-lang" % "scala-compiler" % _)
	//)


	def examplesSettings = commonSettings ++ Seq(
		name := projectName + "-examples"
		//libraryDependencies ++= Seq("com.twitter" % "algebird-core_2.9.2" % "0.1.11")
	)


	/*
	def streamingSettings = commonSettings ++ Seq(
		name := "spark-streaming",
		libraryDependencies ++= Seq(
			"org.apache.flume" % "flume-ng-sdk" % "1.2.0" % "compile" excludeAll(excludeNetty),
			"com.github.sgroschupf" % "zkclient" % "0.1",
			"org.twitter4j" % "twitter4j-stream" % "3.0.3" excludeAll(excludeNetty),
			"com.typesafe.akka" % "akka-zeromq" % "2.0.3" excludeAll(excludeNetty)
		)
	) ++ assemblySettings ++ extraAssemblySettings
	*/


	def extraAssemblySettings() = Seq(test in assembly := {}) ++ Seq(
		mergeStrategy in assembly := {
			case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
			case "reference.conf" => MergeStrategy.concat
			case _ => MergeStrategy.first
		}
	)
}

