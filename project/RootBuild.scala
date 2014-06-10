import sbt._
import sbt.Classpaths.publishTask
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._
import scala.sys.process._
import scala.util.Properties.{ envOrNone => env }
import scala.collection.JavaConversions._


object RootBuild extends Build {

  //////// Project definitions/configs ///////
  val SPARK_VERSION = "0.9.1"
  val SHARK_VERSION = "0.9.1"
        val HIVE_VERSION = "0.11.0-shark-0.9.1"
  // Target JVM version
  val SCALAC_JVM_VERSION = "jvm-1.6"
  val JAVAC_JVM_VERSION = "1.6"
  val theScalaVersion = "2.10.3"
        val majorScalaVersion = theScalaVersion.split(".[0-9]+$")(0)
  val targetDir = "target/scala-" + majorScalaVersion // to help mvn and sbt share the same target dir
  //val theScalaVersion = "2.9.3"
  //val targetDir = "target/scala-" + theScalaVersion // to help mvn and sbt share the same target dir

  val rootOrganization = "com.adatao"
  val projectName = "ddf"
  val rootProjectName = projectName
  val rootVersion = "0.9"

  val projectOrganization = rootOrganization + "." + projectName

  val coreProjectName = projectName + "_core"
  val coreVersion = rootVersion
  val coreJarName = coreProjectName.toLowerCase + "_" + theScalaVersion + "-" + coreVersion + ".jar"
  val coreTestJarName = coreProjectName + "-" + coreVersion + "-tests.jar"

  val sparkProjectName = projectName + "_spark"
  val sparkVersion = rootVersion
  val sparkJarName = sparkProjectName.toLowerCase + "_" + theScalaVersion + "-" + sparkVersion + ".jar"
  val sparkTestJarName = sparkProjectName.toLowerCase + "_" + theScalaVersion + "-" + sparkVersion + "-tests.jar"
  
  val enterpriseProjectName = projectName + "_enterprise"
  val enterpriseVersion = rootVersion
  val enterpriseJarName = enterpriseProjectName + "-" + sparkVersion + ".jar"
  val enterpriseTestJarName = enterpriseProjectName + "-" + sparkVersion + "-tests.jar"

  val paProjectName = projectName + "_pa"
  val paVersion = rootVersion
  val paJarName = paProjectName + "_" + theScalaVersion + "-" + sparkVersion + ".jar"
  val paTestJarName = paProjectName + "_" + theScalaVersion + "-" + sparkVersion + "-tests.jar"

  val examplesProjectName = projectName + "_examples"
  val examplesVersion = rootVersion
  val examplesJarName = examplesProjectName + "-" + sparkVersion + ".jar"
  val examplesTestJarName = examplesProjectName + "-" + sparkVersion + "-tests.jar"

  val contribProjectName = projectName + "_contrib"
  val contribVersion = rootVersion
  val contribJarName = contribProjectName + "-" + contribVersion + ".jar"
  val contribTestJarName = contribProjectName + "-" + contribVersion + "-tests.jar"
  
  lazy val root = Project("root", file("."), settings = rootSettings) aggregate(core, spark, examples, contrib,pa,enterprise)
  lazy val core = Project("core", file("core"), settings = coreSettings)
  lazy val spark = Project("spark", file("spark"), settings = sparkSettings) dependsOn (core)
  lazy val enterprise = Project("enterprise", file("enterprise"), settings = enterpriseSettings) dependsOn (core) dependsOn(spark)
  lazy val pa = Project("pa", file("pa"), settings = paSettings) dependsOn (core) dependsOn(spark)
  lazy val examples = Project("examples", file("examples"), settings = examplesSettings) dependsOn (spark) dependsOn (core)
  lazy val contrib = Project("contrib", file("contrib"), settings = contribSettings) dependsOn (spark) dependsOn(core)

  // A configuration to set an alternative publishLocalConfiguration
  lazy val MavenCompile = config("m2r") extend(Compile)
  lazy val publishLocalBoth = TaskKey[Unit]("publish-local", "publish local for m2 and ivy")


  //////// Variables/flags ////////

  // Hadoop version to build against. For example, "0.20.2", "0.20.205.0", or
  // "1.0.4" for Apache releases, or "0.20.2-cdh3u5" for Cloudera Hadoop.
  val HADOOP_VERSION = "1.0.4"
  val HADOOP_MAJOR_VERSION = "0"

  // For Hadoop 2 versions such as "2.0.0-mr1-cdh4.1.1", set the HADOOP_MAJOR_VERSION to "2"
  //val HADOOP_VERSION = "2.0.0-mr1-cdh4.1.1"
  //val HADOOP_MAJOR_VERSION = "2"

  val slf4jVersion = "1.7.2"
  val excludeAvro = ExclusionRule(organization = "org.apache.avro" , name = "avro-ipc")
  val excludeJacksonCore = ExclusionRule(organization = "org.codehaus.jackson", name = "jackson-core-asl")
  val excludeJacksonMapper = ExclusionRule(organization = "org.codehaus.jackson", name = "jackson-mapper-asl")
  val excludeNetty = ExclusionRule(organization = "org.jboss.netty", name = "netty")
  val excludeScala = ExclusionRule(organization = "org.scala-lang", name = "scala-library")
  val excludeGuava = ExclusionRule(organization = "com.google.guava", name = "guava-parent")
  val excludeJets3t = ExclusionRule(organization = "net.java.dev.jets3t", name = "jets3t")
  val excludeAsm = ExclusionRule(organization = "asm", name = "asm")
  val excludeSpark = ExclusionRule(organization = "org.apache.spark", name = "spark-core_2.10")
  val excludeEverthing = ExclusionRule(organization = "*", name = "*")
  val excludeEverythingHackForMakePom = ExclusionRule(organization = "_MAKE_POM_EXCLUDE_ALL_", name = "_MAKE_POM_EXCLUDE_ALL_")

  // We define this explicitly rather than via unmanagedJars, so that make-pom will generate it in pom.xml as well
  // org % package % version
  val com_adatao_unmanaged = Seq(
    "com.adatao.unmanaged.net.rforge" % "REngine" % "1.7.2.compiled",
    "com.adatao.unmanaged.net.rforge" % "Rserve" % "1.7.2.compiled"
  )

  val scalaArtifacts = Seq("jline", "scala-compiler", "scala-library", "scala-reflect")
  val scalaDependencies = scalaArtifacts.map( artifactId => "org.scala-lang" % artifactId % theScalaVersion)

  val spark_dependencies = Seq(
    "commons-configuration" % "commons-configuration" % "1.6",
    "com.google.code.gson"% "gson" % "2.2.2",
    //"javax.jdo" % "jdo2-api" % "2.3-ec",
    "org.eclipse.jetty" % "jetty-server" % "8.1.14.v20131031",
    "org.eclipse.jetty" % "jetty-security" % "8.1.14.v20131031",
    "org.eclipse.jetty" % "jetty-util" % "8.1.14.v20131031",
    "org.eclipse.jetty" % "jetty-plus" % "8.1.14.v20131031",
    "org.eclipse.jetty" % "jetty-servlet" % "8.1.14.v20131031",
    "org.eclipse.jetty" % "jetty-webapp" % "8.1.14.v20131031",
    "org.eclipse.jetty" % "jetty-jsp" % "8.1.14.v20131031",
    //"org.scalatest" %% "scalatest" % "1.9.1" % "test",
    //"org.scalacheck" %% "scalacheck" % "1.10.0" % "test",
    "com.novocode" % "junit-interface" % "0.9" % "test",
    "org.jblas" % "jblas" % "1.2.3", // for fast linear algebra
    "org.apache.thrift" % "libthrift" % "0.9.0",
    "org.apache.thrift" % "libfb303" % "0.9.0",
    "org.apache.hbase" % "hbase" % "0.94.20",
    "com.google.protobuf" % "protobuf-java" % "2.4.1",
    //"org.antlr" % "antlr" % "3.4", // needed by shark.SharkDriver.compile
    // needed by Hive
    //"commons-dbcp" % "commons-dbcp" % "1.4",
    //"org.apache.derby" % "derby" % "10.4.2.0",
   // "org.apache.spark" % "spark-streaming_2.10" % SPARK_VERSION excludeAll(excludeSpark),
    "org.apache.spark" % "spark-core_2.10" % SPARK_VERSION excludeAll(excludeJets3t) exclude("com.google.protobuf", "protobuf-java") exclude("io.netty", "netty-all") exclude("org.jboss.netty", "netty"),
    "org.apache.spark" % "spark-repl_2.10" % SPARK_VERSION excludeAll(excludeSpark) exclude("com.google.protobuf", "protobuf-java") exclude("io.netty", "netty-all") exclude("org.jboss.netty", "netty"),
    "org.apache.spark" % "spark-mllib_2.10" % SPARK_VERSION excludeAll(excludeSpark) exclude("com.google.protobuf", "protobuf-java") exclude("io.netty", "netty-all") exclude("org.jboss.netty", "netty"),
    //"edu.berkeley.cs.amplab" % "shark_2.9.3" % SHARK_VERSION excludeAll(excludeSpark)
    "edu.berkeley.cs.shark" %% "shark" % SHARK_VERSION exclude("org.apache.avro", "avro-ipc") exclude("com.google.protobuf", "protobuf-java") exclude("io.netty", "netty-all")
//libraryDependencies ++= Seq("edu.berkeley.cs.shark" %% "shark" % SHARK_VERSION)
  )

  val pa_dependencies = Seq(
    "com.googlecode.matrix-toolkits-java" % "mtj" % "0.9.14"
    //"org.renjin" % "renjin-script-engine" % "0.7.0-RC6" excludeAll(ExclusionRule(organization="org.renjin", name="gcc-bridge-plugin"))
  )

  /////// Common/Shared project settings ///////

  def commonSettings = Defaults.defaultSettings ++ Seq(
    organization := projectOrganization,
    version := rootVersion,
    scalaVersion := theScalaVersion,
    scalacOptions := Seq("-unchecked", "-optimize", "-deprecation"),
    //retrieveManaged := false, // Do not create a lib_managed, leave dependencies in ~/.ivy2
    retrieveManaged := true, // Do create a lib_managed, so we have one place for all the dependency jars to copy to slaves, if needed
    retrievePattern := "[type]s/[artifact](-[revision])(-[classifier]).[ext]",
    transitiveClassifiers in Scope.GlobalScope := Seq("sources"),

    // Fork new JVMs for tests and set Java options for those
    fork in Test := true,
    javaOptions in Test ++= Seq("-Xmx2g"),

    // Only allow one test at a time, even across projects, since they run in the same JVM
    concurrentRestrictions in Global += Tags.limit(Tags.Test, 1),

    conflictManager := ConflictManager.strict,

    // This goes first for fastest resolution. We need this for com_adatao_unmanaged.
    // Now, sometimes missing .jars in ~/.m2 can lead to sbt compile errors.
    // In that case, clean up the ~/.m2 local repository using bin/clean-m2-repository.sh
    // @aht: needs this to get Rserve jars, I don't know how to publish to adatao/mvnrepos
//resolvers ++= Seq("Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"),
resolvers ++= Seq("Local Maven" at Path.userHome.asFile.toURI.toURL+".m2/repository"),   
//resolvers ++= Seq("Adatao Repo Snapshots"  at "https://raw.github.com/adatao/mvnrepos/master/snapshots",
         // "Adatao Repo Releases"   at "https://raw.github.com/adatao/mvnrepos/master/releases"),
    resolvers ++= Seq(
      //"BetaDriven Repository" at "http://nexus.bedatadriven.com/content/groups/public/",
      "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
      "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
      "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"
      //"Sonatype Testing" at "https://oss.sonatype.org/content/repositories/eduberkeleycs-1016"

      //"Sonatype Staging" at "https://oss.sonatype.org/service/local/staging/deploy/maven2/"
    ),


    publishMavenStyle := true, // generate pom.xml with "sbt make-pom"

    libraryDependencies ++= Seq(
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
      "commons-configuration" % "commons-configuration" % "1.6",
      "com.google.guava" % "guava" % "14.0.1",
      "com.google.code.gson"% "gson" % "2.2.2",
      //"org.scalatest" % "scalatest_2.10" % "2.1.0" % "test",
      "org.scalatest" % "scalatest_2.10" % "2.2.0-M1" % "test",
      "org.scalacheck"   %% "scalacheck" % "1.11.0" % "test",
      "com.novocode" % "junit-interface" % "0.9" % "test",
      "org.jblas" % "jblas" % "1.2.3", // for fast linear algebra
      "com.googlecode.matrix-toolkits-java" % "mtj" % "0.9.14",
      "commons-io" % "commons-io" % "1.3.2",
      "org.easymock" % "easymock" % "3.1" % "test",
      "edu.berkeley.cs.shark" % "hive-contrib" % "0.11.0-shark-0.9.1" exclude("com.google.protobuf", "protobuf-java") exclude("io.netty", "netty-all") exclude("org.jboss.netty", "netty"),
      "mysql" % "mysql-connector-java" % "5.1.25"
    ),


    otherResolvers := Seq(Resolver.file("dotM2", file(Path.userHome + "/.m2/repository"))),


    publishLocalConfiguration in MavenCompile <<= (packagedArtifacts, deliverLocal, ivyLoggingLevel) map {
      (arts, _, level) => new PublishConfiguration(None, "dotM2", arts, Seq(), level)
    },
    publishMavenStyle in MavenCompile := true,
    publishLocal in MavenCompile <<= publishTask(publishLocalConfiguration in MavenCompile, deliverLocal),
    publishLocalBoth <<= Seq(publishLocal in MavenCompile, publishLocal).dependOn,


    //dependencyOverrides += "org.scala-lang" % "scala-library" % theScalaVersion,
    //dependencyOverrides += "org.scala-lang" % "scala-compiler" % theScalaVersion,
    // dependencyOverrides += "commons-configuration" % "commons-configuration" % "1.6",
    // dependencyOverrides += "commons-logging" % "commons-logging" % "1.1.1",
    dependencyOverrides += "commons-lang" % "commons-lang" % "2.6",
    dependencyOverrides += "it.unimi.dsi" % "fastutil" % "6.4.4",
    dependencyOverrides += "log4j" % "log4j" % "1.2.17",
    dependencyOverrides += "org.slf4j" % "slf4j-api" % slf4jVersion,
    dependencyOverrides += "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
    dependencyOverrides += "commons-io" % "commons-io" % "2.4", //tachyon 0.2.1
    dependencyOverrides += "org.apache.thrift" % "libthrift" % "0.9.0", //bigr
    dependencyOverrides += "org.apache.httpcomponents" % "httpclient" % "4.1.3", //libthrift
    //dependencyOverrides += "org.apache.commons" % "commons-math" % "2.1", //hadoop-core, renjin newer use a newer version but we prioritize hadoop
    dependencyOverrides += "com.google.guava" % "guava" % "14.0.1", //spark-core
    // dependencyOverrides += "org.codehaus.jackson" % "jackson-core-asl" % "1.8.8",
    dependencyOverrides += "org.codehaus.jackson" % "jackson-mapper-asl" % "1.8.8",
    // dependencyOverrides += "org.codehaus.jackson" % "jackson-xc" % "1.8.8",
    // dependencyOverrides += "org.codehaus.jackson" % "jackson-jaxrs" % "1.8.8"
    dependencyOverrides += "com.thoughtworks.paranamer" % "paranamer" % "2.4.1", //net.liftweb conflict with avro
    dependencyOverrides += "org.xerial.snappy" % "snappy-java" % "1.0.5", //spark-core conflicts with avro
    dependencyOverrides += "org.apache.httpcomponents" % "httpcore" % "4.1.4",
    dependencyOverrides += "org.apache.avro" % "avro-ipc" % "1.7.4",
    dependencyOverrides += "org.apache.avro" % "avro" % "1.7.4",
    dependencyOverrides += "org.apache.zookeeper" % "zookeeper" % "3.4.5",
    dependencyOverrides += "net.java.dev.jets3t" % "jets3t" % "0.9.0",
    dependencyOverrides += "org.eclipse.jetty" % "jetty-server" % "8.1.14.v20131031",
    dependencyOverrides += "org.eclipse.jetty" % "jetty-jndi" % "8.1.14.v20131031",
     dependencyOverrides += "org.eclipse.jetty" % "jetty-security" % "8.1.14.v20131031",
     dependencyOverrides += "org.eclipse.jetty" % "jetty-util" % "8.1.14.v20131031",
     dependencyOverrides += "org.eclipse.jetty" % "jetty-plus" % "8.1.14.v20131031",
     dependencyOverrides += "org.eclipse.jetty" % "jetty-servlet" % "8.1.14.v20131031",
     dependencyOverrides += "org.eclipse.jetty" % "jetty-webapp" % "8.1.14.v20131031",
     dependencyOverrides += "org.eclipse.jetty" % "jetty-jsp" % "8.1.14.v20131031",

    dependencyOverrides += "io.netty" % "netty" % "3.6.6.Final",
    dependencyOverrides += "asm" % "asm" % "4.0", //org.datanucleus#datanucleus-enhancer's

    
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
                <enableAssertions>false</enableAssertions>
                <environmentVariables>
                    <RSERVER_JAR>${{basedir}}/{targetDir}/*.jar,${{basedir}}/{targetDir}/lib/*</RSERVER_JAR>              
                </environmentVariables>
                <systemPropertyVariables>
                  <spark.serializer>org.apache.spark.serializer.KryoSerializer</spark.serializer>
                  <spark.kryo.registrator>com.adatao.spark.content.KryoRegistrator</spark.kryo.registrator>
                  <spark.ui.port>8085</spark.ui.port>
                  <log4j.configuration>ddf-log4j.properties</log4j.configuration>
                  <derby.stream.error.file>${{basedir}}/target/derby.log</derby.stream.error.file>
                </systemPropertyVariables>
                <additionalClasspathElements>
                  <additionalClasspathElement>${{basedir}}/conf/</additionalClasspathElement>
                  <additionalClasspathElement>${{basedir}}/conf/local/</additionalClasspathElement>
                  <additionalClasspathElement>${{basedir}}/../lib_managed/jars/*</additionalClasspathElement>
                  <additionalClasspathElement>${{HADOOP_HOME}}/conf/</additionalClasspathElement>
                  </additionalClasspathElements>
                <includes>
                  <include>**/*.java</include>
                </includes>
              </configuration>
            </plugin>
            <plugin>
              <groupId>org.apache.maven.plugins</groupId>
              <artifactId>maven-assembly-plugin</artifactId>
              <version>2.2.2</version>
              <configuration>
                <descriptors>
                  <descriptor>assembly.xml</descriptor>
                </descriptors>
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
            <plugin>
              <groupId>org.apache.maven.plugins</groupId>
              <artifactId>maven-checkstyle-plugin</artifactId>
              <version>2.6</version>
              <configuration>
                <configLocation>${{basedir}}/../src/main/resources/sun_checks.xml</configLocation>
                <propertyExpansion>checkstyle.conf.dir=${{basedir}}/../src/main/resources</propertyExpansion>
                <outputFileFormat>xml</outputFileFormat>
              </configuration>
            </plugin>
            
            <!--
            <plugin>
                <groupId>org.scalastyle</groupId>
                <artifactId>scalastyle-maven-plugin</artifactId>
                <version>0.4.0</version>
                <configuration>
                  <verbose>false</verbose>
                  <failOnViolation>true</failOnViolation>
                  <includeTestSourceDirectory>true</includeTestSourceDirectory>
                  <failOnWarning>false</failOnWarning>
                  <sourceDirectory>${{basedir}}/src/main/scala</sourceDirectory>
                  <testSourceDirectory>${{basedir}}/src/test/scala</testSourceDirectory>
                  <configLocation>${{basedir}}/../src/main/resources/scalastyle-config.xml</configLocation>
                  <outputFile>${{basedir}}/{targetDir}/scalastyle-output.xml</outputFile>
                  <outputEncoding>UTF-8</outputEncoding>
                </configuration>
                <executions>
                  <execution>
                    <goals>
                      <goal>check</goal>
                    </goals>
                  </execution>
                </executions>
              </plugin>
              -->
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
            <id>distributed</id>
            <activation><property><name>dist</name></property></activation>
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
                      <additionalClasspathElement>${{basedir}}/../lib_managed/jars/*</additionalClasspathElement>
                      <additionalClasspathElement>${{basedir}}/conf/distributed/</additionalClasspathElement>
                      <additionalClasspathElement>${{HADOOP_HOME}}/conf/</additionalClasspathElement>
                      <additionalClasspathElement>${{HIVE_HOME}}/conf/</additionalClasspathElement>
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
      )

  ) // end of commonSettings


  /////// Individual project settings //////
  def rootSettings = commonSettings ++ Seq(publish := {})


  def coreSettings = commonSettings ++ Seq(
    name := coreProjectName,
    //javaOptions in Test <+= baseDirectory map {dir => "-Dspark.classpath=" + dir + "/../lib_managed/jars/*"},
    // Add post-compile activities: touch the maven timestamp files so mvn doesn't have to compile again
    compile in Compile <<= compile in Compile andFinally { List("sh", "-c", "touch core/" + targetDir + "/*timestamp") },
    libraryDependencies += "org.xerial" % "sqlite-jdbc" % "3.7.2",
    libraryDependencies ++= scalaDependencies
  ) ++ assemblySettings ++ extraAssemblySettings



  def sparkSettings = commonSettings ++ Seq(
    name := sparkProjectName,
    javaOptions in Test <+= baseDirectory map {dir => "-Dspark.classpath=" + dir + "/../lib_managed/jars/*"},
    // Add post-compile activities: touch the maven timestamp files so mvn doesn't have to compile again
    compile in Compile <<= compile in Compile andFinally { List("sh", "-c", "touch spark/" + targetDir + "/*timestamp") },
    resolvers ++= Seq(
      //"JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
      //"Spray Repository" at "http://repo.spray.cc/",
      //"Twitter4J Repository" at "http://twitter4j.org/maven2/"
      //"Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
    ),
    libraryDependencies ++= com_adatao_unmanaged,
    libraryDependencies ++= spark_dependencies
  ) ++ assemblySettings ++ extraAssemblySettings



  def paSettings = commonSettings ++ Seq(
    name := paProjectName,
    javaOptions in Test <+= baseDirectory map {dir => "-Dspark.classpath=" + dir + "/../lib_managed/jars/*"},
    // Add post-compile activities: touch the maven timestamp files so mvn doesn't have to compile again
    compile in Compile <<= compile in Compile andFinally { List("sh", "-c", "touch pa/" + targetDir + "/*timestamp") },
    libraryDependencies ++= pa_dependencies
  ) ++ assemblySettings ++ extraAssemblySettings


  def enterpriseSettings = commonSettings ++ Seq(
    name := enterpriseProjectName,
    //javaOptions in Test <+= baseDirectory map {dir => "-Dspark.classpath=" + dir + "/../lib_managed/jars/*"},
    // Add post-compile activities: touch the maven timestamp files so mvn doesn't have to compile again
    compile in Compile <<= compile in Compile andFinally { List("sh", "-c", "touch enterprise/" + targetDir + "/*timestamp") }
  ) ++ assemblySettings ++ extraAssemblySettings


  def examplesSettings = commonSettings ++ Seq(
    name := examplesProjectName,
    //javaOptions in Test <+= baseDirectory map {dir => "-Dspark.classpath=" + dir + "/../lib_managed/jars/*"},
    // Add post-compile activities: touch the maven timestamp files so mvn doesn't have to compile again
    compile in Compile <<= compile in Compile andFinally { List("sh", "-c", "touch examples/" + targetDir + "/*timestamp") }
  ) ++ assemblySettings ++ extraAssemblySettings



  def contribSettings = commonSettings ++ Seq(
    name := contribProjectName,
    //javaOptions in Test <+= baseDirectory map {dir => "-Dspark.classpath=" + dir + "/../lib_managed/jars/*"},
    // Add post-compile activities: touch the maven timestamp files so mvn doesn't have to compile again
    compile in Compile <<= compile in Compile andFinally { List("sh", "-c", "touch contrib/" + targetDir + "/*timestamp") }
  ) ++ assemblySettings ++ extraAssemblySettings



  def extraAssemblySettings() = Seq(test in assembly := {}) ++ Seq(
    mergeStrategy in assembly := {
      case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
      case "reference.conf" => MergeStrategy.concat
      case _ => MergeStrategy.first
    }
  )
}
