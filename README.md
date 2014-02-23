# DDF - Distributed DataFrame 

DDF aims to make Big Data easy yet powerful, by bringing together
the best ideas from R Data Science, RDBMS/SQL, and Big Data distributed
processing.

It exposes high-level abstractions like RDBMS tables,
SQL queries, data cleansing and transformations, machine-learning
algorithms, even collaboration and authentication, etc., while
hiding all the complexities of parallel distributed processing
and data handling.

DDF is a general abstraction that can be implemented on multiple
execution and data engines. We are providing a native implementation
on Apache Spark, as it is today the most expressive in its DAG
parallelization and also most powerful in its in-memory distributed
dataset abstraction (RDD). With this release, DDF provides native
Spark support for R, Python, Java, Scala.

An aim of the DDF project is to shine a focus of Big Data conversations
on top-down, user-focussed simplicity and power, where "users" include
business analysts, data scientists, and high-level Big Data engineers.

---

### Directory Structure

<table>
<tr><td>bin/</td><td>useful helper scripts</td></tr>
<tr><td>exe/</td><td>DDF execution/launch scripts and executables</td></tr>
<tr><td>conf/</td><td>DDF configuration files</td></tr>
<tr><td>clients/</td><td>DDF client code, e.g., R, Python, etc.</td></tr>
<tr><td>contrib/</td><td>Contributed DDF code that has not/does not fit into the core API</td></tr>
<tr><td>core/</td><td>DDF core API</td></tr>
<tr><td>spark/</td><td>DDF Spark implementation</td></tr>
<tr><td>examples/</td><td>DDF example API-user code</td></tr>
<tr><td>project/</td><td>Scala build config files</td></tr>
</table>

### Getting Started

First clone or fork a copy of DDF, e.g.:

    % git clone http://git.adatao.com/DDF

Now you need to prepare the build, which prepares the libraries,
creates pom.xml in the various sub-project directories, and Eclipse
.project and .classpath files.

    % cd DDF
    DDF % bin/run-once.sh

If you ever need to regenerated the pom.xml files:

    DDF % bin/make-poms.sh

The following regenerates Eclipse .project and .classpath files:
		
    DDF % bin/make-eclipse-projects.sh

### Building `DDF_core` or `DDF_spark`
		
    DDF/core % mvn clean package
    DDF/spark % mvn clean package

### Running tests
		
    DDF % bin/sbt test

or

    DDF/core % mvn test
    DDF/spark % mvn test
