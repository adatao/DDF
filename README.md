DDF
===

Distributed DataFrame - Native R Support on Spark, with API access for R, Python, Java, Scala

### Getting Started

		DDF% bin/run-once.sh

### Regenerating pom.xml

    DDF% bin/make-poms.sh

### Regenerating Eclipse projects
		
    DDF% bin/make-eclipse-projects.sh

### Building `DDF_core`
		
    DDF/core% mvn clean package

### Running tests
		
    DDF% bin/sbt test

or

   DDF/core% mvn test
