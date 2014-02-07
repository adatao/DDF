DDF (Distributed DataFrame)
===

Distributed DataFrame - Native R Support on Spark, with API access for R, Python, Java, Scala

### Getting Started

    DDF% bin/run-once.sh

### Regenerating pom.xml (already done in run-once.sh)

    DDF% bin/make-poms.sh

### Regenerating Eclipse projects (already done in run-once.sh)
		
    DDF% bin/make-eclipse-projects.sh

### Building `DDF_core` or `DDF_spark`
		
    DDF/core% mvn clean package
    DDF/spark% mvn clean package

### Running tests
		
    DDF% bin/sbt test

or

    DDF/core% mvn test
    DDF/spark% mvn test
