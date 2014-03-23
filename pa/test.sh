#mvn clean && mvn compile -DskipTests && mvn package -DskipTests && mvn -X test -Dtest=com.adatao.pa.spark.execution.RegressionSuite
mvn clean && mvn compile -DskipTests && mvn package -DskipTests && mvn -X test -Dtest=SqlExecutionSuite > /tmp/a
