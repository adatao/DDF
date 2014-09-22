# This script assume ddf-opensource and ddf-enterprise is in a same folder
# this is for local mode
export DDF_ENTERPRISE_HOME="$(cd `dirname ${BASH_SOURCE[0]}`/../ >/dev/null 2>&1; echo $PWD)"
echo $DDF_ENTERPRISE_HOME
export DDF_OPENSOURCE_HOME=${DDF_ENTERPRISE_HOME}/../ddf-opensource
echo $DDF_OPENSOURCE_HOME

echo "***********BUILDING DDF-OPENSOURCE**********"
cd $DDF_OPENSOURCE_HOME
bin/sbt clean compile package; ${DDF_OPENSOURCE_HOME}/bin/make-poms.sh; mvn install -DskipTests

echo "***********BUILDING DDF-ENTERPRISE**********"
cd $DDF_ENTERPRISE_HOME
bin/sbt clean compile package; bin/make-pom.sh; mvn install -DskipTests
