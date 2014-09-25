# This script assume ddf-opensource and ddf-enterprise is in a same folder

export DDF_ENTERPRISE_HOME="$(cd `dirname ${BASH_SOURCE[0]}`/../ >/dev/null 2>&1; echo $PWD)"
echo DDF_ENTERPRISE_HOME=$DDF_ENTERPRISE_HOME
export DDF_OPENSOURCE_HOME=${DDF_ENTERPRISE_HOME}/../ddf-opensource
echo DDF_OPRNSOURCE_HOME=$DDF_OPENSOURCE_HOME

echo "building ddf-opensource"
cd $DDF_OPENSOURCE_HOME
bin/get-sbt.sh
spark/lib/mvn-install-jars.sh
bin/sbt clean compile package; bin/make-pom.sh; mvn install -DskipTests

cd $DDF_ENTERPRISE_HOME
bin/get-sbt.sh
pa/exe/build_and_deploy_jars.sh
