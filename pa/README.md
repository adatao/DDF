Adatao BigR.

See: https://github.com/adatao/BigR/wiki

IMPORTANT: First, install the unmanaged jars under lib/ into your ~/.m2 repository.

    % lib/mvn-install-jars.sh

To build:

    % mvn compile

After this you can also use `sbt` to compile or test. You may wonder why we want `sbt` in
addition to `mvn`. The short answer is that `sbt` has some useful capabilities, such as
incremental compile/test.

Basic mvn usage:

    % mvn compile
    % mvn compile -DskipTests
    % mvn test
    % mvn test -Dtest=TestLinearRegression
    % mvn test -Dtest=TestLinearRegression#testSingleVar


Installation:

    # Install .jars locally to ~/deploy/rserver/
    % mvn install -DskipTests

    # Deploy .jars to remote server (default root@canary.adatao.com)
    # This will rsync to root@canary.adatao.com:/root/deploy/rserver/
    % bin/deploy-server.sh
    % DST_USER=root DST_HOST=canary.adatao.com bin/deploy-server.sh
