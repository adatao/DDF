[33mcommit 8068207f6ce69fddb32ceefc2b52cb72784a641c[m
Merge: b750d98 1d09f2e
Author: huandao0812 <huan@adatau.com>
Date:   Mon Dec 22 10:18:57 2014 -0800

    Merge branch 'fix-kmeans-pred' of https://bitbucket.org/adatao/ddf into fix-kmeans-pred

[33mcommit b750d9801ed14065cf4130021341f956ff6c1104[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Dec 22 10:17:29 2014 -0800

    NRow throw exception if error occurs

[33mcommit 1d09f2e3f3aa535967c4dc233a618882484557f8[m
Author: adatao <khangich@gmail.com>
Date:   Mon Dec 22 18:08:07 2014 +0000

    dont ignore KmeansSuite

[33mcommit 73dcc43858c51e7210219b9f9a89da6a1c5174a4[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Dec 22 10:04:07 2014 -0800

    fix bug XsYPred

[33mcommit adade7696a194561fb0eb668e5268483894c5880[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Dec 22 10:00:19 2014 -0800

    fix kmeans prediction

[33mcommit 23c218fd30c750b2501652924a084ca5100c8334[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Dec 18 14:08:14 2014 -0800

    add IHandePersistence to ddf.ini

[33mcommit 57c6308062a1b8bd28e2302180dfc4da69a872b6[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Dec 16 15:54:04 2014 -0800

    fix jets3t jar hell

[33mcommit 2a2ab6b3f79a8c4dde0cd9c52f65480c31a11504[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Dec 16 15:40:17 2014 -0800

    remove jets3t 0.9.0

[33mcommit b7be32a5ec9e82836d7935febb7295c1421a1026[m
Merge: 5851319 0bce25f
Author: adatao <khangich@gmail.com>
Date:   Fri Dec 12 02:21:03 2014 +0000

    Merge branch 'jaccard_sim' of https://bitbucket.org/adatao/ddf into jaccard_sim
    
    Conflicts:
    	pa/src/test/scala/com/adatao/pa/spark/execution/GraphSuite.scala

[33mcommit 58513196a8888684e6c0cf40f09918a9715b0345[m
Author: adatao <khangich@gmail.com>
Date:   Fri Dec 12 02:19:06 2014 +0000

    fix test

[33mcommit 0bce25f153ddd7d3d8960687eb2bd5ac805fac06[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Dec 11 18:18:53 2014 -0800

    add param to filter duplicate in JaccardSimilarity

[33mcommit db963ca90354ea4c4b7cf434d837a4a5f092a280[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Dec 11 17:08:53 2014 -0800

    filter null row

[33mcommit bdaed517a1aa3ef7e5fe8149f27cf666b76b888f[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Dec 11 16:20:36 2014 -0800

    add test for JC Similarity

[33mcommit ef52d838696d521ea2be2657a0d4141635b70581[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Dec 11 16:15:31 2014 -0800

    add JC similarity

[33mcommit dc3afe8b8ebed09a0362eb47f2e06329394e093f[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Dec 10 22:54:32 2014 -0800

    add params to filterDuplicate

[33mcommit c58eee4a24f4a46385d4eda8cfa8d24945baab4d[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Dec 10 22:07:55 2014 -0800

    get the matrix directlt from RDD[Row]

[33mcommit f737ee399cd62e2583ffc51a748667c462eb52e4[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Dec 10 19:07:03 2014 -0800

    add method symmetricDifference2DDFs

[33mcommit 304d7cb90e1542524f680b972f6a76ef395788fa[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Dec 10 17:34:07 2014 -0800

    small bug

[33mcommit cece3dd864f8b0e82780fbd0620e17305cfb8052[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Dec 10 17:25:47 2014 -0800

    improve cosineSim

[33mcommit 26e1a6e43fbb4f9b318fafe6f72e7a77e506882a[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Dec 10 17:16:37 2014 -0800

    add method to get Graph from RDD Row

[33mcommit 1c058b72e31d8ac569e24b4d412cde26de20f772[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Dec 10 16:58:37 2014 -0800

    git add method cosineSim for 2 distMatrix

[33mcommit 6a744e6cf1834999d48994e3aafdbd9c6aaf2f19[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Dec 10 16:44:21 2014 -0800

    add log message, add method to calculate symmetricDifference between ddfs

[33mcommit d6bc56fd41b9a25470b0cb1ccc473a501433ef32[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Dec 10 15:57:15 2014 -0800

    add log message, add method to get bloomFilter from DDF

[33mcommit 8852d2367495cab91327623a1c6c7e4a2719cd87[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Dec 10 15:19:20 2014 -0800

    add log message

[33mcommit 4e1778b63d5ed0ddcb29d55f1c6625d4ba398c11[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Dec 10 13:07:15 2014 -0800

    check for switched ordering of the result

[33mcommit ae86e2afab8c10494119682de5acba759cda8188[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Dec 10 12:02:18 2014 -0800

    cleaning up CosineSimilarity

[33mcommit 4d1cf75fd49fdc2c5848a082cb5f36479704de26[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Dec 10 11:08:55 2014 -0800

    clean up test

[33mcommit 5c4281da8f5483eae33da58167657b5ea41392d5[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Dec 10 11:03:52 2014 -0800

    fix test

[33mcommit e5503e7bbc9f17608b1ed30f77b9b49f1b8a122f[m
Author: adatao <khangich@gmail.com>
Date:   Wed Dec 10 19:00:22 2014 +0000

    fix graph3.csv

[33mcommit 7d7dd01f394edda871d1b44e46ddb26f2236ec44[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Dec 10 10:56:34 2014 -0800

    add assertion

[33mcommit d60198279e5a78dded08748ed41662f09253e065[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Dec 10 10:45:42 2014 -0800

    debuggign

[33mcommit 5ab5717a89ccdc059b847b5462bf402baa7442dc[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Dec 9 22:51:12 2014 -0800

    debugging

[33mcommit 8a9735e33c62997b528edb6b229419d0df9517ab[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Dec 9 22:48:02 2014 -0800

    debugging

[33mcommit f48e9204567983545c68f53485520b60f7fcad34[m
Merge: 65bfaea 11834a8
Author: adatao <khangich@gmail.com>
Date:   Wed Dec 10 06:30:32 2014 +0000

    Merge branch 'improve-Rserve' of https://bitbucket.org/adatao/ddf into improve-Rserve
    
    Conflicts:
    	pa/src/main/scala/com/adatao/pa/spark/execution/CosineSimilarity.scala

[33mcommit 65bfaea6cebcf97de66b36e6021284731a58f93d[m
Author: adatao <khangich@gmail.com>
Date:   Wed Dec 10 06:29:42 2014 +0000

    debugging

[33mcommit 11834a87232bea9b35d5418a5dc1849eea47edf8[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Dec 9 22:29:12 2014 -0800

    fix bug in CosineSimilarity

[33mcommit ed8a67ecba6850f4ad3e902a14649816f003b42e[m
Merge: e28966e 0198c06
Author: adatao <khangich@gmail.com>
Date:   Wed Dec 10 06:14:20 2014 +0000

    Merge branch 'improve-Rserve' of https://bitbucket.org/adatao/ddf into improve-Rserve

[33mcommit e28966e307939196d30e3af7b69b928b13925c17[m
Author: adatao <khangich@gmail.com>
Date:   Wed Dec 10 06:14:13 2014 +0000

    fix graph3.csv

[33mcommit 0198c06d2d243384466d7788f223f2c2fd5f7387[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Dec 9 22:13:49 2014 -0800

    debugging

[33mcommit ceeb5f74a859e9ab563338ced3958bc52c46abbf[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Dec 9 22:02:54 2014 -0800

    debugging

[33mcommit 5e7d372153d17b7bbcd38ab0c51ec13261468a2f[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Dec 9 21:21:36 2014 -0800

    debugging

[33mcommit bb50206ee38dfe8e90b5f2b10c3eb3b27bfef251[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Dec 9 21:18:49 2014 -0800

    debugging

[33mcommit 7c27529d8b9a187cd52e081609e1587212e5287c[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Dec 9 21:15:53 2014 -0800

    debugging

[33mcommit bf8dc97f12829ec7306a3a51b19f051115132119[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Dec 9 21:04:17 2014 -0800

    print test result

[33mcommit e0e1f4ad958e3b24a7de222c59e0c265e42f7fb0[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Dec 9 19:22:58 2014 -0800

    print out test result

[33mcommit 030576e0ac2c32d761c467a1f3348a92e7a12ae6[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Dec 9 19:17:06 2014 -0800

    debugging

[33mcommit fe7738e6daeeab1d33f7467a02db1cf0f75ca26e[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Dec 9 19:15:54 2014 -0800

    debugging

[33mcommit 4542338c9edf7e2f65fb878c6d19b19b67ef3b7d[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Dec 9 19:13:42 2014 -0800

    [debugging] similarity

[33mcommit 44a6ca4e9ec47d6551458ba9e9beb7ae4fe47654[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Dec 9 19:10:44 2014 -0800

    use bloomFilter to find the diff between 2 graphs

[33mcommit d129164c79588ef1dad3474eee4c5b5fb5d8705e[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Dec 9 18:15:41 2014 -0800

    debuggign

[33mcommit a3acd10a8399af7ed92bdcc62976ee520b1808a4[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Dec 9 18:09:11 2014 -0800

    add log bloomFilter

[33mcommit bd21d12914b9c238f063953425c4161e6a94f12d[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Dec 9 18:03:06 2014 -0800

    debugging

[33mcommit 1ab36845f094a95a0e383061b24d4150a0131401[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Dec 9 17:56:23 2014 -0800

    add assert

[33mcommit da6c2973c6c78d5bc6a8ec87ce0025cae436f81f[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Dec 9 17:53:14 2014 -0800

    don't change Graph persistence level

[33mcommit edf75e157183a196c81dd09671535f3dc7576d27[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Dec 9 17:50:56 2014 -0800

    fix NPE

[33mcommit 30d19f7441364a5d0dcd38b5513659791c160389[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Dec 9 17:48:40 2014 -0800

    graph test

[33mcommit 2450626c520eb47f2921d5634b8aa43e78679eb5[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Dec 9 17:43:57 2014 -0800

    add test

[33mcommit 7b5206b163efad059f850f97ba03917393110bd7[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Dec 9 17:25:46 2014 -0800

    change dataset graph3.csv

[33mcommit 18262059ca358dbd978e75b9c771219c3376350b[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Dec 9 17:17:05 2014 -0800

    chang dataset graph3

[33mcommit 91356ff03415a80ae9eded82b3ef7735b20fee2d[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Dec 9 16:57:01 2014 -0800

    fix test

[33mcommit 5f56031db8a78da70ffc71bfbd00a0bd7456ef2e[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Dec 9 16:46:32 2014 -0800

    add test for Cosine Similarity

[33mcommit 20ba6b3858bc3fc22e15f876de46ad62dedf41a4[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Dec 9 16:22:30 2014 -0800

    fix compile error

[33mcommit a3a896115abd780ec12d984de427fcb2489c1993[m
Merge: eca2c2f 04c0c06
Author: huandao0812 <huan@adatau.com>
Date:   Tue Dec 9 16:21:05 2014 -0800

    Merge branch 'improve-Rserve' of https://bitbucket.org/adatao/ddf into improve-Rserve

[33mcommit eca2c2f85eddcc1c43047dfc1b3383dbb8a5421a[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Dec 9 16:20:55 2014 -0800

    fix compile error

[33mcommit 04c0c061aeaba64f53eee94cad6515a753a6bb3a[m
Merge: a08f2ee 3ceeb25
Author: adatao <khangich@gmail.com>
Date:   Wed Dec 10 00:18:46 2014 +0000

    Merge branch 'improve-Rserve' of https://bitbucket.org/adatao/ddf into improve-Rserve

[33mcommit 3ceeb2583070ec9cb6d8df72e961e19dfe69fe7f[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Dec 9 16:18:28 2014 -0800

    fix compile error

[33mcommit a08f2ee3d512d7283513003fb490480c04625df2[m
Merge: 16c898f cfa39e7
Author: adatao <khangich@gmail.com>
Date:   Wed Dec 10 00:16:19 2014 +0000

    Merge branch 'improve-Rserve' of https://bitbucket.org/adatao/ddf into improve-Rserve

[33mcommit 16c898f8094886fde3ad62b3bf05a1692a126dad[m
Author: adatao <khangich@gmail.com>
Date:   Wed Dec 10 00:16:12 2014 +0000

    dont persist PartitionedGraph

[33mcommit cfa39e76d3b5ce5c32ad400ff3f35bc6e64d043f[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Dec 9 16:15:26 2014 -0800

    add CosineSimilarity

[33mcommit d3194ff8efff611ae4bc57723b4d7778f487a239[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Dec 8 16:45:50 2014 -0800

    persist graph with MEMORY_AND_DISK leve'

[33mcommit a82b296dccccd4582235a9dc27f7deb383d4ded6[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Dec 8 16:31:42 2014 -0800

    store graph as a Graph[String, Double]

[33mcommit a3f210ac9dfe5ec49acc533175cee43961d3d768[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Dec 8 16:05:27 2014 -0800

    remove mistakenly committed file

[33mcommit 7228d0de77282f8d387a01a4a16fc8022551b9f9[m
Merge: 1fa6c13 3259f5f
Author: huandao0812 <huan@adatau.com>
Date:   Mon Dec 8 15:47:16 2014 -0800

    Merge branch 'improve-Rserve' of https://bitbucket.org/adatao/ddf into improve-Rserve

[33mcommit 1fa6c13aadbe720d4121091e4611c0d65404d197[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Dec 8 15:46:22 2014 -0800

    add graph to repHandler

[33mcommit 3259f5f73fc60c5d8a4235e537c46ea3095e1838[m
Author: Pham Nam Long <longpham@adatao.com>
Date:   Mon Dec 8 18:59:46 2014 +0000

    chage ddf-version to 1.1-adatao[

[33mcommit 377deee899b7bf53ed6b399c5b02a7edcdffe26a[m
Author: huandao0812 <huan@adatau.com>
Date:   Sat Dec 6 02:33:22 2014 -0800

    fix assert

[33mcommit db641a3672a2a02e74cf49b7eef0d09e22c8f527[m
Author: huandao0812 <huan@adatau.com>
Date:   Sat Dec 6 02:30:46 2014 -0800

    change assert to assertEquals

[33mcommit e7c7b93e50eb893c87101cd867677a23af700625[m
Author: huandao0812 <huan@adatau.com>
Date:   Sat Dec 6 02:14:41 2014 -0800

    change numFeatures -> numColumns
    
    correct numFeatures = numColumns - 1

[33mcommit d12a1cdc87f36cb1a129a0c69c04ea15962e3c9a[m
Author: huandao0812 <huan@adatau.com>
Date:   Sat Dec 6 01:35:55 2014 -0800

    numFeatures=2 in RegressionSuite

[33mcommit 8462c103f333ee2b25e5e3e42d1613ef507dff38[m
Author: huandao0812 <huan@adatau.com>
Date:   Sat Dec 6 01:29:33 2014 -0800

    fix numFeatures

[33mcommit dd335f398c3d6d42c236235450ee94b6247c7efe[m
Author: huandao0812 <huan@adatau.com>
Date:   Sat Dec 6 01:25:35 2014 -0800

    delete old test in GraphSuite

[33mcommit 0a313672b917cbafcb69ecaa61ca558231ce7a96[m
Merge: 54bfad0 3c0f261
Author: huandao0812 <huan@adatau.com>
Date:   Sat Dec 6 01:20:15 2014 -0800

    Merge branch 'improve-Rserve' of https://bitbucket.org/adatao/ddf into improve-Rserve
    
    Conflicts:
    	pa/src/test/scala/com/adatao/pa/spark/execution/RegressionSuite.scala

[33mcommit 54bfad08f89df3838677b5d30e34354bb50d5598[m
Author: huandao0812 <huan@adatau.com>
Date:   Sat Dec 6 01:17:58 2014 -0800

    fix numFeatures for LinearRegression

[33mcommit d37a965cf3701ca205d5bbf97b15aef1bc6c1386[m
Author: huandao0812 <huan@adatau.com>
Date:   Sat Dec 6 01:17:04 2014 -0800

    fix numFeatures for LinearRegressionNQ

[33mcommit f8ec2bf9c57335527212bcd91d590402e385d496[m
Author: huandao0812 <huan@adatau.com>
Date:   Sat Dec 6 01:11:35 2014 -0800

    remove schema file

[33mcommit 558c559a2f82c9d6b4eb892c1e0841a1f24b1530[m
Author: huandao0812 <huan@adatau.com>
Date:   Sat Dec 6 01:11:01 2014 -0800

    remove *.sch file

[33mcommit 3c0f261e7d83ae8a9b44bfdef32671433991c209[m
Author: adatao <khangich@gmail.com>
Date:   Sat Dec 6 07:46:38 2014 +0000

    fix RegressionSuite

[33mcommit 6529329300c50245a1193ff702cc2a63656b4404[m
Merge: 3c99102 ca56356
Author: huandao0812 <huan@adatau.com>
Date:   Fri Dec 5 23:14:00 2014 -0800

    Merge branch 'master' into improve-Rserve
    
    Conflicts:
    	pa/src/test/scala/com/adatao/pa/spark/execution/RegressionSuite.scala

[33mcommit 3c99102d2588101b492e586f8e6a84ea80db8551[m
Author: adatao <khangich@gmail.com>
Date:   Sat Dec 6 04:18:56 2014 +0000

    un-ignore test in MapReduceNaiveSuite

[33mcommit 3a63c00de625fbde79ddb66b866b4c3239d6e9cc[m
Author: adatao <khangich@gmail.com>
Date:   Sat Dec 6 03:05:24 2014 +0000

    un-ignore test

[33mcommit 53608e17094a076890ed2cdd93c3a5937f168cd0[m
Author: adatao <khangich@gmail.com>
Date:   Sat Dec 6 02:54:12 2014 +0000

    ignore sparse glm test

[33mcommit 40b7b0aaa239d0d585207be196e7be4ec03698db[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Dec 5 18:51:03 2014 -0800

    fix CachedBatch2REXP

[33mcommit 8015f3bb1621d2cb9f96287f385f171db8bad798[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Dec 5 18:37:07 2014 -0800

    add assertion for check validity of TransformDummy

[33mcommit 6bb2164847b916eb3e6360aad50aa544d08378aa[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Dec 5 18:33:08 2014 -0800

    add assertion in TransformSuite

[33mcommit ecbedccecb204cfb2acda8828ff8d6a718fe12c4[m
Author: adatao <khangich@gmail.com>
Date:   Sat Dec 6 02:04:32 2014 +0000

    fix SparkSQL columnar extraction to matrix

[33mcommit eec87f623e5899b77a854dcedbca671f7495c503[m
Merge: 8e2162b d55fcf0
Author: adatao <khangich@gmail.com>
Date:   Sat Dec 6 01:00:22 2014 +0000

    Merge branch 'improve-Rserve' of https://bitbucket.org/adatao/ddf into improve-Rserve
    
    Conflicts:
    	spark_adatao/src/test/scala/com/adatao/spark/ddf/analytics/TransformSuite.scala

[33mcommit 8e2162b35c2908169dcd7e4406ec8b67624ab08e[m
Author: adatao <khangich@gmail.com>
Date:   Sat Dec 6 00:59:51 2014 +0000

    print out result of TransformDummy

[33mcommit d55fcf0531f8bc0e0219359a8b7d693dc9763356[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Dec 5 16:59:17 2014 -0800

    add test for TransformDummy

[33mcommit 3f68c6b2ea7e5b3e256d8f380cbad362703b3125[m
Author: adatao <khangich@gmail.com>
Date:   Sat Dec 6 00:35:40 2014 +0000

    fix createTableTransform sql

[33mcommit a4bf5787e32c6f82ff44db0da9f12537714dd3dc[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Dec 5 16:19:09 2014 -0800

    add test for TransformDummy

[33mcommit d076e4060e79fa8aac97c66732d495a2e9ee84ca[m
Merge: d71e07d ed47e92
Author: huandao0812 <huan@adatau.com>
Date:   Fri Dec 5 15:08:02 2014 -0800

    Merge branch 'spark-1.3.0' into improve-Rserve

[33mcommit ed47e92a7297c7ac1075c2f6ebc215cee4676ebf[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Dec 5 14:23:39 2014 -0800

    add assertion to GraphSuite

[33mcommit 156abc25e782c4d41bd3f935dc87c254e5fe5a96[m
Author: adatao <khangich@gmail.com>
Date:   Fri Dec 5 22:17:39 2014 +0000

    fix bug in get denom_tfidf in GRaphTFIDF

[33mcommit 5f28172db76e9d3e8fff3e5982ac77e5e795ec09[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Dec 5 14:09:21 2014 -0800

    delete header in graph2.csv

[33mcommit a8c810d3fa58022042c264d185717bd78838a209[m
Merge: 1579dfe 2365120
Author: adatao <khangich@gmail.com>
Date:   Fri Dec 5 22:02:06 2014 +0000

    Merge branch 'spark-1.3.0' of https://bitbucket.org/adatao/ddf into spark-1.3.0

[33mcommit 236512020cc8353ab48519b9fa5047b8aec6efb7[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Dec 5 14:01:55 2014 -0800

    fix pattern matching in GraphTFIDF

[33mcommit 1579dfeefe427fbd2bc51f7c7672032663b6a002[m
Merge: a5f8c4d 1f39d63
Author: adatao <khangich@gmail.com>
Date:   Fri Dec 5 21:54:46 2014 +0000

    Merge branch 'spark-1.3.0' of https://bitbucket.org/adatao/ddf into spark-1.3.0

[33mcommit d71e07d5c09977bab8c8a1f18828131d8a6565dc[m
Author: adatao <khangich@gmail.com>
Date:   Fri Dec 5 21:54:20 2014 +0000

    fix testSuite, start server in beforeAll, fix transformDummy getNRows

[33mcommit 1f39d63ce4458f73fb6445b3cb887f42a7fb841f[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Dec 5 13:50:47 2014 -0800

    fix graphTFIDF formular

[33mcommit 1fcc4e990c99b598e3380308f3ca1620d8eb4fad[m
Merge: 0834788 380fc76
Author: adatao <khangich@gmail.com>
Date:   Thu Dec 4 22:04:09 2014 +0000

    Merge branch 'improve-Rserve' of https://bitbucket.org/adatao/ddf into improve-Rserve

[33mcommit 380fc7671264e8a744cfce6cab8057c3b5eb02b7[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Dec 4 14:03:59 2014 -0800

    debugging TransformDummy

[33mcommit 08347880ae6113637b7a1ff00a9ea3523b8424e8[m
Merge: a276390 a358959
Author: adatao <khangich@gmail.com>
Date:   Thu Dec 4 22:02:00 2014 +0000

    Merge branch 'improve-Rserve' of https://bitbucket.org/adatao/ddf into improve-Rserve

[33mcommit a358959ced3f49e5f2b8932de962ba0e183aceb9[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Dec 4 14:01:46 2014 -0800

    debugging TransformDummy

[33mcommit a2763906032551b50ae0fc81fc9f0ed0dd1cf9b3[m
Merge: 5ada9f9 f2f88be
Author: adatao <khangich@gmail.com>
Date:   Thu Dec 4 22:01:15 2014 +0000

    Merge branch 'improve-Rserve' of https://bitbucket.org/adatao/ddf into improve-Rserve

[33mcommit f2f88be74921581e9d6f4c40e7c14e3ad99af049[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Dec 4 14:01:02 2014 -0800

    debugging TransformDummy

[33mcommit 5ada9f9c29b0049e6c672892dab015b82f17a02c[m
Merge: bdf63ad eb18b8a
Author: adatao <khangich@gmail.com>
Date:   Thu Dec 4 22:00:26 2014 +0000

    Merge branch 'improve-Rserve' of https://bitbucket.org/adatao/ddf into improve-Rserve

[33mcommit eb18b8ae91e7bee7da93413fb68774dabe5edc87[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Dec 4 13:59:17 2014 -0800

    debugging

[33mcommit 03e792f58e8bcd1968c78c83dd4c09c9b3911f5a[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Dec 4 12:35:03 2014 -0800

    change idf calculation

[33mcommit bdf63ad989ce54e902a7e4c0ee1a0250c2202287[m
Merge: 2859088 0c4e740
Author: adatao <khangich@gmail.com>
Date:   Thu Dec 4 08:49:50 2014 +0000

    Merge branch 'improve-Rserve' of https://bitbucket.org/adatao/ddf into improve-Rserve

[33mcommit 2859088742cf026e3bf0ae1575775148e479380e[m
Author: adatao <khangich@gmail.com>
Date:   Thu Dec 4 08:49:39 2014 +0000

    testing

[33mcommit 0c4e74083c90f61d12a994983e8e5d817b2568cd[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Dec 4 00:48:31 2014 -0800

    [debugging]

[33mcommit dd019b2a6b550d5de2ab98640d13a1d52ae2acb3[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Dec 4 00:32:02 2014 -0800

    [debugging]

[33mcommit ca56356bbe8eb0edef8e1be51ee28be8eaacf3b4[m
Author: khangich <khangich@gmail.com>
Date:   Thu Dec 4 13:21:11 2014 +0700

    ignore failed in MapReduceNative, fix MetricsSuite

[33mcommit df4c6f57886e2d86a677b31a7eb2d9065fd419a9[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Dec 3 22:10:06 2014 -0800

    debugging

[33mcommit de4322388573674e97beb2d5bc2b01ddd71fc686[m
Author: khangich <khangich@gmail.com>
Date:   Thu Dec 4 13:04:42 2014 +0700

    rename table carowner

[33mcommit 8ae07a33b656790ab57553e40289dbb0109c2658[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Dec 3 21:23:37 2014 -0800

    add log message for TransformDummy

[33mcommit e86efda498c98e1f01881ea3e86bcd0d4d30f90a[m
Merge: 61be46b e44db4e
Author: nhanitvn <nhanitvn@gmail.com>
Date:   Thu Dec 4 11:56:53 2014 +0700

    Merged in fix-lm.result (pull request #166)
    
    results now are identical with R

[33mcommit e44db4eee669dc80c3da9781fe89dcdf09f12643[m
Author: khangich <khangich@gmail.com>
Date:   Thu Dec 4 11:53:14 2014 +0700

    results now are identical with R

[33mcommit bb2a6bf5399506ec05c64d12bc8c1bbc41239a22[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Dec 3 18:53:48 2014 -0800

    use try catch to check for empty buffer

[33mcommit 3070f451683bba74b17fc068b228acf26780c583[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Dec 3 18:31:36 2014 -0800

    add log for nullCounts

[33mcommit b45df7d128421074fb93e6bd46266f4c921ddba6[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Dec 3 18:29:33 2014 -0800

    check columnAccessor.hasNext()

[33mcommit e8290ecdaab5e3618db2ccac62ce29b212c5d844[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Dec 3 16:57:03 2014 -0800

    add log message to RDDCachedBatch2REXP.scala

[33mcommit 4636485dc8f3b0e09b737a06a8ce39ddedf969b4[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Dec 3 16:36:42 2014 -0800

    use CachedBatch columns statistics to get number of rows per Partition

[33mcommit 7fdaf5a1c3e291b7af1c1bbd245c19eb778d4318[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Dec 2 23:43:42 2014 -0800

    un-ignore tests in CreateSharkDataFrameSuite

[33mcommit c15297906dabfe70f8651bd988cb8680d5242fcd[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Dec 2 23:24:38 2014 -0800

    fix bug in RDDCachedBatch2REXP.scala

[33mcommit e23408d87a37a5a4cdea8ad470c46d926fb9ff15[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Dec 2 22:48:50 2014 -0800

    debugging

[33mcommit 73fcb5b6e01dde5a8c99c3ff30fff634afda9ef4[m
Merge: 55f9c87 77a31e0
Author: adatao <khangich@gmail.com>
Date:   Wed Dec 3 06:47:19 2014 +0000

    Merge branch 'improve-Rserve' of https://bitbucket.org/adatao/ddf into improve-Rserve
    
    Conflicts:
    	pa/src/test/scala/com/adatao/pa/spark/CreateSharkDataFrameSuite.scala

[33mcommit 55f9c87d012b0acc8095d2e56485e0c61a2a3e15[m
Author: adatao <khangich@gmail.com>
Date:   Wed Dec 3 06:38:18 2014 +0000

    debugging RServe

[33mcommit 143a5b1e86246f5d5425992e3d0284ad3421c90b[m
Author: adatao <khangich@gmail.com>
Date:   Wed Dec 3 06:32:49 2014 +0000

    add new repHandler in ddf.ini

[33mcommit 77a31e0fff0448a95d890bebe651fa7d16cc4891[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Dec 2 22:31:06 2014 -0800

    move new RepHandler to correct package

[33mcommit 596a9e59f8a17a9f67c7b241f2055286b0aa9ccd[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Dec 2 19:54:34 2014 -0800

    add method to convert from RDD[CachedBatch] to RDD[REXP]

[33mcommit c89027798de9842d0c81baf4fd812f0148b24ac3[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Nov 28 22:12:15 2014 -0800

    add dummy variable for ListPersistedDDFs

[33mcommit 380d75747dcd40f3a276e9e5609e75767a48f8b8[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Nov 28 17:15:28 2014 -0800

    return array of string for ListPersistedDDF

[33mcommit db8a10303148f4bbdf2de41cc692ca443794f94f[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Nov 28 16:51:28 2014 -0800

    fix ListPersistedDDF uri

[33mcommit 106ff5275426d0182456e9eb9b3f54082e7bcc8d[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Nov 28 16:47:38 2014 -0800

    add method to list persisted DDFs

[33mcommit 61be46bf5384bef47b1b722de828c85750c13faf[m
Merge: a96c781 ce18040
Author: khangpham <khangpham@adatau.com>
Date:   Thu Nov 27 08:27:44 2014 +0700

    Merged in ddf-sparksql-1.1.0 (pull request #164)
    
    move PA to SparkSQL

[33mcommit b5d9d1e78579e8f87368b06e8e71c50f226b6ace[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Nov 26 15:57:26 2014 -0800

    pa.spark.Utils.generateMetaInfo: set empty factor if column is factor but don't found factor levels in column

[33mcommit b9bfc104ce2c2d0c69d1eebcc36d06cde80c251d[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Nov 26 12:39:14 2014 -0800

    add executor for persist and load ddf

[33mcommit ce180408d9c28e3c9d75197aaa46d85f7c53d9a1[m
Author: khangich <khangich@gmail.com>
Date:   Wed Nov 26 08:10:42 2014 +0700

    remove unnecessary copy file in smaster

[33mcommit a5f8c4d302cc539e775ac0f54ddd261e62077d80[m
Merge: b0ffa35 3650a97
Author: adatao <khangich@gmail.com>
Date:   Tue Nov 25 06:20:59 2014 +0000

    Merge branch 'spark-1.3.0' of https://bitbucket.org/adatao/ddf into spark-1.3.0

[33mcommit 3650a9790faa00b235ee6e0a4c71cfa82449fbfb[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Nov 24 22:20:41 2014 -0800

    fix colname to tfidf

[33mcommit b60f859a4b8236f41b701201c8cd348ad030e373[m
Merge: 29127bc ff7fb70
Author: huandao0812 <huan@adatau.com>
Date:   Mon Nov 24 20:57:09 2014 -0800

    Merge branch 'ddf-sparksql-1.1.0' into spark-1.3.0

[33mcommit ff7fb70a80e435b97bcded5967b06919b5a3961c[m
Merge: 9d322d3 3432d9f
Author: huandao0812 <huan@adatau.com>
Date:   Mon Nov 24 20:56:35 2014 -0800

    Merge branch 'ddf-sparksql-1.1.0' of https://bitbucket.org/adatao/ddf into ddf-sparksql-1.1.0

[33mcommit 29127bc8530b096b90eac0e9414d9d63041463f0[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Nov 24 18:18:07 2014 -0800

    catch and throw exception if table already exists in hive

[33mcommit b0ffa35a55288d32d51308494b0292b73883bb37[m
Merge: 4d65690 1abf9a1
Author: adatao <khangich@gmail.com>
Date:   Mon Nov 24 23:56:45 2014 +0000

    Merge branch 'spark-1.3.0' of https://bitbucket.org/adatao/ddf into spark-1.3.0

[33mcommit 1abf9a14c481a1578ef085ef8b028a5a49c19010[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Nov 24 15:56:28 2014 -0800

    bug in persistDDF

[33mcommit 4d6569004e2745098cc643aaddd0aa828ce4a079[m
Merge: 3836736 50bfd2c
Author: adatao <khangich@gmail.com>
Date:   Mon Nov 24 23:46:10 2014 +0000

    Merge branch 'spark-1.3.0' of https://bitbucket.org/adatao/ddf into spark-1.3.0

[33mcommit 50bfd2c2cc10185f0f8fe77a23adf507d3af571a[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Nov 24 15:45:47 2014 -0800

    use insertInto to persist DDF

[33mcommit 3836736c841bed8816e13a41a8720c811edc20f9[m
Merge: 32aed87 5819979
Author: adatao <khangich@gmail.com>
Date:   Mon Nov 24 22:50:41 2014 +0000

    Merge branch 'spark-1.3.0' of https://bitbucket.org/adatao/ddf into spark-1.3.0

[33mcommit 581997908cd0c9f004316315f6236afc6991828a[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Nov 24 14:50:26 2014 -0800

    persistDDF

[33mcommit 32aed87bb6a762330a62d84a35b8d0c418ade842[m
Merge: ab29250 9741ca8
Author: adatao <khangich@gmail.com>
Date:   Mon Nov 24 21:48:12 2014 +0000

    Merge branch 'spark-1.3.0' of https://bitbucket.org/adatao/ddf into spark-1.3.0

[33mcommit 9741ca80b4cb68d98e47971fab993e945903d141[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Nov 24 12:03:59 2014 -0800

    add executor to persist ddf

[33mcommit 9d322d395c1a8991d6bd0a548c5a23ebed6db74d[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Nov 24 10:05:40 2014 -0800

    add maxBin to executor

[33mcommit ab292504e06a4215ec0367f12b113b9975ea9ddb[m
Merge: 91b7e77 3432d9f
Author: adatao <khangich@gmail.com>
Date:   Mon Nov 24 18:02:41 2014 +0000

    Merge branch 'ddf-sparksql-1.1.0' into spark-1.3.0

[33mcommit 3432d9fffc5f97aa4618c5882971a6e024e4bec0[m
Author: adatao <khangich@gmail.com>
Date:   Mon Nov 24 18:00:56 2014 +0000

    fix pa-env for package

[33mcommit 4d8cce17d74f2936e0436030d78191c09fe39898[m
Author: khangich <khangich@gmail.com>
Date:   Mon Nov 24 22:53:31 2014 +0700

    change lib_managed to libs

[33mcommit daff25cf0b3a8bbf37b4afb9d8d33056c92e9327[m
Author: khangich <khangich@gmail.com>
Date:   Mon Nov 24 17:49:46 2014 +0700

    add compute class path

[33mcommit 91b7e778afdd7cdd5577e37ed988f4ef288a4b92[m
Author: adatao <khangich@gmail.com>
Date:   Mon Nov 24 08:45:14 2014 +0000

    with spark 1.3.0

[33mcommit 0e8e336e429a4fd2b87738b9db2bf70c8ae9db69[m
Author: khangich <khangich@gmail.com>
Date:   Mon Nov 24 13:45:42 2014 +0700

    serialize tree to json

[33mcommit 342299dc88d343917e8d2a8fdffae86ade309750[m
Merge: 8635a8b 2252258
Author: adatao <khangich@gmail.com>
Date:   Sun Nov 23 01:29:37 2014 +0000

    Merge branch 'ddf-sparksql-1.1.0' of https://bitbucket.org/adatao/ddf into ddf-sparksql-1.1.0

[33mcommit 8635a8b064c5206108c7a5b961f97e7eceed6681[m
Author: adatao <khangich@gmail.com>
Date:   Sun Nov 23 01:18:39 2014 +0000

    Revert "set graph storage level to memory and disk"
    
    This reverts commit d7ba062f600090b2f47efb20ffb59a08d9fce281.

[33mcommit 2252258f53c49a90b655602c4a7c24e46703b6d4[m
Merge: 7a226ff d7ba062
Author: bhan <bhan@adatao.com>
Date:   Sat Nov 22 01:33:25 2014 -0800

    Merge branch 'ddf-sparksql-1.1.0' of https://bitbucket.org/adatao/ddf into ddf-sparksql-1.1.0

[33mcommit 7a226ffe35ee276587b84c9c79974d9e66a3afad[m
Author: bhan <bhan@adatao.com>
Date:   Sat Nov 22 01:32:58 2014 -0800

    testing transformHive

[33mcommit d7ba062f600090b2f47efb20ffb59a08d9fce281[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Nov 21 17:40:32 2014 -0800

    set graph storage level to memory and disk

[33mcommit 7ed12e364aace3eda6e0e5e148100ce872ab5c03[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Nov 21 16:38:51 2014 -0800

    fix wrong filter null condition in GraphTFIDF

[33mcommit 507d158f33ea3e9da61602e9f6ebbd22e648b922[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Nov 21 16:23:29 2014 -0800

    filter out null value

[33mcommit a51b693216ebb0a88b6c5be093b5cf47ea50d609[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Nov 21 15:42:01 2014 -0800

    add log message

[33mcommit 99fb15b9e6d217720c7079d78d750d7a6dc3f362[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Nov 21 15:16:34 2014 -0800

    change default value of edge to empty String
    
    update pa-env for —yarn vs localspark

[33mcommit 7ba98f43e9064b13a527655e3df13f915548b594[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Nov 21 11:36:08 2014 -0800

    add more test

[33mcommit ed93831567acd669d03514fad7a052611dcebf38[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Nov 21 11:33:56 2014 -0800

    fix test

[33mcommit bdbedccd408e7664cc88b1692d9ea7b63b11db84[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Nov 21 11:32:23 2014 -0800

    fix GraphSuite

[33mcommit 76905c6b94841263fbda30ab8fd48a172a0ad443[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Nov 21 11:29:47 2014 -0800

    add more assertion

[33mcommit 8e2f8075753d0d8f74b2fe6fba5eeffcfd4508a6[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Nov 21 11:14:19 2014 -0800

    fix test

[33mcommit a9e2b772069b3d1f21480d840a8ab6458e4086a3[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Nov 21 11:09:51 2014 -0800

    fix split test

[33mcommit d05309bafb577242c7078793fcf6e13d512cac50[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Nov 21 10:43:22 2014 -0800

    fix test

[33mcommit 429850b02f52d94052bae0162259c8b7db8812bb[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Nov 21 10:38:24 2014 -0800

    add assert to GraphSuite

[33mcommit dedda46427aef0d71fe7b7fa3ec4d859d7ebc256[m
Author: adatao <khangich@gmail.com>
Date:   Fri Nov 21 18:29:03 2014 +0000

    fix compile error

[33mcommit 4ba0f86149f4ddd66c9c3b7439dab30966fcc73d[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Nov 21 10:19:21 2014 -0800

    add edge column to GraphTFIDF executor

[33mcommit f2863d75c214a6d9a5e4724b45cf71c9a65ed0b4[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Nov 21 01:24:29 2014 -0800

    fix GraphSuite

[33mcommit 67e6658e0467de10030bfb9445a70e3cd61f2f5f[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Nov 21 01:23:50 2014 -0800

    [TF-IDF] use string for src and dest

[33mcommit 67ef686ba9fb6040260ee9359dbc7065e5ad02ba[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Nov 20 23:52:05 2014 -0800

    add comment, fix minor bug

[33mcommit eb1a70fe9a8c6f9316e474347cf0b98ba4151e52[m
Merge: f3abdc0 e889f88
Author: khangich <khangich@gmail.com>
Date:   Fri Nov 21 14:06:23 2014 +0700

    Merge branch 'ddf-sparksql-1.1.0' of bitbucket.org:adatao/ddf into ddf-sparksql-1.1.0

[33mcommit f3abdc087f598876bb18203755cb0f4fbb59e0b5[m
Author: khangich <khangich@gmail.com>
Date:   Fri Nov 21 14:06:03 2014 +0700

    fix testMissingData  handling

[33mcommit e889f8832938cf84a7583de1bd6e8361218f7dfc[m
Merge: ba09bcb 92e5e8f
Author: huandao0812 <huan@adatau.com>
Date:   Thu Nov 20 22:53:07 2014 -0800

    Merge branch 'ddf-sparksql-1.1.0' of https://bitbucket.org/adatao/ddf into ddf-sparksql-1.1.0

[33mcommit ba09bcbefc9ab501eb1332e4f6922a83c355eefb[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Nov 20 22:53:04 2014 -0800

    graph to calculate tf-idf for HH

[33mcommit 92e5e8ff29c798c1b25dc91911f959bc9582866b[m
Author: khangich <khangich@gmail.com>
Date:   Fri Nov 21 08:43:45 2014 +0700

    remove unresolve class

[33mcommit 17829188ad0dfcf4c6ae26f3a7f10c47683ae467[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Nov 20 10:35:42 2014 -0800

    remove TFIDF & NaiveBayes

[33mcommit 6067c8999fba0350d2ab535b0e8acd22e982ae44[m
Merge: 9c0b973 a96c781
Author: huandao0812 <huan@adatau.com>
Date:   Thu Nov 20 10:23:00 2014 -0800

    Merge branch 'master' into ddf-sparksql-1.1.0
    
    Conflicts:
    	pa/conf/distribution/linux/pa-env.sh
    	pa/conf/pa-env.sh
    	pa/src/main/java/com/adatao/pa/thrift/Server.java
    	project/RootBuild.scala

[33mcommit 9c0b973b467943935d6337d6610440e716dcfa6b[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Nov 20 10:16:20 2014 -0800

    fix assembly.xml, conf/distribution/linux/pa-env.sh

[33mcommit ab229ebd1d1494c86f9371d1aad8cf6d7ced50b9[m
Merge: 82da817 fe27f1d
Author: huandao0812 <huan@adatau.com>
Date:   Wed Nov 19 17:51:53 2014 -0800

    Merge branch 'ddf-sparksql-1.1.0' of https://bitbucket.org/adatao/ddf into ddf-sparksql-1.1.0

[33mcommit 82da817bcaebc02d9c92b7ce862ec2e745c3f822[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Nov 19 17:31:10 2014 -0800

    initial commit for GraphAl

[33mcommit fe27f1df1f08a0771b1b0980d8abb68fbbc3d170[m
Author: khangich <khangich@gmail.com>
Date:   Wed Nov 19 16:17:21 2014 +0700

    fix basetest

[33mcommit bf3d9e60a28ac2a2cc7b271ac206570422388f7f[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Nov 18 22:44:29 2014 -0800

    add com.adatao.spark.ddf.SparkDDFManager to ddf.ini

[33mcommit c827bce3018c65b6fd21d0cb2486227927e760d7[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Nov 18 12:51:03 2014 -0800

    add log message

[33mcommit bc788dd09c0196bc01157e1a2306aa01786d6616[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Nov 18 12:22:44 2014 -0800

    put key value map in SparkDDFManager

[33mcommit 6134a47940c34010da298dc0081f696cba7bba86[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Nov 18 11:21:52 2014 -0800

    don't recompute factor in DecisionTree

[33mcommit 961f70978b9c6e37a8769a8289b9bb38588d23dc[m
Author: khangich <khangich@gmail.com>
Date:   Tue Nov 18 14:59:19 2014 +0700

    change permission for release.sh

[33mcommit 15e875e960c038f683e74eee76327cb3078fb440[m
Author: khangich <khangich@gmail.com>
Date:   Tue Nov 18 14:45:49 2014 +0700

    update release script

[33mcommit ceddb61e305f0ab63764c7dd526af5f6053e42f6[m
Author: khangich <khangich@gmail.com>
Date:   Tue Nov 18 14:43:45 2014 +0700

    add release script

[33mcommit 0d971c9749d7a98a7f06802658f9d070e7554021[m
Author: khangich <khangich@gmail.com>
Date:   Tue Nov 18 14:43:28 2014 +0700

    add ddf.ini to assembly

[33mcommit cb571daceb1086f7efdda5979499820a2bbbc1e0[m
Author: khangich <khangich@gmail.com>
Date:   Tue Nov 18 10:40:50 2014 +0700

    fix the connect issue in unit tests

[33mcommit 9c2e0b6b002b76dcb2649da3e1f5e1a62e5484e7[m
Author: khangich <khangich@gmail.com>
Date:   Mon Nov 17 09:21:52 2014 +0700

    use ddf 1.1

[33mcommit e19effcb2784822ce96761d169eab3ddaa9bdc68[m
Author: khangich <khangich@gmail.com>
Date:   Mon Nov 17 09:15:45 2014 +0700

    enable

[33mcommit 51b16a9b05184e009d6fff3690aaeda86ef9e5a5[m
Author: khangich <khangich@gmail.com>
Date:   Mon Nov 17 09:15:27 2014 +0700

    use new method to serialized tree

[33mcommit a96c781303754cf1b250dcb9388b2f7c430b2c03[m
Merge: b3e9193 6309ce5
Author: khangpham <khangpham@adatau.com>
Date:   Fri Nov 14 17:07:09 2014 +0700

    Merged in tf-idf (pull request #160)
    
    add TF-IDF and NaiveBayes

[33mcommit b3e9193affc87c0ab25c8853c502b52c09f05d4b[m
Merge: 88c752c 164981d
Author: khangpham <khangpham@adatau.com>
Date:   Fri Nov 14 17:05:51 2014 +0700

    Merged in hadoop-2-full-version-number (pull request #163)
    
    Hadoop 2 full version number + pA security + postgres support

[33mcommit f497f3fd517d99565ee060838d07f3f4d0d56318[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Nov 13 20:21:45 2014 -0800

    filter null value

[33mcommit 43ede73e6df79aee911b854db43fae1309b02bc7[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Nov 13 18:18:44 2014 -0800

    bug, Row() don't accept Array

[33mcommit 0813ff1753f21360eb4f711cdd39587f1f4bf9f2[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Nov 13 18:06:10 2014 -0800

    remove log message in tight loop

[33mcommit 3eb3713216399165f20aafbbd682d38942d770e5[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Nov 13 17:59:31 2014 -0800

    change columnType to Double after transform

[33mcommit 6c193d28fa4b7584d68e40f0de49013b748024c4[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Nov 13 17:17:41 2014 -0800

    fix bug in TransformHMap, wrong instantiate Array

[33mcommit bd5f23d4751c7d5fceaaff081fd258df05c6b5e7[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Nov 13 16:58:03 2014 -0800

    debugging TransformFactor

[33mcommit 4e9f6a22fa356effa59aef347cf23e99e2f33fa4[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Nov 13 16:43:21 2014 -0800

    fix bug in transformHMap

[33mcommit 98bd05e378521feff69e1b0cd83f3d45668e37ad[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Nov 13 16:29:10 2014 -0800

    debugging TransformHMap

[33mcommit 14f35b453744804aea81ed73b955aea908861aaf[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Nov 13 16:26:59 2014 -0800

    debugging, print colIdx

[33mcommit 43c7cdaaa9d6145824db7a75b3aa7adf41acd9a1[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Nov 13 15:58:14 2014 -0800

    debugging, fix keyValueMap

[33mcommit a3a32f99885817152a5f6cb2f0fbd3e976a7bd6f[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Nov 13 15:40:52 2014 -0800

    refactor TransformFactor

[33mcommit b2d1fdde068c41b05b701fff308113914b60047b[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Nov 13 15:29:07 2014 -0800

    add log message

[33mcommit 0899383f9d818eb5021f7d57f906bd9e2fa22d72[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Nov 13 15:09:12 2014 -0800

    finish class TransformFactor

[33mcommit e7d524f6b5ab29a1c31b3476e18412b00d2cf8c7[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Nov 13 13:11:41 2014 -0800

    move TransformFactor to correct position

[33mcommit 604e3cf911c1b259d165df81438b49509e3e1f31[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Nov 13 11:47:07 2014 -0800

    add new Executor TransformFactor
    
    to test getting all factor first

[33mcommit 10b5180fd33aed8b578ff72e2bd4ed9f0d82f120[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Nov 13 02:03:47 2014 -0800

    fix bug .max

[33mcommit 6c878fd46db35a6a4af85e7bd21572c9eb5c5435[m
Author: khangich <khangich@gmail.com>
Date:   Thu Nov 13 16:36:17 2014 +0700

    reformat rules

[33mcommit 92b280814cabb744d22a7c000389c7caafd03be8[m
Author: khangich <khangich@gmail.com>
Date:   Thu Nov 13 16:16:11 2014 +0700

    consolidate rules 1

[33mcommit b844ecef348e2480b7450157010cd800caa93187[m
Author: khangich <khangich@gmail.com>
Date:   Thu Nov 13 15:16:14 2014 +0700

    typo

[33mcommit 6a826703b84b9ae8a23f744d936fdcb18d38fc3b[m
Author: khangich <khangich@gmail.com>
Date:   Thu Nov 13 15:14:17 2014 +0700

    fix categorical rules

[33mcommit c80230f25fa1e61d6e25317eaeb73c2ca7dee539[m
Merge: 2e49d9b 7f9dfa6
Author: khangich <khangich@gmail.com>
Date:   Thu Nov 13 14:51:42 2014 +0700

    Merge branch 'ddf-sparksql-1.1.0' of bitbucket.org:adatao/ddf into ddf-sparksql-1.1.0

[33mcommit 2e49d9b680197093bb7a04974aeb1d43a783b154[m
Author: khangich <khangich@gmail.com>
Date:   Thu Nov 13 14:51:36 2014 +0700

    revert back rules

[33mcommit 7f9dfa6c9df38334a36a65abb3c56c0e03def3ca[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Nov 12 23:35:27 2014 -0800

    fix bug maxBins must be > max number of categorical

[33mcommit 53eac744d42437148a4a00a36e81d3aadf164854[m
Merge: ecf8f2e 9d4400a
Author: khangich <khangich@gmail.com>
Date:   Thu Nov 13 14:23:13 2014 +0700

    Merge branch 'ddf-sparksql-1.1.0' of bitbucket.org:adatao/ddf into ddf-sparksql-1.1.0

[33mcommit ecf8f2ea13a74e2c25798bcd21cfd37804764e96[m
Author: khangich <khangich@gmail.com>
Date:   Thu Nov 13 14:23:06 2014 +0700

    wip

[33mcommit 9d4400a6bfdf0922cc54c511ff540b5c289feda1[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Nov 12 23:06:01 2014 -0800

    get factor in DecisionTree, fix bug

[33mcommit b12b6ca8583a1f14b8e2c625aa95499377aecd55[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Nov 12 22:09:13 2014 -0800

    fix compile error

[33mcommit cccf55b98430c7e328c9e65277de46d4906e1b84[m
Merge: a972170 e95a06a
Author: khangich <khangich@gmail.com>
Date:   Thu Nov 13 13:08:55 2014 +0700

    Merge branch 'ddf-sparksql-1.1.0' of bitbucket.org:adatao/ddf into ddf-sparksql-1.1.0

[33mcommit e95a06aa9840b0e0a9a66bf61af03452c737feb3[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Nov 12 22:08:23 2014 -0800

    [DT] check if column is categorical, and get neccessary information for DT

[33mcommit a97217080e210f8984ca8ca72efb82e4300da3f8[m
Author: khangich <khangich@gmail.com>
Date:   Thu Nov 13 13:07:54 2014 +0700

    test remove unnecessary rules

[33mcommit 3a0f991d774ac657da2a9f80dee1ac55687bb5e1[m
Author: khangich <khangich@gmail.com>
Date:   Thu Nov 13 11:09:22 2014 +0700

    categorical column

[33mcommit 0e18afec0b3e05cbdcf7f54e39df96ecc69f1412[m
Author: khangich <khangich@gmail.com>
Date:   Thu Nov 13 10:32:27 2014 +0700

    reformat

[33mcommit 315226fb3ced7b47378bee0da5af499c32fff273[m
Author: khangich <khangich@gmail.com>
Date:   Thu Nov 13 10:15:55 2014 +0700

    add class no, rule

[33mcommit ef6df428e3db034780b083dcf60d36cae237f68f[m
Author: khangich <khangich@gmail.com>
Date:   Thu Nov 13 09:36:05 2014 +0700

    add rules to output

[33mcommit 16f13275b9c1dfd0d4129f632ce61c3f5d40b9d8[m
Merge: 14dcda1 9731f2e
Author: khangich <khangich@gmail.com>
Date:   Thu Nov 13 09:34:32 2014 +0700

    resolve conflict, print rules

[33mcommit 14dcda1c90ee4c1a6c7c79dc54fe9f77c023e564[m
Author: khangich <khangich@gmail.com>
Date:   Thu Nov 13 09:32:07 2014 +0700

    print rules

[33mcommit 9731f2e13431164b4eb142095a27770f18f20d0c[m
Author: adatao <khangich@gmail.com>
Date:   Thu Nov 13 01:34:13 2014 +0000

    fix pa-env for security patch

[33mcommit ce3c4fe56fe6a239b28879e633c8dad1e5e53c1e[m
Author: adatao <khangich@gmail.com>
Date:   Thu Nov 13 01:16:00 2014 +0000

    Revert "Revert "Merge branch 'master' into ddf-sparksql-1.1.0""
    
    This reverts commit 4faa483bb50e9e5ce10285fc67b836bcc474ae22.

[33mcommit 8505ce64d50a42c8311d6cef9c8f2805d73b31a5[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Nov 12 14:51:35 2014 -0800

    shorten model description

[33mcommit 4faa483bb50e9e5ce10285fc67b836bcc474ae22[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Nov 12 12:34:29 2014 -0800

    Revert "Merge branch 'master' into ddf-sparksql-1.1.0"
    
    This reverts commit 8e2c04c5801a31cc4318ca8ffd93a2e456a16c32, reversing
    changes made to 360832e849508fb7f02bed974d0395400687b949.

[33mcommit 34fc0a1a28b477de8a47f3340496f6ad13f601a7[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Nov 12 11:51:09 2014 -0800

    fix bug in DecisionTree.scala
    
    match case regression fail

[33mcommit 8e2c04c5801a31cc4318ca8ffd93a2e456a16c32[m
Merge: 360832e 88c752c
Author: huandao0812 <huan@adatau.com>
Date:   Wed Nov 12 09:14:46 2014 -0800

    Merge branch 'master' into ddf-sparksql-1.1.0
    
    Conflicts:
    	pa/conf/pa-env.sh

[33mcommit 360832e849508fb7f02bed974d0395400687b949[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Nov 12 08:34:47 2014 -0800

    add param minInstancePerNode and minInfomationGain

[33mcommit 264fee775c42e13065ea6dc679a1ef1a02979d93[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Nov 12 06:06:13 2014 -0800

    only compute numClasses if doing classification in DT

[33mcommit 6a500b497191cd22c2dd65a0189ed328e04f3d30[m
Merge: 8d4e977 384cf17
Author: huandao0812 <huan@adatau.com>
Date:   Wed Nov 12 06:04:41 2014 -0800

    Merge branch 'ddf-sparksql-1.1.0' of https://bitbucket.org/adatao/ddf into ddf-sparksql-1.1.0

[33mcommit 384cf17128747bfb608ee0fde92ecbafeaf1616e[m
Author: adatao <khangich@gmail.com>
Date:   Wed Nov 12 13:39:46 2014 +0000

    send substree to client

[33mcommit 7205b6d7d27300170f105a7ab586168da077b93d[m
Merge: 4fd25e3 be3f6f2
Author: adatao <khangich@gmail.com>
Date:   Wed Nov 12 10:39:23 2014 +0000

    Merge branch 'ddf-sparksql-1.1.0' of https://bitbucket.org/adatao/ddf into ddf-sparksql-1.1.0

[33mcommit be3f6f2c0020a56fac84fa80644981856d6ffac9[m
Author: khangich <khangich@gmail.com>
Date:   Wed Nov 12 17:38:43 2014 +0700

    remove maxdepth hardcode

[33mcommit 8d4e977215ade739a5f6f5fb47f9e0c331af588b[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Nov 11 22:39:12 2014 -0800

    check for null DDF in ROC

[33mcommit 4fd25e3613aad76625ab9343cd1ff87eedb22ed2[m
Merge: e238d47 85ef91a
Author: adatao <khangich@gmail.com>
Date:   Wed Nov 12 01:16:53 2014 +0000

    Merge branch 'ddf-sparksql-1.1.0' of https://bitbucket.org/adatao/ddf into ddf-sparksql-1.1.0

[33mcommit 85ef91a3e6bab9678b1c7a1bbe4219018fa7134f[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Nov 11 17:15:40 2014 -0800

    add resultDDF to DDFManager in YTrueYPred

[33mcommit e238d474ab7c7e61fe7b231f43cf64279a879f18[m
Merge: bdb2f5a 122cbe5
Author: adatao <khangich@gmail.com>
Date:   Wed Nov 12 01:02:09 2014 +0000

    Merge branch 'ddf-sparksql-1.1.0' of https://bitbucket.org/adatao/ddf into ddf-sparksql-1.1.0

[33mcommit 122cbe5ba5964a560d942b9e78c434f05098b3b4[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Nov 11 17:01:40 2014 -0800

    add R2Score for decisionTree

[33mcommit bdb2f5a3692f5f5d3843bdc6b3856739b7a92e24[m
Merge: 38b32cf 5437b5d
Author: adatao <khangich@gmail.com>
Date:   Wed Nov 12 00:46:35 2014 +0000

    Merge branch 'ddf-sparksql-1.1.0' of https://bitbucket.org/adatao/ddf into ddf-sparksql-1.1.0

[33mcommit 5437b5d5e2312faf1830cb2f6737bf2c12fc29f2[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Nov 11 16:45:39 2014 -0800

    add prediction for DecisionTree

[33mcommit 1de4e28c4226615a213ff110ecccefd6ca17194b[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Nov 11 15:48:16 2014 -0800

    add log message

[33mcommit 38b32cf22614098326ba77fc2bea4a4b06b4feba[m
Author: adatao <khangich@gmail.com>
Date:   Tue Nov 11 14:11:09 2014 +0000

    set driver.memory in pa-env.sh

[33mcommit 40f25d80b7e3924250b873808787a163062c283a[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Nov 11 06:03:35 2014 -0800

    fix DecisionTreeModel

[33mcommit 1acaa0ba5aaede949496782959a2cb67ca799d3b[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Nov 11 05:41:39 2014 -0800

    include spark_yarn

[33mcommit 9023322db3a8dbd7d04e148f6e7f13e1c1b30245[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Nov 11 05:38:16 2014 -0800

    exclude yarn from ddf_spark

[33mcommit 477d4b9d0d47c23882b3b00ce6fd3fd75a01a2c5[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Nov 11 05:20:57 2014 -0800

    decisionTree return its own model to RClient

[33mcommit 230a5a233c775ef3fa62856943baa6974f03a3d1[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Nov 11 04:47:40 2014 -0800

    print out tree in DecisionTree

[33mcommit bd0010c06fad95cf0f6e09872624ad6c87ed5dab[m
Author: adatao <khangich@gmail.com>
Date:   Tue Nov 11 12:29:28 2014 +0000

    change ddf_version to 1.0

[33mcommit 03db737729ee1b70be152013c9e33e3e817252c6[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Nov 11 02:01:41 2014 -0800

    use mtcars for DecisionTreeSuite

[33mcommit e7e21efea42d1d834d93c3fa0766227692b8d2b8[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Nov 11 00:18:03 2014 -0800

    fix compile error

[33mcommit 1b18511bc105d01bb5b767f775a190c212858da9[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Nov 11 00:14:37 2014 -0800

    run decisionTree directly without going through MLSupporter

[33mcommit a21b2a0baac0bd513d0a1b56344f022726ff0e6b[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Nov 10 22:55:47 2014 -0800

    add decision tree, add test

[33mcommit 805d9bb205422f0cd4915fa9f38b223a1be8542d[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Nov 10 17:33:33 2014 -0800

    [wip] decision tree

[33mcommit 00dead952a47fa4bbf8d6e9d01ff4741fd5edd90[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Nov 10 15:37:05 2014 -0800

    fix rootbuild

[33mcommit 20e29b97cebf1dcda20f74f029695b1893a101fe[m
Author: adatao <khangich@gmail.com>
Date:   Mon Nov 10 23:20:10 2014 +0000

    use runJob to force materialization

[33mcommit e55cc0202a360159644197fceaf7dee8e7ca9bf3[m
Author: adatao <khangich@gmail.com>
Date:   Mon Nov 10 23:19:42 2014 +0000

    fix rootbuild for spark 1.2.0

[33mcommit 7d1ce907f8097509aad83d61a0d683e697ea9649[m
Author: adatao <khangich@gmail.com>
Date:   Fri Nov 7 08:17:20 2014 +0000

    fix build_and_deploy_jars.sh

[33mcommit ad97d7fde9a19721c90efffa6d4228da4337ad09[m
Author: adatao <khangich@gmail.com>
Date:   Fri Nov 7 08:16:32 2014 +0000

    fix start-pa-server

[33mcommit 9ad1b77bb2e346860d55596cbad32dbb15b33835[m
Author: adatao <khangich@gmail.com>
Date:   Fri Nov 7 08:14:47 2014 +0000

    filter empty partition in TransformDummy

[33mcommit 31165d44b4d32f4980c55e30f8f79522219495bb[m
Author: adatao <khangich@gmail.com>
Date:   Fri Nov 7 08:04:47 2014 +0000

    fix pa-env.sh

[33mcommit 04dd50eac1d42f14c78edcbc89a41b52984af978[m
Author: adatao <khangich@gmail.com>
Date:   Fri Nov 7 08:02:19 2014 +0000

    fix rootbuild

[33mcommit 70260779de2cbe6ca299bbd5bd5629aeae6f539a[m
Author: adatao <khangich@gmail.com>
Date:   Fri Nov 7 06:34:51 2014 +0000

    change SPARK_VERSION

[33mcommit 4daa89c8805a0cae6643d55fb3d843338922d41a[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Nov 6 12:37:23 2014 -0800

    change SPARK_VERSION

[33mcommit 88c752cbcfcbf25fda0d5d3c77b02130779f12ea[m
Merge: a3f42c1 a6c9fbb
Author: huandao0812 <huan@adatau.com>
Date:   Tue Nov 4 18:55:06 2014 -0800

    Merged in add-security-3 (pull request #162)
    
    refactor security

[33mcommit a6c9fbb7f6455d5c844692940c7666d401ac2e07[m
Merge: eaf4437 a3f42c1
Author: huandao0812 <huan@adatau.com>
Date:   Tue Nov 4 14:00:06 2014 -0800

    Merge branch 'master' into add-security-3
    
    Conflicts:
    	pa/conf/pa-env.sh

[33mcommit 164981d8656fe919ca093bdb37a28cc782a3a978[m
Author: nhan vu <nhanitvn@adatao.com>
Date:   Tue Nov 4 08:20:37 2014 +0000

    mysql is the default db for hive metastore

[33mcommit f0eb5f97ad8ec38ab5051b5ecf1062b491c41447[m
Author: nhan vu <nhanitvn@adatao.com>
Date:   Tue Nov 4 08:16:01 2014 +0000

    fix assemply pom version

[33mcommit 32adb8afc5f9251869fc49ddee060adf48a7e756[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Oct 23 23:17:18 2014 -0700

    small fix for Binning.scala
    
    don’t need to convert dataContainerID to ddfID

[33mcommit 3e55fcd71020c62c7098ea2dde7cb8b34bc739f9[m
Merge: 722b60a 51a053e
Author: root <root@ip-10-159-15-124.ec2.internal>
Date:   Wed Oct 22 22:12:26 2014 +0000

    Merge branch 'ddf-sparksql-1.1.0' of https://bitbucket.org/adatao/ddf into ddf-sparksql-1.1.0

[33mcommit 722b60adb227ebd8ce153a9b0fcc734ca22b65fc[m
Author: root <root@ip-10-159-15-124.ec2.internal>
Date:   Wed Oct 22 22:11:43 2014 +0000

    fix GroupBy test

[33mcommit 51a053e662bb8b17b45dfc192381fbab87edd2aa[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Oct 22 14:52:18 2014 -0700

    set env variable bigr.multiuser=false in pom.xml

[33mcommit 495ac07df807cca3c70e09622228c8e2ec8b42cc[m
Author: root <root@ip-10-159-15-124.ec2.internal>
Date:   Wed Oct 22 21:50:04 2014 +0000

    add Model in ddf.ini

[33mcommit 55505b2652127d92dce381d277a3b38d37ddad59[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Oct 21 22:45:58 2014 -0700

    delete MetricsTests & MLlibIntegrationSuite

[33mcommit 5387597895553cdc2d7e5c88437961beac3446a3[m
Author: root <root@ip-10-159-15-124.ec2.internal>
Date:   Wed Oct 22 05:43:17 2014 +0000

    fix FiveNumSuite

[33mcommit 28b605aa60a848c33778e4d565745087de8d08a6[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Oct 21 22:37:14 2014 -0700

    ignore com.adatao.spark.ddf.analytics.RegressionSuite as it no longer applicable

[33mcommit 3ae7717c1b8e06be51f40654d1ba88ec83e4a674[m
Author: root <root@ip-10-159-15-124.ec2.internal>
Date:   Wed Oct 22 04:51:40 2014 +0000

    fix sampleDataFrameSuite and VectorQuantilesSuite

[33mcommit b60c643161a35167d14b295ccd74e0030e4d44de[m
Merge: 134a8aa cadf5c1
Author: root <root@ip-10-159-15-124.ec2.internal>
Date:   Wed Oct 22 00:19:09 2014 +0000

    Merge branch 'ddf-sparksql-1.1.0' of https://bitbucket.org/adatao/ddf into ddf-sparksql-1.1.0

[33mcommit 134a8aa5eeb692dd265690b52ccf56ce2b5e66c2[m
Author: root <root@ip-10-159-15-124.ec2.internal>
Date:   Wed Oct 22 00:18:32 2014 +0000

    fix typo in TJsonSerializable.scala

[33mcommit cadf5c171aa442da4b713cc251b22ec5ea66586e[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Oct 21 17:17:10 2014 -0700

    [wip] refactor TransformDummy for new ColumnAccessor design in Spark

[33mcommit f59f7c061a4347087745f3414eca53770f937263[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Oct 20 16:47:15 2014 -0700

    fix typo in new SparkModelDeserializer

[33mcommit cf56ae0b2abf0170c5d097781cb5c6c0ff193b5b[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Oct 20 16:38:42 2014 -0700

    add serDes for new io.spark.ddf.ml.Model

[33mcommit 7ff4cdf99d194cfd701d5bc6bfe319f50279d633[m
Author: nhan vu <nhanitvn@adatao.com>
Date:   Mon Oct 20 07:29:42 2014 +0000

    revert to a Bach's working version

[33mcommit 05cc2f72d7b579d8e323a04ea8eeac0caaf2c531[m
Author: nhan vu <nhanitvn@adatao.com>
Date:   Mon Oct 20 07:29:14 2014 +0000

    create/edit files for packaging

[33mcommit 993a177d565c4dcd3928b45ef2c6c30d34efd7de[m
Author: nhan vu <nhanitvn@adatao.com>
Date:   Mon Oct 20 07:25:21 2014 +0000

    security added to Thrift Server by a Bach

[33mcommit b5d5f163072d0a051ffb29ec072600148255fc1b[m
Merge: d37df3c eaf4437
Author: nhan vu <nhanitvn@adatao.com>
Date:   Mon Oct 20 07:22:58 2014 +0000

    a Bach added security

[33mcommit d37df3cca61e89c898d169ccb9a5f863c99c645d[m
Merge: a34ac5d 61f87e2
Author: nhan vu <nhanitvn@adatao.com>
Date:   Wed Oct 15 15:16:27 2014 +0000

    merge

[33mcommit eaf443753860ad0913ec8694aa1facfccade3beb[m
Author: Michael B. Bui <freeman@adatao.com>
Date:   Wed Oct 15 22:13:43 2014 +0700

    update

[33mcommit be0fdb84786b4deb70dc9571442d8aaf317ef013[m
Author: Michael B. Bui <freeman@adatao.com>
Date:   Wed Oct 15 21:46:08 2014 +0700

    fix config

[33mcommit 61f87e286e31f7ae7a0e1649ec17df8e0217a4f5[m
Author: Michael B. Bui <freeman@adatao.com>
Date:   Wed Oct 15 21:40:21 2014 +0700

    add run.as.admin

[33mcommit a34ac5d205883fae002831b6c969d19fccbbf980[m
Merge: 96405b7 6b78497
Author: nhan vu <nhanitvn@adatao.com>
Date:   Wed Oct 15 10:03:06 2014 +0000

    Merge branch 'add-security-3' into hadoop-2-full-version-number

[33mcommit 96405b7c3d035f945b37892a621899be9c831c3c[m
Merge: 4cdfa42 a3f42c1
Author: nhan vu <nhanitvn@adatao.com>
Date:   Wed Oct 15 10:00:24 2014 +0000

    fix pa-env.sh

[33mcommit 6b784971e7860fa1971d07d1de4dbbd38379d5e3[m
Author: Michael B. Bui <freeman@adatao.com>
Date:   Wed Oct 15 16:56:28 2014 +0700

    refactor

[33mcommit 3b0982b1d88cd2c8cb6f484dbd70bfb5e5f86848[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Oct 15 15:36:48 2014 +0700

    remove class com.adatao.spark.ddf.etl.SqlHandler

[33mcommit f2aa8f48655ce546c0b1769718f9a7860ee30f90[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Oct 15 14:23:15 2014 +0700

    sql2ddf call the correct super method

[33mcommit b8933961f19a656aab8aeb1d89cc53caf93ef199[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Oct 15 14:07:09 2014 +0700

    add new class SqlHandler

[33mcommit 63e226b5939cc5f3dedbfbb683577f8732be6e05[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Oct 15 12:42:06 2014 +0700

    fix error in YtrueYPred

[33mcommit 23278eed1951e32d08edb52fe27667ed26aee9a2[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Oct 15 12:32:02 2014 +0700

    add log message

[33mcommit e227bc9b3708dae386cd60b412593d46dd94dc99[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Oct 15 12:18:47 2014 +0700

    fix compile error

[33mcommit cade6fcb83c110b5cf1878cfca12aed60fa85136[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Oct 15 11:38:09 2014 +0700

    fix compile error

[33mcommit 86c82f2e1f7204ceff23f71f3d637bca88ff26f2[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Oct 15 11:36:56 2014 +0700

    fix compile error

[33mcommit ef2726da5854219952b65a4a7af7865f70c570c6[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Oct 15 11:31:54 2014 +0700

    create new class SparkDDF
    
    to cache table and force materializing of the table

[33mcommit 4cdfa424410782f5228bd3f1bfc9413a0fd83766[m
Author: nhan vu <nhanitvn@adatao.com>
Date:   Fri Oct 10 07:48:43 2014 +0000

    fix configs to work with hadoop 2.4.1

[33mcommit a3f42c18ac6b7635a234a3442aab2d165385ef33[m
Merge: 75608f4 25a973e
Author: Binh Han <bhan@adatao.com>
Date:   Wed Oct 8 13:36:16 2014 -0700

    Merged in fix-start-pa-server (pull request #161)
    
    fix start - pa -server

[33mcommit 25a973e08735c52bb3b9d504b4ce666a8cdc79e1[m
Author: Long Pham <longpham@adatao.com>
Date:   Wed Oct 8 09:50:22 2014 +0000

    start-pa-server.sh edited online with Bitbucket

[33mcommit 02dc9fdf203f73dfdc0380551a5f491c98f68091[m
Author: Long Pham <longpham@adatao.com>
Date:   Wed Oct 8 09:36:42 2014 +0000

    pa-env.sh edited online with Bitbucket

[33mcommit 2898fb36ea6dd683ff343f7fa334f5b2baea0421[m
Author: Long Pham <longpham@adatao.com>
Date:   Wed Oct 8 09:33:43 2014 +0000

    pa-env.sh edited online with Bitbucket

[33mcommit b70508dd26a77a70faff9de6c2dd4604e5b164a3[m
Author: Long Pham <longpham@adatao.com>
Date:   Wed Oct 8 09:30:49 2014 +0000

    pa-env.sh edited online with Bitbucket

[33mcommit 1d545b2588e8901ea94b2e53a6349015f02056e0[m
Author: Long Pham <longpham@adatao.com>
Date:   Wed Oct 8 09:29:05 2014 +0000

    start-pa-server.sh edited online with Bitbucket

[33mcommit db1dd32f262f20efd96c19931750308dab7b619f[m
Author: nhan vu <nhanitvn@adatao.com>
Date:   Wed Oct 8 08:47:39 2014 +0000

    fix RootBuild to have full Hadoop version number

[33mcommit 4a0a9581bc79bcc404dfd1da6dd2377f9b2f5d10[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Oct 6 12:50:35 2014 +0700

    fix bug in dummyCoding

[33mcommit e0965bea942de1620db38c281535d78d11e0dbeb[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Oct 6 11:01:19 2014 +0700

    remove print statements in TransformDummy

[33mcommit 23cb760cd0e46e53309400ff7a36982ff9fe302b[m
Author: huandao0812 <huan@adatau.com>
Date:   Sat Oct 4 13:27:32 2014 +0700

    [wip] debugging TransformDummy

[33mcommit 6309ce5ce515a7a510f44189cffea3bfa57f27ff[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Oct 1 14:27:06 2014 +0700

    add test data

[33mcommit b357df754f1e281e492bf6ff0348d99771182d48[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Sep 30 17:30:06 2014 +0700

    add TF-IDF and NaiveBayes

[33mcommit 75608f46a133b06cfaebd3e837976f0c9ae6c06b[m
Merge: bd56771 ed27789
Author: Binh Han <bhan@adatao.com>
Date:   Mon Sep 29 18:11:01 2014 -0700

    Merged in ddf-add-movingaverage (pull request #159)
    
    add moving average UDF

[33mcommit 71fb63a9d9da41a9d49d619fb36c45cd45c57ec1[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Sep 29 16:00:36 2014 +0700

    fix BinningSuite, JoinSuite, MRNativeSuite, TransformNativeSuite

[33mcommit ca686e88390f9955a89c7fed32f335b249ba662b[m
Merge: be3fe96 bd56771
Author: huandao0812 <huan@adatau.com>
Date:   Mon Sep 29 14:30:40 2014 +0700

    Merge branch 'master' into ddf-sparksql-1.1.0
    
    Conflicts:
    	bin/build_all.sh
    	pa/conf/pa-env.sh
    	pa/src/main/scala/com/adatao/pa/spark/execution/Kmeans.scala
    	pa/src/main/scala/com/adatao/pa/spark/execution/YtrueYpred.scala

[33mcommit be3fe96dc8c06a13805ddb4de160ef21fcb63c91[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Sep 29 10:20:54 2014 +0700

    caching SchemaRDD for dummyCoding

[33mcommit b76ad1532fe76616645ab2beee85d365b7adfa65[m
Author: huandao0812 <huan@adatau.com>
Date:   Sun Sep 28 16:06:00 2014 +0700

    fix SharkDataFrameSuite

[33mcommit e87aa5723a210929a0006d385d3cccd41b97ecf5[m
Author: huandao0812 <huan@adatau.com>
Date:   Sun Sep 28 12:29:56 2014 +0700

    clean up TransformDummy.scala

[33mcommit 47d4f9a64c4ee5b2807039fb3b542bec9c189577[m
Author: huandao0812 <huan@adatau.com>
Date:   Sun Sep 28 11:42:06 2014 +0700

    uncomment test

[33mcommit 8e89a7fa733c2eef8cd74f902c03644f6b90f41f[m
Author: huandao0812 <huan@adatau.com>
Date:   Sun Sep 28 11:12:39 2014 +0700

    testing

[33mcommit c09a2ed0c20a34eae14efe1a74437eb5a703b2a7[m
Author: huandao0812 <huan@adatau.com>
Date:   Sun Sep 28 00:39:03 2014 +0700

    port TransformDummy to SparkSQL
    
    still have issues running test, mostly outOfMemory

[33mcommit ed277896bb2101a79a282b869b6fcb295bd55f60[m
Author: khangich <khangich@gmail.com>
Date:   Sat Sep 27 15:36:08 2014 +0700

    add moving average UDF

[33mcommit bd56771bb5a7163554a93ee6f2d792de0212d5d1[m
Merge: 55a713e 922a7c3
Author: khangpham <khangpham@adatau.com>
Date:   Fri Sep 26 17:29:09 2014 +0700

    Merged in ddf-naiveBayes (pull request #158)
    
    add naiveBayes executor

[33mcommit 922a7c38e99349411b55411eeae1bc74972f0e73[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Sep 26 17:24:56 2014 +0700

    add ROC test for naiveBayes

[33mcommit 1c8876b7920cea5f7392c6e12e04920c6d669452[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Sep 26 17:12:16 2014 +0700

    add test for NaiveBayes

[33mcommit 13f778747eaa972e03342a41bd1fdd3b70443ea9[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Sep 26 16:26:26 2014 +0700

    fix error

[33mcommit 1bec449bd71af5a09192706357accbf2a43ce232[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Sep 26 16:19:25 2014 +0700

    update confusionMatrix, to work with model other than ALinearModel

[33mcommit 9b1e604f48a5a8b4c7b79aa428bccc27a5152964[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Sep 26 15:45:46 2014 +0700

    add executor for NaiveBayes

[33mcommit 55a713ee91347080a03b444fd03c246e4d644e9c[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Sep 25 15:48:44 2014 +0700

    fix typo make-poms.sh

[33mcommit ba8b59c7744136e3f44f78f66819f9fa1adec144[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Sep 25 15:33:33 2014 +0700

    update bin/build-all.sh
    
    get-sbt in bin/build-all.sh

[33mcommit 1a887a70e7a5007d3260c0e754362c4a7c3e177f[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Sep 25 01:31:50 2014 +0700

    fix SampleDataFrameSuite

[33mcommit b5fb6eed3d60aebea4219cc555d7a1d182ebe90e[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Sep 24 11:48:38 2014 +0700

    fix kmeans bug

[33mcommit 2913f414e8afd037e13d6ea26cd81818e9d9990e[m
Merge: 9112d58 c56b57f
Author: Binh Han <bhan@adatao.com>
Date:   Tue Sep 23 14:21:25 2014 -0700

    Merged in adding-security-2 (pull request #157)
    
    Adding security (compile bug fix)

[33mcommit c56b57fdd8042d1cf2da96228cc33c3ae6b76753[m
Author: Pham Nam Long <longpham@adatao.com>
Date:   Tue Sep 23 19:40:11 2014 +0000

    disable multiContextConnect

[33mcommit fbecf6b79309ac1810c57cf247bbc391d413a0f2[m
Author: Pham Nam Long <longpham@adatao.com>
Date:   Tue Sep 23 19:11:59 2014 +0000

    rm BigRWorker

[33mcommit 14481d02c7ec0192bba07e46f2f3a2307c39f233[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Sep 23 17:05:11 2014 +0700

    able to compile

[33mcommit b69ec0b2f7b6b05d8c5cb0ea7da3c8f1375b52fc[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Sep 23 16:21:21 2014 +0700

    fix build

[33mcommit 535d73e28a2fb847ac84a55ed67c285574502e9f[m
Merge: f5748bc 9112d58
Author: huandao0812 <huan@adatau.com>
Date:   Tue Sep 23 14:58:29 2014 +0700

    merge with master

[33mcommit e77d75b913b218cfe5e15c7c72eaf857a0c8ab8e[m
Author: Michael B. Bui <freeman@adatao.com>
Date:   Mon Sep 22 16:56:02 2014 -0700

    change pa-env.sh to support security

[33mcommit 1d89b01327a4ab218f33b14ce4e1916efaf3d3ac[m
Author: Michael B. Bui <freeman@adatao.com>
Date:   Mon Sep 22 15:45:44 2014 -0700

    fix bugs

[33mcommit 45966241f168d15177a61499f6d59d6fa03d3977[m
Author: Michael B. Bui <freeman@adatao.com>
Date:   Wed Sep 17 16:15:56 2014 -0700

    adding pA security with kerberos authetication

[33mcommit 9112d58a0e963ae443bbc0210b4db1ce832aee45[m
Merge: 6c6f565 7bae896
Author: Binh Han <bhan@adatao.com>
Date:   Mon Sep 22 13:51:46 2014 -0700

    Merged in cleanup (pull request #155)
    
    pa cleanup

[33mcommit 7bae896cb6e51b81dbca53ae26037af36bba6694[m
Author: bhan <bhan@adatao.com>
Date:   Mon Sep 22 13:48:32 2014 -0700

    executor to throw exception instead of null return

[33mcommit 948212c4135e6613ceb4343e9d36ae511c1768fc[m
Author: bhan <bhan@adatao.com>
Date:   Mon Sep 22 13:47:44 2014 -0700

    add featuredCols to kmeansModel

[33mcommit cb8455ee07926b21588533014b68b47129c5f9f0[m
Author: bhan <bhan@adatao.com>
Date:   Mon Sep 22 12:43:38 2014 -0700

    fix subsetsuite

[33mcommit 6c6f5658b56aaa7b941b7018c7f1137496ec16db[m
Merge: 95b033d 68121ed
Author: huandao0812 <huan@adatau.com>
Date:   Tue Sep 23 00:04:21 2014 +0700

    Merged in fix-r2score (pull request #154)
    
    set dummyCoding for ALinearModel model after training

[33mcommit 68121edf9f8cf5bfc0fd50af8b4f4d86cae549bc[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Sep 22 23:42:24 2014 +0700

    reformat code

[33mcommit 95b033d4df619984d16a42c2bf38b33c32d32248[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Sep 22 23:39:47 2014 +0700

    Revert "Merged in adding-security (pull request #145)"
    
    This reverts commit 8f1d4d77a49ca528ae146df1f29b391ec625a355, reversing
    changes made to 6d88d7f5dfcc2c2b9e4456c1f3441ecaae7d1799.

[33mcommit 5c9f87af1de228aebc1f8928ea02ebd5e3c6cb29[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Sep 22 22:45:03 2014 +0700

    revert change commit from smaster

[33mcommit 714af68d1aca609503029bf62b32325a8e946a17[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Sep 22 17:28:46 2014 +0700

    fix metricsSuite

[33mcommit fddaad65d17470612e141f389e26882156e9871c[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Sep 22 16:20:10 2014 +0700

    fix sample2dataframe

[33mcommit 01ef93964ad626f9989a2ec2dfc4a96938d0b720[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Sep 22 16:06:07 2014 +0700

    set dummyCoding from transformedDDF

[33mcommit e0b9013cd4de6860c009b3be6f59068196c464ca[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Sep 22 15:51:52 2014 +0700

    set dummyCoding for ALinearModel model after training

[33mcommit 2eded57848bb678bdc3f43b98d3c0b181b43b025[m
Merge: e8953d2 ef24697
Author: Pham Nam Long <longpham@adatao.com>
Date:   Mon Sep 22 06:59:54 2014 +0000

    Merge branch 'master' of https://bitbucket.org/adatao/ddf
    
    Conflicts:
    	pa/conf/pa-env.sh

[33mcommit e8953d2e2da1097b2dbcb42124deadd27b7359e1[m
Author: Pham Nam Long <longpham@adatao.com>
Date:   Mon Sep 22 06:58:37 2014 +0000

    add security change from smaster

[33mcommit ef24697d9ec841c00909deebe5db6259931fce3a[m
Merge: 0b0fb8e 77a285a
Author: huandao0812 <huan@adatau.com>
Date:   Mon Sep 22 11:33:39 2014 +0700

    Merged in fix-r2score (pull request #153)
    
    fix r2Score, work with dummyCoding

[33mcommit 77a285af615d9d0a30bbdfc701b29943158d608f[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Sep 22 11:18:52 2014 +0700

    move cache/unpersist in LogisticRegressionIRLS inside train method

[33mcommit f5bc33685429f9d39cd7b90d9a5e1d9de83bac61[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Sep 22 10:53:48 2014 +0700

    cache/uncache rdd for LogisticRegressionIRLS

[33mcommit e67576ca4c3b1e21d98ba9ae88fb2e1f60ef312d[m
Author: huandao0812 <huan@adatau.com>
Date:   Sun Sep 21 17:56:29 2014 +0700

    clean up

[33mcommit 0b0fb8e087f764e2092fd2f68d9bc9ea7a975b26[m
Merge: be0c271 8df5e62
Author: huandao0812 <huan@adatau.com>
Date:   Sun Sep 21 17:52:19 2014 +0700

    Merged in fix-dummy-applyModel (pull request #152)
    
    New design for Residuals & BinaryConfusionMatrix

[33mcommit 8df5e62e07145459e79b7a63009c2d8f2b3c2658[m
Author: khangich <khangich@gmail.com>
Date:   Sun Sep 21 17:44:18 2014 +0700

    pass roc

[33mcommit 22dc32a9e05980dc9b90f2cb400b8f00da6fd803[m
Author: huandao0812 <huan@adatau.com>
Date:   Sun Sep 21 17:18:34 2014 +0700

    cache training data for LinearRegressionGD and LogisticsRegressionGD

[33mcommit 552295b934f50a7d891952460c6b92a32b48c0a4[m
Author: huandao0812 <huan@adatau.com>
Date:   Sun Sep 21 16:51:42 2014 +0700

    fix-r2Score

[33mcommit 895a507bf31fbbb3a90bdfb4fb8e66ee201c3017[m
Author: huandao0812 <huan@adatau.com>
Date:   Sun Sep 21 16:21:59 2014 +0700

    add assert to ROC test in MetricsSuite

[33mcommit 0add0a3f964d37079ee37d2deacbc0a03b8977ef[m
Author: huandao0812 <huan@adatau.com>
Date:   Sun Sep 21 14:34:20 2014 +0700

    IRLSLogisticRegressionModel extends ALinearModel

[33mcommit e3547a65a5df6507eb0e9c091a5a6e983e2f0ee8[m
Author: huandao0812 <huan@adatau.com>
Date:   Sun Sep 21 12:25:35 2014 +0700

    debugging

[33mcommit 46d2f52f4403e7aba5f514209b2272c82a56ce60[m
Author: huandao0812 <huan@adatau.com>
Date:   Sun Sep 21 11:46:15 2014 +0700

    change Array to ListBuffer

[33mcommit 4b2bc757692c9275d9d7079be18f489562258863[m
Merge: 4ba4e9a 09e41c2
Author: khangich <khangich@gmail.com>
Date:   Sat Sep 20 22:40:07 2014 +0700

    fix conflict, confusion matrix still have error

[33mcommit 4ba4e9ad3e6cf50571a4ed5650f6ba7afb1f909d[m
Author: khangich <khangich@gmail.com>
Date:   Sat Sep 20 22:28:05 2014 +0700

    fix roc

[33mcommit 09e41c22e904effe8b2feda15a141fa32028a5d8[m
Author: huandao0812 <huan@adatau.com>
Date:   Sat Sep 20 18:03:34 2014 +0700

    remove debug message

[33mcommit 029838e68b6c08d4cd62c4b4b97f4a1c2d295893[m
Author: huandao0812 <huan@adatau.com>
Date:   Sat Sep 20 17:34:45 2014 +0700

    new BinaryConfusionMatrix design
    
    test passed

[33mcommit 044a37fbaf8e29bea7334a5cf06284fb429afcf7[m
Merge: bf1398a 9a24daf
Author: huandao0812 <huan@adatau.com>
Date:   Sat Sep 20 10:48:40 2014 +0700

    Merge branch 'fix-dummy-applyModel' of https://bitbucket.org/adatao/ddf into fix-dummy-applyModel
    
    Conflicts:
    	spark_adatao/src/main/scala/com/adatao/spark/ddf/analytics/ALinearModel.scala
    	spark_adatao/src/main/scala/com/adatao/spark/ddf/analytics/LogisticRegression.scala

[33mcommit bf1398afd58218b646734b563b306974a5b61eae[m
Author: huandao0812 <huan@adatau.com>
Date:   Sat Sep 20 10:18:44 2014 +0700

    fix yTrueYPred
    
    the method called in side yTrueYPred flatmap must be a static method,
    else we’ll get not serializable exception on SparkDDFManager

[33mcommit 9a24daf55f1df6464d446f55f7430b96f9c05802[m
Author: khangich <khangich@gmail.com>
Date:   Sat Sep 20 10:04:33 2014 +0700

    wip to fix roc

[33mcommit be0c2714670d57d34fd9e99e27c464fcc1616953[m
Merge: 8f1d4d7 aac4aa9
Author: khangpham <khangpham@adatau.com>
Date:   Sat Sep 20 09:10:20 2014 +0700

    Merged in fix-dummy-applyModel (pull request #151)
    
    new design for dummyCoding prediction

[33mcommit 8f1d4d77a49ca528ae146df1f29b391ec625a355[m
Merge: 6d88d7f 2302791
Author: Binh Han <bhan@adatao.com>
Date:   Fri Sep 19 13:07:51 2014 -0700

    Merged in adding-security (pull request #145)
    
    adding pA security with kerberos authetication

[33mcommit aac4aa91dfb1399d29bda14c20582f2733bb508b[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Sep 19 11:57:26 2014 +0700

    reformat ALinearModel

[33mcommit ba44b35bd834c1ca2557d27d6c350e1322afe8ec[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Sep 19 11:55:37 2014 +0700

    reformat code

[33mcommit 889dbaea815c1ea1a54ede35fd5121a464b5af67[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Sep 19 11:50:39 2014 +0700

    clean up code, reformat MetricsSuite

[33mcommit 6d88d7f5dfcc2c2b9e4456c1f3441ecaae7d1799[m
Merge: 21aa9b4 0d32c37
Author: Binh Han <bhan@adatao.com>
Date:   Thu Sep 18 13:53:50 2014 -0700

    Merged in bhan-fix (pull request #150)
    
    compute WCSS for kmeans

[33mcommit 0d32c371229edc0712b9d7787e91d59e5492eae3[m
Author: bhan <bhan@adatao.com>
Date:   Thu Sep 18 13:50:54 2014 -0700

    tested

[33mcommit 83c858f26d6cbcd5253e371dfb5fa223c76c600f[m
Merge: 8b33a5b 21aa9b4
Author: bhan <bhan@adatao.com>
Date:   Thu Sep 18 13:36:42 2014 -0700

    Merge branch 'master' of https://bitbucket.org/adatao/ddf into bhan-fix

[33mcommit 8b33a5bcab106026ec4a6744ee3b0d7a35417c27[m
Author: bhan <bhan@adatao.com>
Date:   Thu Sep 18 13:35:33 2014 -0700

    compute wcss for kmeans

[33mcommit fd78415f1805b39e680121d103bf25ceb82d5aa8[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Sep 18 22:48:33 2014 +0700

    uncomment assert

[33mcommit 3596a680493d6d127ba4ce8ca75657de2dd14b9e[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Sep 18 22:21:53 2014 +0700

    clean up, add implicit conversion for TransformationHandler

[33mcommit bc28cd31baf9109c76d446a73ca1046ebdc89126[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Sep 18 19:58:45 2014 +0700

    add debug message

[33mcommit 2d67342840ce98320909e0d9e47c608e808c0423[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Sep 18 17:05:31 2014 +0700

    new design for dummyCoding prediction

[33mcommit 21aa9b4462098f8df10f856f2f55be71f5f1b49b[m
Merge: 3500e65 bed35fd
Author: khangpham <khangpham@adatau.com>
Date:   Thu Sep 18 14:50:20 2014 +0700

    Merged in smart-summary (pull request #149)
    
    fix coltypes

[33mcommit bed35fd8589bca9f4dba39fd997e223e02c66550[m
Author: khangich <khangich@gmail.com>
Date:   Thu Sep 18 14:49:30 2014 +0700

    fix coltypes

[33mcommit 3500e65b7d39b948bc61a8e5be9fa61de504800b[m
Merge: 8ef8653 acd42c7
Author: huandao0812 <huan@adatau.com>
Date:   Thu Sep 18 11:31:40 2014 +0700

    Merged in fix-dummy (pull request #148)
    
    fix residuals

[33mcommit 8ef8653b88b3cc66b9856030e43fdfc8febf31ef[m
Merge: 13c53c1 3c04872
Author: khangpham <khangpham@adatau.com>
Date:   Thu Sep 18 11:23:32 2014 +0700

    Merged in smart-summary (pull request #147)
    
    Smart summary to support SQ on PI

[33mcommit acd42c74c884d5f9c378bb3508b6cd05259fb75e[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Sep 18 11:22:10 2014 +0700

    fix residuals

[33mcommit 3c048722d0a47f1859066677256af811274a2d6c[m
Author: khangich <khangich@gmail.com>
Date:   Thu Sep 18 11:19:13 2014 +0700

    change namespace to compatible with pInsights

[33mcommit 13c53c1a69c053c6c6df2f3be83d870f0c0ee99d[m
Merge: 7df4bfe f8d8a90
Author: huandao0812 <huan@adatau.com>
Date:   Thu Sep 18 11:01:42 2014 +0700

    Merged in fix-dummy (pull request #146)
    
    Fix dummyCoding return trained columns & dummy mapping

[33mcommit 23027910cddfd5653a2ad6f5a2a518ad2f400c58[m
Author: Michael B. Bui <freeman@adatao.com>
Date:   Wed Sep 17 16:15:56 2014 -0700

    adding pA security with kerberos authetication

[33mcommit f8d8a90d0aac5dbcd3e20541436e2ad298805d6e[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Sep 17 17:06:10 2014 +0700

    add field colNameMapping to dummyCoding

[33mcommit 33b82873008d8ce2e13e695bab07da0f0a2d9f4b[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Sep 17 15:45:19 2014 +0700

    don't return feature's label to client

[33mcommit 3beed5d62b9d5153a7a3d644c592dc782a056eba[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Sep 17 10:51:02 2014 +0700

    clean up dummyCoding

[33mcommit 9e25265319e22d3f701c87394d65a8a477a2ee14[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Sep 17 10:39:25 2014 +0700

    fix return column name

[33mcommit cc9f1d58dabedfe7913101aba6f68a27ba8f1353[m
Author: root <root@ip-10-158-36-101.ec2.internal>
Date:   Tue Sep 16 09:09:30 2014 +0000

    fix build_all

[33mcommit a75b1420970a29c47b2d25a86c0183e77d27ff21[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Sep 16 16:04:23 2014 +0700

    fix dummyCoding

[33mcommit f5748bcffb8a85804a832e4aea4cc98d3d1ce0be[m
Author: root <root@ip-10-159-12-103.ec2.internal>
Date:   Tue Sep 16 05:45:18 2014 +0000

    fix pa-env

[33mcommit 0a13ef2cc7e90309e4312c78c98ccae70d2dac06[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Sep 15 17:57:24 2014 +0700

    fix local log4j

[33mcommit 566f659568f1068383bfaffa675ed77dd2af1cc5[m
Author: root <root@ip-10-159-8-249.ec2.internal>
Date:   Mon Sep 15 09:44:08 2014 +0000

    able to start sparkCTX

[33mcommit 63402269b4bdc0f7acaa55ecbde1b36de20da27f[m
Author: khangich <khangich@gmail.com>
Date:   Mon Sep 15 15:05:18 2014 +0700

    add advance summary to PA with unit test

[33mcommit 430eb482b71a4743f4a156500bda34791e925604[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Sep 15 12:16:54 2014 +0700

    exclude hadoop-core from ddf-opensource

[33mcommit 7df4bfed90fde67dafeea716535f356241b651ad[m
Author: khangich <khangich@gmail.com>
Date:   Sun Sep 14 21:47:18 2014 +0700

    fix hardcode for namenode, pa-env for cluster deply

[33mcommit 60cc9b6bff9f98708997df64d72e96f413b146ee[m
Merge: 030d5cd 89760fd
Author: huandao0812 <huan@adatau.com>
Date:   Sun Sep 14 09:39:30 2014 +0700

    Merged in fix-executor (pull request #143)
    
    throw exception when needed, do not return null for executor

[33mcommit 89760fdbee380dd906a4ca505ecff0baa70ddd73[m
Merge: 6712391 030d5cd
Author: huandao0812 <huan@adatau.com>
Date:   Sun Sep 14 09:37:48 2014 +0700

    Merge branch 'master' into fix-executor

[33mcommit 030d5cd949f1a1747a6d5cae6974426b3842fbca[m
Merge: 9ba4d1a 05319b2
Author: huandao0812 <huan@adatau.com>
Date:   Sun Sep 14 09:37:23 2014 +0700

    Merge branch 'master' of https://bitbucket.org/adatao/ddf

[33mcommit 6712391a954bc34340cb8e9b7f5ee6c331c6b124[m
Merge: f83d803 9ba4d1a
Author: huandao0812 <huan@adatau.com>
Date:   Sun Sep 14 09:32:53 2014 +0700

    Merge branch 'master' into fix-executor
    
    Conflicts:
    	pa/src/main/java/com/adatao/pa/spark/execution/Sql2DataFrame.java
    	pa/src/main/java/com/adatao/pa/spark/execution/VectorCorrelation.java

[33mcommit 9ba4d1a04ba01847ed07eaaac6bab45ae1b0e245[m
Author: huandao0812 <huan@adatau.com>
Date:   Sun Sep 14 09:31:36 2014 +0700

    Merge branch 'master' of https://bitbucket.org/adatao/ddf

[33mcommit 05319b246620b26db3216008ec00b522aa26b24a[m
Merge: bcbc0ad c6a5fdf
Author: Binh Han <bhan@adatao.com>
Date:   Sat Sep 13 17:01:31 2014 -0700

    Merged in bhan-fix (pull request #144)
    
    Vector executors

[33mcommit c6a5fdf3ef8b3e8ef4f7f0091536edfc1caaba41[m
Merge: b127341 bcbc0ad
Author: bhan <bhan@adatao.com>
Date:   Sat Sep 13 16:56:25 2014 -0700

    Merge branch 'master' of https://bitbucket.org/adatao/ddf into bhan-fix

[33mcommit b12734198120ba4b515c45f429cf274ff266c4d4[m
Author: bhan <bhan@adatao.com>
Date:   Sat Sep 13 16:55:15 2014 -0700

    tested

[33mcommit 2186697ae3dfb8d6d687c802b00ea9292c62c14c[m
Author: bhan <bhan@adatao.com>
Date:   Sat Sep 13 16:54:26 2014 -0700

    Executors to compute vector stats

[33mcommit f83d803951ef6177a17acec7590e864150d92bf5[m
Author: huandao0812 <huan@adatau.com>
Date:   Sat Sep 13 14:09:58 2014 +0700

    throw exception when needed, do not return null for executor
    
    without very good reason

[33mcommit 1a33735b5247ad508d314244f4331b99949bf2ad[m
Merge: 6630e27 bcbc0ad
Author: huandao0812 <huan@adatau.com>
Date:   Sat Sep 13 13:20:48 2014 +0700

    Merge branch 'master' of https://bitbucket.org/adatao/ddf

[33mcommit 6630e279bc92192e9869732d2f08b309409dfc6f[m
Author: huandao0812 <huan@adatau.com>
Date:   Sat Sep 13 12:24:56 2014 +0700

    fix build_all_local

[33mcommit 444f8d3653915d2552fdc30d1db9a3236cb22a53[m
Author: Pham Nam Long <longpham@adatao.com>
Date:   Fri Sep 12 03:43:10 2014 +0000

    [wip] running on yarn

[33mcommit bcbc0ad15d7404c1355dcacf3fc8a958ead04061[m
Merge: 1a80e47 3dc00ff
Author: Binh Han <bhan@adatao.com>
Date:   Wed Sep 10 21:36:04 2014 -0700

    Merged bhan-fix into master

[33mcommit 3dc00ff75ca9682e8317046bd9fd3d1d44a24fdd[m
Author: bhan <bhan@adatao.com>
Date:   Wed Sep 10 21:30:11 2014 -0700

    cleanup

[33mcommit 1a80e479c112aa36aec2ec4a386912a31916c465[m
Merge: 135ab84 24bcd1d
Author: khangpham <khangpham@adatau.com>
Date:   Thu Sep 11 10:13:49 2014 +0700

    Merged in ddf-FEX1 (pull request #141)
    
    [WIP] new feature extraction design for DDF

[33mcommit 24bcd1d7ad120ac3b6d0a15fade5b6d088f0f55c[m
Author: khangich <khangich@gmail.com>
Date:   Thu Sep 11 10:12:50 2014 +0700

    remvoe print

[33mcommit d4f3db7c779ab358321096c9722a90504e19b7d3[m
Author: khangich <khangich@gmail.com>
Date:   Wed Sep 10 17:36:57 2014 +0700

    enable unittsts

[33mcommit da20963fa6969231201e92ff5d4a8da2b48a420a[m
Author: khangich <khangich@gmail.com>
Date:   Wed Sep 10 17:33:34 2014 +0700

    run dummy coding on selected columns only

[33mcommit c596c09346446620f7412fba8d4a3d807df9d614[m
Author: khangich <khangich@gmail.com>
Date:   Wed Sep 10 11:08:41 2014 +0700

    refactor model

[33mcommit a0710d621c1e441e27a1b3a39d7d1f00a75c2c81[m
Author: root <root@ip-10-159-9-79.ec2.internal>
Date:   Wed Sep 10 02:35:24 2014 +0000

    fix build and deploy

[33mcommit 3d587c24ec224259aec1d648b417f78f5953551c[m
Author: khangich <khangich@gmail.com>
Date:   Wed Sep 10 08:59:03 2014 +0700

    pass regressionsuite test

[33mcommit 20443f179b9a3e14f288fd9affe2ba2633b5543f[m
Author: root <root@ip-10-159-9-79.ec2.internal>
Date:   Tue Sep 9 09:00:01 2014 +0000

    wip running on yarn

[33mcommit b54d2f0b45f81a4255e1862aa17389daae39f600[m
Author: khangich <khangich@gmail.com>
Date:   Tue Sep 9 14:03:48 2014 +0700

    remove nFeatuers

[33mcommit 135ab8446b933282e7c8ab50b5cbc54ac832e1bc[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Sep 9 09:37:41 2014 +0700

    add script to build both opensource & master on local

[33mcommit da5b55e6a2daf330bf0462eed62585969c16149f[m
Author: khangich <khangich@gmail.com>
Date:   Mon Sep 8 17:51:48 2014 +0700

    pass unit test

[33mcommit f31995af3cfb70b7bcb88b7cb8b92d18e78ec103[m
Author: khangich <khangich@gmail.com>
Date:   Mon Sep 8 17:51:33 2014 +0700

    implement dummy coding in transformationHandler using TP

[33mcommit bebce53d184d75be4af4a78706ba5d8175b17d86[m
Merge: 6268013 2189180
Author: khangich <khangich@gmail.com>
Date:   Mon Sep 8 10:56:16 2014 +0700

    Merge branch 'ddf-FEX1' into dummyCoding-FEX1

[33mcommit 62680130853df1dbc1b81cafbd75c27381ef231e[m
Author: khangich <khangich@gmail.com>
Date:   Mon Sep 8 10:50:57 2014 +0700

    update dummy coding

[33mcommit 2189180a7b26ab6da7c9d897dff6f0dc572fcfaa[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Sep 8 10:48:42 2014 +0700

    remove com.adatao.spark.ddf.DDF and MLFacade

[33mcommit 7ab18ef3a56f9a48a147a57825f6c6342e00d8b8[m
Merge: d1cff41 c85bb77
Author: Binh Han <bhan@adatao.com>
Date:   Fri Sep 5 00:20:28 2014 -0700

    Merged in bhan-fix (pull request #142)
    
    Make wildcard work in aggregate function

[33mcommit c85bb771e631f06bc4b7fe4a7108af8930fad133[m
Merge: 5031eb6 d1cff41
Author: bhan <bhan@adatao.com>
Date:   Fri Sep 5 00:10:33 2014 -0700

    Merge branch 'master' of https://bitbucket.org/adatao/ddf into bhan-fix

[33mcommit 5031eb6c9b31bf89b747eb6b1a233eacbd2db36d[m
Author: bhan <bhan@adatao.com>
Date:   Fri Sep 5 00:10:04 2014 -0700

    make wildcard work in aggregate function

[33mcommit 475f790be72e7aa69622eb185b6d4e41f36a7b2b[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Sep 4 18:29:08 2014 -0700

    [WIP] new feature extraction design for DDF
    
    test passed

[33mcommit d1cff41cc30a2f161055ea1ecc8df2d126ad60ae[m
Merge: 956314a 4658f29
Author: Binh Han <bhan@adatao.com>
Date:   Wed Sep 3 18:53:39 2014 -0700

    Merged in bhan-fix (pull request #140)
    
    TestMissingDataHandling fixed

[33mcommit 4658f29387405ee96e0a7124f803ff8f078c0b97[m
Author: bhan <bhan@adatao.com>
Date:   Wed Sep 3 18:50:21 2014 -0700

    TestMissingDataHandling fixed

[33mcommit cd9247b9af01a0b06cf169352b3b912ccbbb1d7f[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Sep 3 11:46:55 2014 -0700

    rootbuild for 1.1.0

[33mcommit 956314adc19c38e542c74a97811dc44aca670578[m
Merge: a7c500d 7408abb
Author: Binh Han <bhan@adatao.com>
Date:   Thu Aug 28 14:33:35 2014 -0700

    Merged in als (pull request #138)
    
    ALS for pa-ddf

[33mcommit 7408abb7320c8dfe388ca7e26b7f86128dab535e[m
Author: bhan <bhan@adatao.com>
Date:   Thu Aug 28 12:53:50 2014 -0700

    avoid unnecessary projection

[33mcommit 2bc41e7dbe2c77cdf00197ae59c404d66f1e5de3[m
Merge: 8e5559f a7c500d
Author: bhan <bhan@adatao.com>
Date:   Wed Aug 27 17:36:30 2014 -0700

    Merge branch 'master' of https://bitbucket.org/adatao/ddf into als

[33mcommit a7c500def17ff0bf2222fa7d72bed453d269ab8d[m
Author: bhan <bhan@adatao.com>
Date:   Wed Aug 27 17:29:38 2014 -0700

    remove core, spark

[33mcommit 8e5559f7ade83faf8b67bebe1de4fbd90ab8ef02[m
Author: bhan <bhan@adatao.com>
Date:   Wed Aug 27 17:28:52 2014 -0700

    remove core, spark

[33mcommit a4faa57b50f559b10c459495fc77c8e8f73d1136[m
Merge: 2d7f259 5330e80
Author: bhan <bhan@adatao.com>
Date:   Wed Aug 27 17:24:20 2014 -0700

    Merge branch 'master' of https://bitbucket.org/adatao/ddf into als

[33mcommit 5330e80237c77ca4e8bdec5b07e8db2554bc5905[m
Merge: 3af34e1 ecea41a
Author: khangich <khangich@gmail.com>
Date:   Wed Aug 27 17:11:13 2014 -0700

    resolve conflict

[33mcommit ecea41aa797aa9801632f654b3ff786cfe678e85[m
Merge: d17a44d c11a6c7
Author: khangich <khangich@gmail.com>
Date:   Wed Aug 27 16:54:28 2014 -0700

    Merge branch 'ddf-enterprise' of bitbucket.org:adatao/ddf into ddf-enterprise

[33mcommit 2d7f259db1eff28c8c6201521bb26d0c7a1ebcec[m
Author: bhan <bhan@adatao.com>
Date:   Tue Aug 26 18:59:46 2014 -0700

    set als params

[33mcommit 7345cc7cb90c536923cbafd35bf95e1e8443114e[m
Author: bhan <bhan@adatao.com>
Date:   Tue Aug 26 18:15:04 2014 -0700

    compute rmse

[33mcommit c11a6c7c61c2364b577f4932d0d384280d48177e[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Aug 26 17:21:49 2014 -0700

    add comment

[33mcommit a78c321a928a863e489770514991323a426c2e5f[m
Author: bhan <bhan@adatao.com>
Date:   Tue Aug 26 15:30:47 2014 -0700

    tested

[33mcommit 137fa187af323e8d7831ca4e139842dab1ac163e[m
Merge: 955cca5 bba017c
Author: bhan <bhan@adatao.com>
Date:   Tue Aug 26 15:09:50 2014 -0700

    Merge branch 'ddf-enterprise' of https://bitbucket.org/adatao/ddf into als

[33mcommit 955cca5494bcb8c1e3d643ceee053168855704f0[m
Author: bhan <bhan@adatao.com>
Date:   Tue Aug 26 15:07:35 2014 -0700

    als on ddf-pa impl

[33mcommit bba017c73fcf59b76792731ca54f2b96a7d23181[m
Author: root <root@ip-10-230-12-2.ec2.internal>
Date:   Mon Aug 25 04:41:08 2014 +0000

    add bin/build-all.sh to build both opensource and enterprise

[33mcommit a537691cb73af1628678c4b804f699b6618c5cb9[m
Author: bhan <bhan@adatao.com>
Date:   Fri Aug 22 17:45:47 2014 -0700

    fix typo

[33mcommit d17a44d567072a18933f1e0ff054c2d6df87d522[m
Merge: fabac6c 623e63e
Author: khangich <khangich@gmail.com>
Date:   Fri Aug 22 16:45:30 2014 -0700

    Merge branch 'ddf-enterprise' of bitbucket.org:adatao/ddf into ddf-enterprise

[33mcommit 07d2bf125a3a88969c6afefac277285f817cbd7a[m
Author: bhan <bhan@adatao.com>
Date:   Fri Aug 22 16:13:27 2014 -0700

    als executor

[33mcommit 61206793ea523b3bd48d68ca80526823cbd7be8e[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Aug 21 11:10:26 2014 -0700

    fix tests

[33mcommit 858ddc89833ecadc22dc0566f0c579e041f60910[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Aug 18 23:37:46 2014 -0700

    ignore 1 kmeans test

[33mcommit 9d283d9af575cff8a4371117bd0487e2032fb74d[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Aug 18 22:56:56 2014 -0700

    remove comment out

[33mcommit d939c6c6334cb0892b535b596e0a5085e89689a7[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Aug 18 22:56:08 2014 -0700

    remove comment out

[33mcommit 23352938ed54c4cc254fde13289cebe2d21f7f04[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Aug 18 22:54:25 2014 -0700

    remove custom serializer for Vector, DenseVector

[33mcommit ac56fe87286f6b586404ab8e11a9bd3614caa625[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Aug 18 20:05:50 2014 -0700

    printout KmeansModel

[33mcommit 9ff0e0648c1184526156a3d6ce5963fed81eea56[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Aug 18 19:59:48 2014 -0700

    fix Kmeans executor

[33mcommit 8f02d09320a4d4db411ed19bfa9540114fe2f9e8[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Aug 18 19:55:38 2014 -0700

    printout model

[33mcommit 4ee2bc9334f00c523de03e28565c0df4f847222b[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Aug 18 19:52:24 2014 -0700

    register custom typeAdapter in standardGson

[33mcommit 23e3f17c22998f92cf32f20a1bc4f86ca8baba43[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Aug 18 19:32:00 2014 -0700

    register VectorDeserializer

[33mcommit bb4a7f7cc4eecc38e77b9a78fbbc9dd7c855a889[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Aug 18 19:28:49 2014 -0700

    add custom deserializer for Vector

[33mcommit 913be67cce9de16db48e4e911598dc088329518f[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Aug 18 19:18:28 2014 -0700

    add InstanceCreator for Vector

[33mcommit 8f3e49cebe37e867453647a4b1a8f992d251836d[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Aug 18 19:01:39 2014 -0700

    add custom deserializer for KMeansModel

[33mcommit 71f321ad2b69c18815139a4a18be2da016c8418d[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Aug 18 18:18:07 2014 -0700

    fix SampleDataFrameSuite

[33mcommit f8ac49155f75466b264cefec33a249572517d52d[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Aug 18 17:00:33 2014 -0700

    don't ignore test in TransformNativeRserveSuite

[33mcommit fabac6ca563b910d8692af92c8f63b7fee92eaa9[m
Merge: a3af82f 89f9e19
Author: khangich <khangich@gmail.com>
Date:   Mon Aug 18 14:58:01 2014 -0700

    Merge branch 'ddf-enterprise' of bitbucket.org:adatao/ddf into ddf-enterprise

[33mcommit 042936f476cad5e3f3d92a04e607ac93f1958bf2[m
Merge: 513c7c2 89f9e19
Author: huandao0812 <huan@adatau.com>
Date:   Mon Aug 18 12:29:36 2014 -0700

    Merge branch 'ddf-enterprise' of https://bitbucket.org/adatao/ddf into ddf-sparksql
    
    Conflicts:
    	project/RootBuild.scala

[33mcommit 623e63ed9514566fafea64558842fd1765ea63f4[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Aug 18 12:20:02 2014 -0700

    fix error message

[33mcommit 89f9e192f05d2eb81961564359a45fcd4728babf[m
Author: huandao0812 <huan@adatau.com>
Date:   Sat Aug 16 10:37:11 2014 -0700

    TransformHive return informative error message

[33mcommit a3af82f670c53f94415623e493ab3f8801a0d15c[m
Merge: 87eb4e5 7b8dee5
Author: khangich <khangich@gmail.com>
Date:   Sat Aug 16 08:48:53 2014 -0700

    Merge branch 'ddf-enterprise' of bitbucket.org:adatao/ddf into ddf-enterprise

[33mcommit 7b8dee5fb7529229128da93e9e6fef6e8e92eb9f[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Aug 14 18:45:46 2014 -0700

    GetDDF return meaningful error message

[33mcommit 87eb4e510e1292910143979f1c52c73c44d50d18[m
Merge: dd2a169 997515b
Author: khangich <khangich@gmail.com>
Date:   Wed Aug 13 08:42:47 2014 -0700

    Merge branch 'ddf-enterprise' of bitbucket.org:adatao/ddf into ddf-enterprise

[33mcommit 997515bcdb2f0357322ff34fdf61241c58ab209b[m
Author: root <root@ip-10-180-186-101.ec2.internal>
Date:   Tue Aug 12 22:50:46 2014 +0000

    fix start-pa-server.sh

[33mcommit dd2a16915822e084af360a223194bbfe9f7ac47a[m
Merge: 19be44f 72a14f0
Author: khangich <khangich@gmail.com>
Date:   Tue Aug 12 15:48:48 2014 -0700

    Merge branch 'ddf-enterprise' of bitbucket.org:adatao/ddf into ddf-enterprise

[33mcommit 72a14f00732e18c1fe0bdfb801be389a5bf51bff[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Aug 12 15:46:53 2014 -0700

    fix DDF_SPARK_JAR

[33mcommit 3853c2ee19c5ae093a56b55986ce3d6401611dae[m
Author: bhan <bhan@adatao.com>
Date:   Tue Aug 12 15:32:27 2014 -0700

    fixing pa test

[33mcommit 6cd5d4391c68f6b176e702418e5bd910f6b01088[m
Author: bhan <bhan@adatao.com>
Date:   Tue Aug 12 15:32:02 2014 -0700

    remove core

[33mcommit 19be44f9c1d671037aac324fc02d90fefb840991[m
Merge: e61ba74 a495d96
Author: khangich <khangich@gmail.com>
Date:   Tue Aug 12 15:10:00 2014 -0700

    Merge branch 'ddf-enterprise' of bitbucket.org:adatao/ddf into ddf-enterprise

[33mcommit a495d96fe9574353a804b7c90c6f7dc49d420b0c[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Aug 12 13:06:15 2014 -0700

    fix log-4j

[33mcommit e61ba74ab1ecb3def9ab6da32ace63772a52bc3e[m
Merge: f2ad16e 1a6427d
Author: khangich <khangich@gmail.com>
Date:   Tue Aug 12 12:16:33 2014 -0700

    Merge branch 'ddf-enterprise' of bitbucket.org:adatao/ddf into ddf-enterprise

[33mcommit 1a6427dc1e156836fc72e95b4187cb9d0ad52bb8[m
Author: root <root@ip-10-16-145-33.ec2.internal>
Date:   Tue Aug 12 18:56:21 2014 +0000

    fix kryo in pa-env

[33mcommit 87340573cd1606b6a9908d212c76ef18ce16a4eb[m
Author: root <root@ip-10-180-186-101.ec2.internal>
Date:   Tue Aug 12 18:38:39 2014 +0000

    fix pa-env.sh

[33mcommit f2ad16ed2c8bad39cf8628ab9913d434d7592359[m
Merge: c76fde1 6a494e8
Author: khangich <khangich@gmail.com>
Date:   Tue Aug 12 11:36:40 2014 -0700

    Merge branch 'ddf-enterprise' of bitbucket.org:adatao/ddf into ddf-enterprise

[33mcommit c76fde14d9ba8a0918536f6eb923e68947ae0ba4[m
Author: khangich <khangich@gmail.com>
Date:   Tue Aug 12 11:30:48 2014 -0700

    remove log file

[33mcommit 6a494e8d9dff49c4490f886c37175f2908464701[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Aug 12 10:02:10 2014 -0700

    fix Kryo

[33mcommit ff6c8f583aad767f84106b21e041e42fdfdef35a[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Aug 12 09:57:33 2014 -0700

    delete examples & contrib in RootBuild.scala

[33mcommit 513c7c26c9ebe30c52dc2f0a9603fe66f1fabf87[m
Merge: 450a65d 524986e
Author: root <root@ip-10-180-186-101.ec2.internal>
Date:   Tue Aug 12 06:49:50 2014 +0000

    Merge branch 'ddf-sparksql' of https://bitbucket.org/adatao/ddf into ddf-sparksql

[33mcommit 450a65dfeb3258c2079b5529c9d2ef371fb19533[m
Author: root <root@ip-10-180-186-101.ec2.internal>
Date:   Tue Aug 12 06:49:05 2014 +0000

    add ddf-log4j.properties

[33mcommit 0044c79e819bea03c0cf260c70045fb7fd68e1fc[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Aug 11 23:41:07 2014 -0700

    revert ddf to spark 0.9

[33mcommit 524986e184754b129b279d7d95115ae320772adc[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Aug 11 16:56:06 2014 -0700

    remove examples proj in pom.xml

[33mcommit 355642236daf944b99d47ace220a1eaf4a5b13d6[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Aug 11 16:50:05 2014 -0700

    remove examples, contrib project

[33mcommit 2c1e751d9381448f2efba56d024358bb2e3d0c3e[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Aug 11 13:15:48 2014 -0700

    [WIP] able to compile

[33mcommit ffbff1031812590051994739a98d490178689366[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Aug 11 12:20:13 2014 -0700

    Clean up PA

[33mcommit 588f66add6ef8f24c61e963a2bb0e6475892939d[m
Merge: e392983 97590ad
Author: huandao0812 <huan@adatau.com>
Date:   Mon Aug 4 16:30:02 2014 +0700

    Merged in refactor-ml (pull request #133)
    
    Move ML implementation to spark

[33mcommit 97590ade62368ad3c094c21a51b7c28a09f08e39[m
Merge: 1666b3d e392983
Author: huandao0812 <huan@adatau.com>
Date:   Mon Aug 4 16:28:53 2014 +0700

    Merge branch 'ddf-enterprise' into refactor-ml
    
    Conflicts:
    	pa/src/test/scala/com/adatao/pa/spark/execution/RegressionSuite.scala

[33mcommit 1666b3d62efa0fe309a74865b95ace2914eac32d[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Aug 4 15:47:15 2014 +0700

    clean up code

[33mcommit e7af6494836a184fe712786edf8df7113b3d1758[m
Merge: d4d434a c5f88d6
Author: huandao0812 <huan@adatau.com>
Date:   Mon Aug 4 11:45:27 2014 +0700

    Merge branch 'ddh-fix-test' into refactor-ml
    
    Conflicts:
    	pa/src/main/scala/com/adatao/pa/spark/execution/LogisticRegression.scala
    	pa/src/test/scala/com/adatao/pa/spark/execution/CrossValidationSuite.scala

[33mcommit d4d434aae6e4d1794ce34a22f9c1d81b7829223c[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Aug 4 10:53:36 2014 +0700

    Merge branch 'ddf-enterprise' into refactor-ml
    
    Conflicts:
    pa/src/test/scala/com/adatao/pa/spark/execution/RegressionSuite.scala

[33mcommit e3929838bc6bbd932df0a81303e3af48eb915173[m
Merge: 2b4a94f 8a38639
Author: huandao0812 <huan@adatau.com>
Date:   Mon Aug 4 10:41:28 2014 +0700

    Merged in ddf-fix-namespace (pull request #132)
    
    ddf-enterprise work with ddf-opensource

[33mcommit 9856a59a85915f785575d5547cfa5e5c1f2cb337[m
Author: khangich <khangich@gmail.com>
Date:   Sun Aug 3 09:15:05 2014 +0700

    cleanup

[33mcommit 5e8bb144f41987f3bc7c395e90c3b7a8b030c96b[m
Author: khangich <khangich@gmail.com>
Date:   Sun Aug 3 09:04:24 2014 +0700

    remove instrumentModel

[33mcommit 135cc6f2ce05734d153ffc54b566797331ea56e9[m
Author: khangich <khangich@gmail.com>
Date:   Sat Aug 2 23:55:40 2014 +0700

    cleanup

[33mcommit 6038e19cb1c8491e500de6ef924d3d1544ea6ddd[m
Author: khangich <khangich@gmail.com>
Date:   Sat Aug 2 23:12:52 2014 +0700

    update dummy

[33mcommit 41fc4ba3b8a80b64ffb54cee91a9369e932fea33[m
Author: khangich <khangich@gmail.com>
Date:   Sat Aug 2 23:05:31 2014 +0700

    remove LossFunction

[33mcommit e7f2a7ca8ee8b74cc2c1010c158a2e4587f584e4[m
Author: khangich <khangich@gmail.com>
Date:   Sat Aug 2 22:48:14 2014 +0700

    remove loss function at pa

[33mcommit 2bb2881b3151affc70cd2ec034fba9832b0cfb81[m
Author: khangich <khangich@gmail.com>
Date:   Sat Aug 2 22:26:59 2014 +0700

    remove xCols, yCOl

[33mcommit 0ffe38e084399151886aa1b0fad8d1ad6c17563f[m
Author: khangich <khangich@gmail.com>
Date:   Sat Aug 2 21:25:10 2014 +0700

    pass unit tests

[33mcommit f6ce822fd3c777b8b16061d078838a5a95c0871b[m
Author: khangich <khangich@gmail.com>
Date:   Sat Aug 2 17:13:23 2014 +0700

    pass regressionsuite test

[33mcommit 78ca452c8548544a0fc118d21d721fd26cee8112[m
Author: khangich <khangich@gmail.com>
Date:   Sat Aug 2 16:39:43 2014 +0700

    compiled

[33mcommit c5f88d686c35dae99127c373520b6cd6e2d75951[m
Merge: f31aa5b 1704535
Author: bhan <bhan@adatao.com>
Date:   Fri Aug 1 13:01:56 2014 -0700

    Merge branch 'ddh-fix-test' of https://bitbucket.org/adatao/ddf into ddh-fix-test
    
    Conflicts:
    	pa/src/main/scala/com/adatao/pa/spark/execution/LogisticRegression.scala

[33mcommit f31aa5ba3db542111afe213653aaf20a76e0116b[m
Author: bhan <bhan@adatao.com>
Date:   Fri Aug 1 12:58:00 2014 -0700

    refactoring

[33mcommit 170453529a49a041cfd68c68f939abe98d2869c7[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Aug 1 15:18:03 2014 +0700

    CrossValidationSuite & MLMetricsSuite passed

[33mcommit 09a71ca368fe8a00a56870d2c283f51bd120190c[m
Author: khangich <khangich@gmail.com>
Date:   Thu Jul 31 22:06:30 2014 +0700

    temporary disable TransformRServe

[33mcommit 542925747e840498a4780fa930208a98e71e277b[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Jul 31 16:48:03 2014 +0700

    persistenceID => getName

[33mcommit 4d6ad1144ab941559aa8b030ddeedede43a784d3[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Jul 31 16:42:07 2014 +0700

    fix model.getName in MetricsSuite

[33mcommit 77bab156023556d3104e7cbba34fd5d76b53dfbd[m
Author: khangich <khangich@gmail.com>
Date:   Thu Jul 31 16:27:01 2014 +0700

    typo ddf.ini

[33mcommit 134a7e06e57a93cadc2b2380024885de7e4bb475[m
Merge: 19944ec 9e7107d
Author: huandao0812 <huan@adatau.com>
Date:   Thu Jul 31 15:53:17 2014 +0700

    Merge branch 'ddh-fix-test' of https://bitbucket.org/adatao/ddf into ddh-fix-test

[33mcommit 19944eca1815f0af917556fb7d38f6009331a5a7[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Jul 31 15:53:06 2014 +0700

    debugging ROC

[33mcommit 9e7107d91c160f2d7ab2c35d6c5e9bee3c443886[m
Author: khangich <khangich@gmail.com>
Date:   Thu Jul 31 15:52:21 2014 +0700

    wip pass some unit test in RegressionSuite

[33mcommit b18ae61108acc2faa12e6c5497e52fe92b7d6c20[m
Author: khangich <khangich@gmail.com>
Date:   Thu Jul 31 15:13:54 2014 +0700

    wip

[33mcommit 3af34e1178934e31c45a528c463086bde6b66631[m
Author: root <root@ip-10-16-145-33.ec2.internal>
Date:   Thu Jul 31 07:20:43 2014 +0000

    fix bug in start-pa

[33mcommit 4cfc4ab83221a2a5078a4e1280868f6e1610570e[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Jul 31 14:03:27 2014 +0700

    wip

[33mcommit 8a386398985f9517f60b7d280c98a0cbdd951743[m
Author: khangich <khangich@gmail.com>
Date:   Thu Jul 31 13:38:14 2014 +0700

    disable regressionsuite to isolate

[33mcommit b2d030a13c20ca9eb72762acccd9e85fc64658c6[m
Author: khangich <khangich@gmail.com>
Date:   Thu Jul 31 11:34:45 2014 +0700

    change port 20001 --> 2002

[33mcommit 86bd7fb042ff8a54914487a441fd660d5fdb1754[m
Author: khangich <khangich@gmail.com>
Date:   Thu Jul 31 11:16:58 2014 +0700

    change port 20001 --> 20002

[33mcommit 99829399e6ddded1ced0dce74dd2a40249c97e2f[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Jul 31 11:14:33 2014 +0700

    wip

[33mcommit e2f8657cd589daa437a253d7c39f47a038f37190[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Jul 30 16:50:50 2014 +0700

    fix conflict

[33mcommit 1ec7b230b3cbfff62e06328a6b147303713b20d1[m
Merge: 5669fe5 4f653b3
Author: huandao0812 <huan@adatau.com>
Date:   Wed Jul 30 16:49:05 2014 +0700

    Merge branch 'ddf-fix-namespace' of https://bitbucket.org/adatao/ddf into ddf-fix-namespace
    
    Conflicts:
    	pom.xml

[33mcommit 5669fe58cacfda4a50a22b21a7eb6cb5aed9d145[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Jul 30 16:27:54 2014 +0700

    fix pom.xml & RootBuild

[33mcommit 4f653b3939def139c4904689584c877e916be672[m
Author: khangich <khangich@gmail.com>
Date:   Tue Jul 29 16:06:55 2014 +0700

    remove enterprise, contrib

[33mcommit e8c3eb1b7c76a9a65d06c742b8053b0fa7700cf7[m
Author: khangich <khangich@gmail.com>
Date:   Tue Jul 29 15:57:03 2014 +0700

    remove clients folder

[33mcommit c1c55c6ea0d2d719873be2de5309e4d98ec7b94f[m
Author: khangich <khangich@gmail.com>
Date:   Tue Jul 29 15:56:16 2014 +0700

    cleanup

[33mcommit 1bda905feb95beb66c1a176fe7fcdbec20fe5d60[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jul 29 14:59:44 2014 +0700

    add junit

[33mcommit 37cef00d789cdf8f223d8a9426ab3833fc843638[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jul 29 13:43:28 2014 +0700

    fix namespace

[33mcommit 09614037ed9b938167fa941c429aafef071302e7[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jul 29 10:50:28 2014 +0700

    fix namespace

[33mcommit 3e1a028afe27893512f34f9b1941197eae853a83[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Jul 14 16:02:30 2014 +0700

    clean up code

[33mcommit 2459d36be51ee19e7f87cc6892ebbac4635fef10[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Jul 14 12:45:18 2014 +0700

    fix namespace dependency on ddf-opensource

[33mcommit 2b4a94f1552ef19ac54bdb4dab8f30be90412db2[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Jul 14 10:08:28 2014 +0700

    put KryoRegistrator to ddf-enterprise

[33mcommit 221205073492e5393e575e55a76151c51bb04274[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Jul 14 09:45:28 2014 +0700

    default SPARK_YARN=TRUE

[33mcommit 19e9f62788da58c532d2efc439af3c4d4be35a50[m
Merge: c022ceb 922a6f8
Author: Binh Han <bhan@adatao.com>
Date:   Fri Jul 11 00:52:01 2014 -0700

    Merged in bhan-refactor (pull request #131)
    
    pandas-like groupBy

[33mcommit 922a6f8a70a6eab8a92afa498039051620090658[m
Author: bhan <bhan@adatao.com>
Date:   Fri Jul 11 00:46:44 2014 -0700

    testing groupBy pa

[33mcommit 6a99b0e6ddc5e8c7bfc732d6793e819b8219e7e6[m
Author: bhan <bhan@adatao.com>
Date:   Fri Jul 11 00:37:29 2014 -0700

    tested

[33mcommit f3fc6ab96488272d53eb6f6627ac1f6cb8a3ceb9[m
Author: bhan <bhan@adatao.com>
Date:   Fri Jul 11 00:00:58 2014 -0700

    refactor AggregationHandler, add pandas groupby

[33mcommit c832b7002a9cded15b62caa5fde7ed1f4b3141f2[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Jul 11 09:26:48 2014 +0700

    add posfix -mesos

[33mcommit c022ceb21be88cf3c92964db463d30ccc7a08935[m
Merge: de35f1e 334d632
Author: Binh Han <bhan@adatao.com>
Date:   Thu Jul 10 14:16:09 2014 -0700

    Merged in bhan-refactor (pull request #130)
    
    refactor views

[33mcommit 334d632c20194f0202c7ed75566f0bbf01a3907d[m
Author: bhan <bhan@adatao.com>
Date:   Thu Jul 10 14:04:18 2014 -0700

    refactor views

[33mcommit b9863f70253e0a689642897495f2fc6b4fe85764[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Jul 9 16:45:58 2014 +0700

    delete proj enterprise
    
    rename spark -> spark_adatao

[33mcommit bfcccfba074a0e3b396f181d150d6ab57c9dc6de[m
Author: khangich <khangich@gmail.com>
Date:   Wed Jul 9 16:16:39 2014 +0700

    spark adatao engine

[33mcommit 0036e17a9d8c07b866928bdeb6ae007f2f79bb14[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Jul 9 15:55:37 2014 +0700

    fix RootBuild.scala

[33mcommit 1466d9307cb2da32a6e2e15805c6755d9b30fd89[m
Author: khangich <khangich@gmail.com>
Date:   Wed Jul 9 08:30:17 2014 +0700

    change RootBuild

[33mcommit 7b2ee9643dc7313b7bf9ae887f5ecc21ab9dbe34[m
Author: khangich <khangich@gmail.com>
Date:   Tue Jul 8 22:41:38 2014 +0700

    wip enterprise

[33mcommit de35f1e0a86e9623285ba7d5fe93ab14f8398fa5[m
Merge: 7ce913e c190c51
Author: khangpham <khangpham@adatau.com>
Date:   Mon Jul 7 13:12:18 2014 +0700

    Merged in ddf-branch-0.9 (pull request #129)
    
    Merge to master

[33mcommit c190c51197198b1c3908b3ab648e8da90513508a[m
Merge: 079f318 7dd69f0
Author: khangpham <khangpham@adatau.com>
Date:   Fri Jul 4 16:58:20 2014 +0700

    Merged in scala-pa-client (pull request #128)
    
    Merge scala client and some new features

[33mcommit 7dd69f0f801b3bbb156a44617d45d7a39d6d870f[m
Author: khangich <khangich@gmail.com>
Date:   Tue Jul 1 22:27:54 2014 +0700

    remove limit, remove setDDFName

[33mcommit 27fb5ffdda45594e0d5daabbda45a2d43f5ba6db[m
Merge: dcb015a 60c94ca
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jul 1 22:11:09 2014 +0700

    Merged in test-scala-connect (pull request #127)
    
    add method connect

[33mcommit 60c94ca445828aa39324f7b11b8d1ed45f3f3a3a[m
Author: khangich <khangich@gmail.com>
Date:   Tue Jul 1 21:47:35 2014 +0700

    run smoke test on small ddf

[33mcommit 9c7d4cb4b6ca0db414b71c7763206af438b8b510[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jul 1 21:33:52 2014 +0700

    add method connect
    
    val manager = DDFManager.get(“spark”)
    manager.connect(“pa2.adatao.com”)

[33mcommit d70f24fd36e76941c4548d25275f21925a4173e2[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jul 1 21:04:05 2014 +0700

    fix connect()

[33mcommit a1d1d7f358824bd77c0f486c0b08cea50a8f90d3[m
Author: khangich <khangich@gmail.com>
Date:   Tue Jul 1 19:04:16 2014 +0700

    add connect method

[33mcommit dcb015ad363c0b680109d633150dc719062b9aa8[m
Author: khangich <khangich@gmail.com>
Date:   Tue Jul 1 16:43:10 2014 +0700

    private var in DDF

[33mcommit a13765a80f457eec5d1454fcb3a679bbb95d18b6[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jul 1 15:21:47 2014 +0700

    add backup cluster for DDF demo

[33mcommit 6c69f4c43df0064967856dce44e9ee250e7b8f8d[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jul 1 12:02:56 2014 +0700

    update test script

[33mcommit 538d7152a9d62f5b083c6dde3b42d8ba03576f16[m
Merge: e0ead55 f5e039f
Author: khangich <khangich@gmail.com>
Date:   Tue Jul 1 11:54:20 2014 +0700

    Merge branch 'scala-pa-client' of bitbucket.org:adatao/ddf into scala-pa-client

[33mcommit e0ead552f4485f1203371fe292d4abbf89ba3962[m
Author: khangich <khangich@gmail.com>
Date:   Tue Jul 1 11:53:42 2014 +0700

    reindent fetchRows

[33mcommit f5e039f9ab948a8321a8705427e085be6e3c07ad[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jul 1 11:38:41 2014 +0700

    update paTest

[33mcommit 28ddc1bc040e7777c91bce28bae8ca0c98c4fa65[m
Author: khangich <khangich@gmail.com>
Date:   Tue Jul 1 11:34:52 2014 +0700

    reindent fetchrows

[33mcommit 58df0268af5e3419cfdf79e95916a6074d002fce[m
Merge: cfd5f58 5522b4b
Author: khangich <khangich@gmail.com>
Date:   Tue Jul 1 11:33:17 2014 +0700

    Merge branch 'scala-pa-client' of bitbucket.org:adatao/ddf into scala-pa-client

[33mcommit cfd5f58f91c5039a923ee0c571be0a2da3591e01[m
Author: khangich <khangich@gmail.com>
Date:   Tue Jul 1 11:33:10 2014 +0700

    reindent fetchrows

[33mcommit 5522b4b33ff44ad0360295c55e15f6a7ef7a9e40[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jul 1 11:29:26 2014 +0700

    fix project, update test script

[33mcommit 5c35b1ae35d8264449aa5655e77a990abfe64cb5[m
Author: khangich <khangich@gmail.com>
Date:   Tue Jul 1 10:56:37 2014 +0700

    re indent fivenum

[33mcommit 3448937b59940a2fbfd48afdc72a5925983bd183[m
Author: khangich <khangich@gmail.com>
Date:   Tue Jul 1 10:32:36 2014 +0700

    use = instead of ==

[33mcommit 20f79f7ed7981a394af51ed61dda3f9a9efcf229[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon Jun 30 17:29:00 2014 -0700

    uncached nrow

[33mcommit 060b3e95192a8d7d70c207710fd810a2c5ab4eb3[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jul 1 05:44:46 2014 +0700

    add test

[33mcommit 8f9642eea57ac7463eba5fbb31b7e771fa1e8fe8[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jul 1 05:29:41 2014 +0700

    return namespace = "adatao"

[33mcommit 3bf11e6ee093e635cae5beb2d07c80a08ab4f672[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jul 1 03:22:15 2014 +0700

    default number of iteration to 1

[33mcommit 7aeec4d1b989206550af1fe8b4654adb277308cc[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jul 1 03:14:57 2014 +0700

    update LogisticRegression to use default value

[33mcommit 195b6281dfc58f260b90dd01eff4b45abbfac883[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jul 1 02:56:27 2014 +0700

    DDFManager does not fall over to pa4.adatao.com
    
    fix transform mutable for Pa scala client

[33mcommit e34567a1af8665aa269435a4091b11d844dcd339[m
Author: khangich <khangich@gmail.com>
Date:   Tue Jul 1 02:18:54 2014 +0700

    transient

[33mcommit dca3ddfec891d00c1b70e51a22ebb4cc95a100da[m
Merge: 533ab2c 72beef5
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jul 1 01:56:57 2014 +0700

    Merge branch 'scala-pa-client' of https://bitbucket.org/adatao/DDF into scala-pa-client

[33mcommit 72beef53ad6220b7e1aa598c3467489e1cd929e3[m
Author: khangich <khangich@gmail.com>
Date:   Tue Jul 1 01:30:31 2014 +0700

    change transform -> transformHive

[33mcommit 533ab2c977335c53719308d9c8115ebb1b0142b0[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jul 1 01:28:49 2014 +0700

    getDDF to use full uri

[33mcommit 2868c9328eb31fbd9aec47ec64392da7b214bceb[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jul 1 01:11:23 2014 +0700

    dont use uri in getUri

[33mcommit 14f3e840ce68caac2e789e9a02e2c864b3f2609f[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jul 1 00:42:51 2014 +0700

    set namespace

[33mcommit 3c9654718dc7c233023642316a02d4ef5ce5b3df[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jul 1 00:22:14 2014 +0700

    move DDF to com.adatao.pa.ddf.spark
    
    pre-import DDFManager when starting console

[33mcommit e9fefd497cd0ba5ecc02aa103e6138e1bd0fa714[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Jun 30 23:29:39 2014 +0700

    uncomment ddf.filter debugging
    
    move DDFManager to com.adatao.pa.ddf.spark.DDFManager

[33mcommit db6dc41bdb453801124fa8e4772f1771c9c3e1f6[m
Merge: 68ff21c 1056227
Author: root <root@ip-10-230-12-76.ec2.internal>
Date:   Mon Jun 30 15:31:30 2014 +0000

    Merge branch 'scala-pa-client' of https://bitbucket.org/adatao/ddf into scala-pa-client

[33mcommit 10562271ca9c4b577838ae8d6fcac7509c6fda8a[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Jun 30 22:16:59 2014 +0700

    ddf project using a string
    
    DDFManager.get(“”spark) use primarily pa2.adatao.com as a major cluster
    and pa4.adatao.com as a backup cluster

[33mcommit 68ff21caccc84722b4980755a8680085d9c34566[m
Author: root <root@ip-10-230-12-76.ec2.internal>
Date:   Mon Jun 30 10:57:06 2014 +0000

    fix RootBuild.scala

[33mcommit 863bbde91de7ce8d58c605d03cdb029d6c58c745[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Jun 30 17:21:01 2014 +0700

    add filter function

[33mcommit a1741672ac365d79eafdf176ba0166a02b151722[m
Author: khangich <khangich@gmail.com>
Date:   Mon Jun 30 17:17:21 2014 +0700

    remove duplidate column

[33mcommit 2ad6d868598b6e317015d680e934e15e8a64fcdd[m
Author: khangich <khangich@gmail.com>
Date:   Mon Jun 30 17:07:06 2014 +0700

    reindent 2

[33mcommit b6e7b6744ab6f48d65b86966f1755f140283f675[m
Author: khangich <khangich@gmail.com>
Date:   Mon Jun 30 16:57:30 2014 +0700

    reindent

[33mcommit e434458301c5a35dfd76b2e21a80c329bb20592e[m
Author: khangich <khangich@gmail.com>
Date:   Mon Jun 30 16:02:23 2014 +0700

    reindent

[33mcommit 34774b112f0a27b235ecdcf72029face558bcc5a[m
Author: khangich <khangich@gmail.com>
Date:   Mon Jun 30 15:50:57 2014 +0700

    refactor

[33mcommit 28bb26e8fd0d997548d52a46ce994584534b5808[m
Author: khangich <khangich@gmail.com>
Date:   Mon Jun 30 15:39:43 2014 +0700

    update topn

[33mcommit d9686b34ed7cf1fcd2ceb59d56355137674f8db8[m
Author: khangich <khangich@gmail.com>
Date:   Mon Jun 30 15:20:41 2014 +0700

    update topn

[33mcommit 5fffe0b67f93509ed5b40809e13305c07df49cba[m
Author: khangich <khangich@gmail.com>
Date:   Mon Jun 30 14:53:29 2014 +0700

    order -> sort, group by fix

[33mcommit 1b28a0751d41bbb7df4507615e117eb10cebad88[m
Author: khangich <khangich@gmail.com>
Date:   Mon Jun 30 14:34:58 2014 +0700

    add topN executor

[33mcommit d8bbe91824890d5e77367a9d01b83ed27c418620[m
Author: khangich <khangich@gmail.com>
Date:   Mon Jun 30 14:30:57 2014 +0700

    add topN

[33mcommit 14480f0602db96c342297c1920575ff7e23184ea[m
Author: khangich <khangich@gmail.com>
Date:   Mon Jun 30 14:06:12 2014 +0700

    add delimiters

[33mcommit d4bf34c911f02e08baa3d5cdda71988c6d5040dd[m
Author: khangich <khangich@gmail.com>
Date:   Mon Jun 30 14:04:03 2014 +0700

    add setNameSpace

[33mcommit 53a00bb2fca56d1d34c8f45e3708885b2a894abb[m
Author: khangich <khangich@gmail.com>
Date:   Mon Jun 30 14:00:39 2014 +0700

    add groupby column in select statement

[33mcommit 5c18d49e72aff6f4b9fbe25046d9b3639cb39e38[m
Author: khangich <khangich@gmail.com>
Date:   Mon Jun 30 13:41:09 2014 +0700

    add namespace

[33mcommit 127919826c67adcd6f64e5e00fdbfccc20870d53[m
Author: khangich <khangich@gmail.com>
Date:   Mon Jun 30 13:33:05 2014 +0700

    change nrow, ncol, aggregation

[33mcommit dc6c218124fca87542cc665c7d89ea3e17cdd27a[m
Merge: 7642c9a 9a77ebe
Author: khangich <khangich@gmail.com>
Date:   Mon Jun 30 13:17:21 2014 +0700

    Merge branch 'scala-pa-client' of bitbucket.org:adatao/ddf into scala-pa-client

[33mcommit 7642c9adb5e557fa42cec36732054f7588c2d49f[m
Author: khangich <khangich@gmail.com>
Date:   Mon Jun 30 13:17:06 2014 +0700

    projectDDF -> project

[33mcommit 9a77ebefdd2b31c2cc4daaaddc2f5909281f0a20[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Jun 30 13:13:27 2014 +0700

    add fiveNum

[33mcommit 91a82e93b8551f2b71fc1defa11ae76c2c83a50b[m
Merge: 7392394 b34855d
Author: khangich <khangich@gmail.com>
Date:   Mon Jun 30 11:19:08 2014 +0700

    Merge branch 'scala-pa-client' of bitbucket.org:adatao/ddf into scala-pa-client

[33mcommit 7392394e13fc5b57837cd1e9f6d9b0dd8b2ee7df[m
Author: khangich <khangich@gmail.com>
Date:   Mon Jun 30 11:18:21 2014 +0700

    update project, setMetaInfo

[33mcommit b34855d4d68c2492e7a977ea949419e7bac7287b[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Jun 30 11:11:14 2014 +0700

    fix CVKFold and CVRandom

[33mcommit edd4ac5651eeb4dbdaf9c6f8fe8d3aa66f399084[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Jun 30 10:49:13 2014 +0700

    sql2dataframe does not substring dcID

[33mcommit 05bab26d64686676b1751a5c0af297494ffa01f3[m
Merge: 0d416d2 57d9d6a
Author: khangich <khangich@gmail.com>
Date:   Mon Jun 30 10:47:33 2014 +0700

    merge and resolve GroupBy conflict

[33mcommit 57d9d6aa4f74195750081f377d0c66768864eb2c[m
Author: khangich <khangich@gmail.com>
Date:   Mon Jun 30 10:41:18 2014 +0700

    add getMetaInfo method in Subset

[33mcommit 0d416d260a877ee29c372146e1e4e56ae149d9d5[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Jun 30 10:40:01 2014 +0700

    PA and DDF use the same dataContainerID

[33mcommit f7e97407081cce3b9c817c9f68b6991943dfa668[m
Merge: 82b24d8 c9919ee
Author: khangich <khangich@gmail.com>
Date:   Mon Jun 30 10:37:53 2014 +0700

    Merge branch 'scala-pa-client' of bitbucket.org:adatao/ddf into scala-pa-client

[33mcommit 82b24d83d8f20d867093c2d1066bdaa398c231e5[m
Author: khangich <khangich@gmail.com>
Date:   Mon Jun 30 10:37:27 2014 +0700

    add project function

[33mcommit c9919ee86b42c3d4e6d914f20ee26c3487a693d7[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Jun 30 10:20:53 2014 +0700

    Utils.dcID2DDFID does not change dcID

[33mcommit 09af882fb881ac1eb7ba414ecebf5747c04e83b5[m
Author: khangich <khangich@gmail.com>
Date:   Mon Jun 30 09:24:20 2014 +0700

    add string to summary

[33mcommit caeb5b05b73035b1293cead88ddc210403daeaf7[m
Author: khangich <khangich@gmail.com>
Date:   Mon Jun 30 09:23:27 2014 +0700

    summary tostring at client

[33mcommit 5901bc4c5cf28c29ba47275fc06837ef6ce9d022[m
Merge: 65da929 55e3f9e
Author: khangpham <khangpham@adatau.com>
Date:   Mon Jun 30 08:59:00 2014 +0700

    Merged in ddf-sparkSummit-binning (pull request #126)
    
    binning on mutable DDF

[33mcommit 55e3f9eeba2887eda5f405f200a08bcd5e634033[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Sun Jun 29 18:31:55 2014 -0700

    mutable binning passed ddf-pa test

[33mcommit 65da929f92dda1b0fbaebcb6080e52d5f7d5eea8[m
Author: khangich <khangich@gmail.com>
Date:   Mon Jun 30 07:46:59 2014 +0700

    add toString

[33mcommit e74cef74bef7d30bf76c96dcc942c3d3078ae64f[m
Merge: 079f318 24bce0f
Author: bhan <binhhan.gt@gmail.com>
Date:   Sun Jun 29 17:03:58 2014 -0700

    Merge branch 'ddf-sparkSummit-binning' of https://bitbucket.org/adatao/DDF into ddf-sparkSummit-binning

[33mcommit 24bce0fc143431f87a5c8fddc9f3a963feaf52f0[m
Author: bhan <bhan@adatao.com>
Date:   Sun Jun 29 16:25:06 2014 -0700

    mutability for binning, ddf-spark test passed

[33mcommit 5fc2ed5d6afe7383269c8a45db83be0e036000ba[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Jun 30 00:51:51 2014 +0700

    fix GetDDF

[33mcommit 876984a2aa1232019c8ffd4a4a01eefe85f1802d[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Jun 30 00:33:06 2014 +0700

    fix addDDF

[33mcommit 3d8641fcd6eaba2ce4b59da52b45e576c60bfb04[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Jun 30 00:03:04 2014 +0700

    fix SetDDFName

[33mcommit b4c18366d3d85a185fd2c6f1085f86ed2699cebc[m
Author: huandao0812 <huan@adatau.com>
Date:   Sun Jun 29 23:52:51 2014 +0700

    SetDDFName and getDDF use DDF's name

[33mcommit a7a51f8ca25915b8cd3bda6db7164d62711c5dd8[m
Author: huandao0812 <huan@adatau.com>
Date:   Sun Jun 29 18:09:33 2014 +0700

    add default constructor for GroupBy

[33mcommit 139c155d700e2cff1fbb0b01b93442a2f1636782[m
Author: huandao0812 <huan@adatau.com>
Date:   Sun Jun 29 18:07:39 2014 +0700

    add groupBy and dropNA

[33mcommit fcd0f7cb0f66562d4567392239a6fea2098350d1[m
Merge: 939e25c 1e3242e
Author: huandao0812 <huan@adatau.com>
Date:   Sun Jun 29 17:39:08 2014 +0700

    Merge branch 'ddf-sparkSummit' into scala-pa-client
    
    Conflicts:
    	pa/src/main/scala/com/adatao/pa/spark/execution/PersistModel.scala

[33mcommit 939e25ccda4a6229638eb66ef0a0adb06531177a[m
Author: huandao0812 <huan@adatau.com>
Date:   Sun Jun 29 17:31:07 2014 +0700

    fix ObjDDF

[33mcommit 1e3242ec745a8c955fd7ccf3039faaed880be0b1[m
Merge: 7b878d1 8a457e9
Author: root <root@ip-10-230-145-212.ec2.internal>
Date:   Sun Jun 29 09:49:46 2014 +0000

    Merge branch 'ddf-sparkSummit' of https://bitbucket.org/adatao/ddf into ddf-sparkSummit

[33mcommit 7b878d1266a27abd019f51fc0bbed788f740747b[m
Author: root <root@ip-10-230-145-212.ec2.internal>
Date:   Sun Jun 29 09:48:06 2014 +0000

    add script to build and start pa server

[33mcommit 8a457e9f282165cb407d86a43115e7a3249ff50a[m
Author: huandao0812 <huan@adatau.com>
Date:   Sun Jun 29 16:43:29 2014 +0700

    LoadModel and PersistModel to use uri

[33mcommit 6ece67c853ec70e4102240664ce4e858f08f1492[m
Author: huandao0812 <huan@adatau.com>
Date:   Sun Jun 29 16:38:30 2014 +0700

    add ObjectDDF to persist model
    
    add setAsFactor and getLevelCounts

[33mcommit 6611e6337c6e7b1326ede64dfc75487dda43d370[m
Author: huandao0812 <huan@adatau.com>
Date:   Sun Jun 29 15:15:49 2014 +0700

    fix SchemaHandler

[33mcommit 94f715a90d43c919cbe2babecd94833d836d6ded[m
Author: huandao0812 <huan@adatau.com>
Date:   Sun Jun 29 15:10:57 2014 +0700

    refractor Schema, DDF

[33mcommit bb281c1d5d7bf9040bfba509b31e1213ec10ce06[m
Author: huandao0812 <huan@adatau.com>
Date:   Sun Jun 29 14:16:38 2014 +0700

    add method for scala PA client

[33mcommit 60d1390dccc8dcaa2ea00e494751d62d09af6c9a[m
Author: huandao0812 <huan@adatau.com>
Date:   Sat Jun 28 20:27:52 2014 +0700

    add log to SparkDDFManager

[33mcommit f2f14012409e7e9cabdbc92dc13b5905ff7142b3[m
Author: huandao0812 <huan@adatau.com>
Date:   Sat Jun 28 10:44:39 2014 +0700

    change BigRClient2 to ManagerClient
    
    change client to static variable
    override toString for LogisticRegressionIRLS

[33mcommit 079f31854451b867accdabb1366bbd8c516cb081[m
Merge: b85f5cf 82ba7be
Author: Binh Han <bhan@adatao.com>
Date:   Fri Jun 27 14:50:50 2014 -0700

    Merged in bhan-missingDataHandling (pull request #125)
    
    MissingDataHandling works for PA

[33mcommit 7ce913ef34185677bd308b1e8b47c2d9295add45[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Fri Jun 27 14:41:54 2014 -0700

    ignore hive-site.xml

[33mcommit 9832e7ba89a5f84f90e55e272031c49775206a79[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Jun 27 17:16:40 2014 +0700

    scala shell for DDF

[33mcommit 3291337cb05091abb8df4a4f9344a4504632f67c[m
Merge: ff3e4b2 82ba7be
Author: huandao0812 <huan@adatau.com>
Date:   Fri Jun 27 09:50:50 2014 +0700

    Merged in bhan-missingDataHandling (pull request #124)
    
    missing DataHandling for ddf-sparkSummit

[33mcommit 82ba7be71e404d4441b6e7f5fe03b5dec8617ea2[m
Merge: 5fe2c02 151ed09
Author: huandao0812 <huan@adatau.com>
Date:   Fri Jun 27 09:48:09 2014 +0700

    Merge branch 'bhan-missingDataHandling' of https://bitbucket.org/adatao/DDF into bhan-missingDataHandling

[33mcommit 5fe2c02399c434f645e31948abd4935fbcfc3ea8[m
Merge: 5b060b6 ff3e4b2
Author: huandao0812 <huan@adatau.com>
Date:   Fri Jun 27 09:47:51 2014 +0700

    Merge branch 'ddf-sparkSummit' into bhan-missingDataHandling

[33mcommit 151ed09496e967c4af59bcf807dfbbb9be4aad67[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Jun 26 16:29:59 2014 -0700

    update inplace works

[33mcommit ff3e4b2c62fb57d5289569b4db0afedc441a9c75[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Jun 26 11:25:05 2014 +0700

    fix model persisting for spark summit demo

[33mcommit b85f5cf43e4eda84f0520f8409152ca01891c41e[m
Merge: aa1a635 9c0ce2d
Author: huandao0812 <huan@adatau.com>
Date:   Thu Jun 26 09:57:59 2014 +0700

    Merged in ddh-fix-model (pull request #121)
    
    PA ML executor return generic IModel

[33mcommit aa1a635ae8f1928b4bacc2de132e088fc3cc5615[m
Merge: 2d73f3c 520068a
Author: huandao0812 <huan@adatau.com>
Date:   Thu Jun 26 09:09:09 2014 +0700

    Merged in ddf-listDDF (pull request #123)
    
    listDDF return only DDF with alias

[33mcommit 520068a4f1184f350612781fc6c2f3dead7df26b[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Jun 26 08:25:57 2014 +0700

    listDDF return only DDF with alias

[33mcommit 5b060b6ddf95c0fb9f81de2e0907ffbf9ec04745[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Jun 25 16:25:35 2014 -0700

    test passed for fillNA executor

[33mcommit 9a39d4669019733c8f233233e92c49a2217eeaab[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Jun 25 15:52:07 2014 -0700

    passed pa test for dropNA

[33mcommit 4aa401de80a521207c22bd5c54e6a191a1e11ae8[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Jun 25 15:10:08 2014 -0700

    code format

[33mcommit 3f1f697080a6223c7e30f8f85403f1ba4d3f32c1[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Jun 25 15:05:54 2014 -0700

    test pa executors

[33mcommit 81a47fb93c89812fd801f45a8049558e8fb0c73b[m
Merge: 7912daa 2d73f3c
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Jun 25 13:43:39 2014 -0700

    Merge branch 'ddf-branch-0.9' of https://bitbucket.org/adatao/DDF into bhan-missingDataHandling

[33mcommit 7912daaae6215413c1545fe404e393fa7cd4011d[m
Author: khangpham <khangpham@adatau.com>
Date:   Wed Jun 25 10:05:10 2014 -0700

    testing

[33mcommit 2d73f3c202651467774d6fb3b54cab34a64b678c[m
Merge: dec8b79 cc2248e
Author: huandao0812 <huan@adatau.com>
Date:   Wed Jun 25 18:00:27 2014 +0700

    Merged in ddf-listDDF (pull request #122)
    
    update ListDDF

[33mcommit cc2248eb22f953fe9487139b751abf30572c373d[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Jun 25 17:52:24 2014 +0700

    update ListDDF
    
    show numRows, numColumns, uri, createdTime.

[33mcommit 8a638151d163742aeeda69e28cbbef339bc3f3e9[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue Jun 24 17:22:21 2014 -0700

    use AggregateFunc

[33mcommit 83343037f233f408688236ff2003783a7f416953[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue Jun 24 17:03:18 2014 -0700

    add pa executors

[33mcommit f8664be0f3c730397cf611ad90cfdc52a4747fa3[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue Jun 24 15:16:20 2014 -0700

    code refactor

[33mcommit 570ee5438a71a5941d546ced667653234511df6e[m
Merge: 5dfa410 dec8b79
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue Jun 24 11:22:07 2014 -0700

    Merge branch 'ddf-branch-0.9' of https://bitbucket.org/adatao/DDF into bhan-missingDataHandling
    
    Conflicts:
    	spark/src/main/java/com/adatao/spark/ddf/ml/MLSupporter.java

[33mcommit 5dfa410a2310519020274d946f435a9e361052d5[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue Jun 24 11:18:42 2014 -0700

    clean code

[33mcommit 9c0ce2d579a2877da0394229855061d31a3fd2a5[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 24 17:39:27 2014 +0700

    clean up pa's Kmeans

[33mcommit 7093ed589e50527f890c045774f4eb5788e1e68f[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 24 14:30:32 2014 +0700

    LinearRegression return generic model

[33mcommit dec8b790539191c594758a29f0e7c4fe91c1c2d5[m
Merge: 38a8e73 99bde85
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 24 11:20:28 2014 +0700

    Merged in model-persisting (pull request #120)
    
    allow ddf user to persist model to disk

[33mcommit 99bde859d429f0986930800c4868f5dcf7d8f95c[m
Merge: a521614 38a8e73
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 24 11:17:53 2014 +0700

    Merge branch 'ddf-branch-0.9' into model-persisting
    
    Conflicts:
    	pa/conf/local/hive-site.xml

[33mcommit f18f0096b1a22a0389532e6c2e7a4f2a9f48e0d7[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 24 10:39:38 2014 +0700

    fix LogisticRegressionIRLS
    
    and LogisticRegressionCRS

[33mcommit ed971cd52449ef58a9699ec82622581053e68181[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 24 09:03:08 2014 +0700

    fix logistic regression predict function

[33mcommit 6428ed84cd42de7ad3b5e3321ff9bed570ffbe00[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 24 08:40:43 2014 +0700

    LogisticRegression use generic model

[33mcommit aa46e73a2a889cb4aaf8313ab7bc9f4d83e4cc14[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 24 05:28:10 2014 +0700

    change mRawModelClass to modelType

[33mcommit a301d55006eeb78e2f61f7da92a3edf29f418c0c[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 24 04:32:06 2014 +0700

    Kmeans & LinearRegression return generic model

[33mcommit a52161425fbfb6fd16ab0056deb45346ecdca5da[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Jun 23 14:33:38 2014 +0700

    fix LogisticRegressionModel
    
    add modelID

[33mcommit ec014057f849b83061c37319329c2cd7007d6ad4[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Jun 23 12:28:45 2014 +0700

    fix LogisticRegressionModel

[33mcommit 61c2c9b619706067d1185d11a29064b58eeee92b[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Jun 23 12:08:38 2014 +0700

    fix LogisticRegressionModel
    
    remove attribute ddfModel

[33mcommit a9a5fb7eceaa7f06b1c6e80d0872a8c4b110b836[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Jun 23 11:50:49 2014 +0700

    add modelID to LogisticRegressionModel

[33mcommit c52a9c696482350bf8791723a5b837615a093c78[m
Author: huandao0812 <huan@adatau.com>
Date:   Sun Jun 22 04:46:06 2014 -0700

    clean up code

[33mcommit 4e79f8a8639d13972728e52c9e11193755833c6b[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Fri Jun 20 16:42:04 2014 -0700

    correct types

[33mcommit ec89fd30f4ffdbabfc9dc2c717a42976fe34a785[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Fri Jun 20 16:30:20 2014 -0700

    add MissingDataTypes

[33mcommit de2339a9d3ff6be1440d028f22da5ab3fb8dac9c[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Jun 20 15:35:45 2014 -0700

    fix R2Score

[33mcommit 291bd64f221aff8119360dbf759bea10c9c614b5[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Jun 20 15:21:32 2014 -0700

    project DDF before run BinaryConfusionMatrix

[33mcommit a588c548cc31fe9cbd6a5f5ca7fbd84a03f9f92e[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Jun 20 14:58:17 2014 -0700

    add bias term for LogisticRegIRLS prediction

[33mcommit 5ab0cb66e2108705b267ad2a3f98989f11d9516a[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Jun 20 13:17:29 2014 -0700

    checking for vector's size before predict in glmIR

[33mcommit ffbb127fd41f45dcc38569a293496df24ef67f64[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Jun 20 12:19:52 2014 -0700

    make IRLSLogisticRegressionModel serializable

[33mcommit 13a81510eeca3c2accda1e5c5115b04fb6d93f59[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Jun 20 12:07:53 2014 -0700

    add modelID to IRLSLogisticRegressionModel

[33mcommit 93082541373e11525197165938d0b758e70b02dc[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Jun 20 11:43:38 2014 -0700

    fix persist model
    
    persist model immediately after train in MLSupporter

[33mcommit 6167621304771ebcf35705bdb52b1ac2d4cf97b9[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Jun 20 00:27:38 2014 -0700

    remove try/catch

[33mcommit 38a8e737f3e45dc280cf9fa8246f1a61c68c9b93[m
Merge: b5373fa 7817247
Author: Binh Han <bhan@adatao.com>
Date:   Thu Jun 19 21:06:40 2014 -0700

    Merged in bhan-missingDataHandling (pull request #119)
    
    Missing Data Handling

[33mcommit 78172472c8df1e0ad804db4a8659311841a5fb79[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Jun 19 20:56:16 2014 -0700

    minor code change

[33mcommit ded10c4699192721c5a2f8b678dffba1a139025a[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Jun 19 20:12:10 2014 -0700

    add model to manager after load from uri

[33mcommit 721bf2b9d2a4b3f82047c61397a9b17186415816[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Jun 19 14:51:32 2014 -0700

    fix LinearRegressionNQModel prediction

[33mcommit f6b9f9d8fe90e8ca6ac8ed0afb5ca154b34d2de7[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Jun 19 13:36:29 2014 -0700

    fix bug

[33mcommit e86fe6daa42a3429885598561a5514482db2db87[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Jun 19 13:19:54 2014 -0700

    add method predict for LinearRegressionNQ

[33mcommit e3bed5590d20bbab5715ff172a744b55c83f7a4f[m
Merge: 884d22b b5373fa
Author: root <root@ip-10-235-66-82.ec2.internal>
Date:   Thu Jun 19 16:56:49 2014 +0000

    Merge branch 'ddf-branch-0.9' into model-persisting

[33mcommit 884d22bee744ede7c31a2d3026842e78a82df7cf[m
Author: root <root@ip-10-235-66-82.ec2.internal>
Date:   Thu Jun 19 16:54:59 2014 +0000

    change script deploy

[33mcommit 6f1a70c97f7223285374c259b012c75adf29020c[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Jun 19 04:03:25 2014 -0700

    fix MLSupporter.apply
    
    able to do sql on prediction dataframe
    add test

[33mcommit efac25bbf5cae64ac9337e2fff868ffa7981a635[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Jun 19 01:42:39 2014 -0700

    set proper type for predicted ddf

[33mcommit e4eb2eb57199d3ba6e8bc012c81b5c37ce0f8490[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Jun 19 00:57:53 2014 -0700

    add ddf to manager after XsYpred

[33mcommit 40fc67022ad49780384c0831f0e7747cf771147a[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Jun 19 00:47:23 2014 -0700

    add XsYPred executor

[33mcommit 32d6473bef2ff3badc07e1f5b82b0df40761b017[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Jun 18 22:21:54 2014 -0700

    correctly convert from double to object

[33mcommit e18c2b34c31b51b26370762075d583442504fb05[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Jun 18 21:44:57 2014 -0700

    add method to RepHandler
    
    to convert from RDD[Array[Double]] to RDD[Array[Object]]

[33mcommit f0ee07426f669c4983a9a2eeb437e841bf810b5e[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Jun 18 20:15:05 2014 -0700

    test passed for different fillNA methods

[33mcommit f5b3c5f486a92e0dacf138d273caf1a396f59343[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Jun 18 19:05:45 2014 -0700

    code comments

[33mcommit feb50c318c56ce8e794ee22dedd4abcd5166b6dd[m
Merge: ca915b4 b5373fa
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Jun 18 17:27:11 2014 -0700

    Merge branch 'ddf-branch-0.9' of https://bitbucket.org/adatao/DDF into bhan-missingDataHandling
    
    Conflicts:
    	core/src/main/java/com/adatao/ddf/DDFManager.java

[33mcommit ca915b42f327c460ad31800d825f529410571ce5[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Jun 18 17:22:12 2014 -0700

    correct column names when handling NA

[33mcommit a7b983cb869cc259585a372595343012572dbc1a[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Jun 18 17:21:10 2014 -0700

    change from Double[] to double[] in aggregateResult

[33mcommit ffb2f7c67aa438ec1d5fa4c1b8235e1cd9ba7f9d[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Jun 18 15:48:28 2014 -0700

    projection before doing YtrueYpred

[33mcommit b5373faafdc1e22625e66004a50d0f0a2887749b[m
Merge: 172fc16 6ac54b2
Author: khangpham <khangpham@adatau.com>
Date:   Wed Jun 18 22:50:40 2014 +0700

    Merged in list-ddf (pull request #118)
    
    add listDDFs command

[33mcommit 6ac54b2b3d89abbf2bed5493a6bc508a81d38257[m
Author: khangich <khangich@gmail.com>
Date:   Wed Jun 18 22:47:49 2014 +0700

    add listDDFs command

[33mcommit d329f7dd3eeccfae86fcc90a7aaaebae0c1647eb[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Jun 18 03:29:47 2014 -0700

    fix bug

[33mcommit 588a11616d75f34b2e106ca6d9b180f3b0c0591c[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Jun 18 00:43:14 2014 -0700

    return proper model in LoadModel

[33mcommit c828772958e45f2f4c0cc0725d8ecb4328822263[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Jun 18 00:12:49 2014 -0700

    revert change from last commit

[33mcommit 439fe958e76f252a0b705e0f428cee719d293ee1[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 17 23:51:02 2014 -0700

    [WIP] return lm model directly from ddf

[33mcommit 8fe8900274e4922b5231e1d6f33387e016077e0d[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue Jun 17 18:58:57 2014 -0700

    test passed for default fillNA case

[33mcommit 85f13e6a708be50325d4cd78804ce9e43ef8ebe5[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 17 18:26:32 2014 -0700

    fix bug

[33mcommit 7aa36dad7bf0fadfbb8b10989469cd2521ef9ea8[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 17 18:15:59 2014 -0700

    add field trainedColumns to KmeansModel, NQLRModel

[33mcommit 0ab430727ff8eff07ef38823ae53cdd372fa2b2c[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue Jun 17 17:39:52 2014 -0700

    refactoring pa

[33mcommit 8f3f6e3f989e6d13fa500afa7d0b9f751236194d[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue Jun 17 17:38:22 2014 -0700

    implement fillNA cases

[33mcommit 4fa40b6aa5ed4394955450dc756212202cd05308[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 17 17:10:20 2014 -0700

    fix bug

[33mcommit 084ca905c7db0460352157eba428e006cc55c70f[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 17 17:06:07 2014 -0700

    add field modelID for NQLRModel

[33mcommit 5a006e1ed794de1299583d3370f570eab7849965[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 17 16:50:14 2014 -0700

    comment out log

[33mcommit 905836aca5f856ff80f62678101299045302432d[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 17 16:31:37 2014 -0700

    fix bug

[33mcommit ee5999a634dc66a1b73546d61d99cff422f25d63[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 17 16:17:55 2014 -0700

    fix bug

[33mcommit 97265e949028b01452b239895772cf9b1af5a83d[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 17 16:05:06 2014 -0700

    fix bug

[33mcommit 1511cdd1ad262a805904fee099211f2f132a87f7[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 17 16:02:49 2014 -0700

    fix bug

[33mcommit 6dae9dc74fd505309993985f8bdd6df19f677cb2[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 17 16:00:19 2014 -0700

    fix Kmeans

[33mcommit 942081a6623c3cc1f2b4ee35d7edc5d8f6f41341[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 17 15:42:41 2014 -0700

    fix bug

[33mcommit 1e426b1cc590385d2b546e91e23b2fd0eed94731[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 17 15:34:42 2014 -0700

    fix bug

[33mcommit 23209446c6daa16396025b168068f5fa7b5627b4[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 17 15:24:55 2014 -0700

    fix bug

[33mcommit 8bdb82ccc2a9c8b3cb1c49836180026d630a431c[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 17 14:46:48 2014 -0700

    add column information

[33mcommit 4f96c645380e55bacfeb3ede057e490a6141a454[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 17 14:15:52 2014 -0700

    add log

[33mcommit 8c24c1666cdd4c295a61321b86480e51a4919782[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue Jun 17 14:09:14 2014 -0700

    Refactor AggregationHandler, add aggregateOnColumn

[33mcommit 421111acf51c1de011f5de2722bc3732116ba059[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 17 13:58:54 2014 -0700

    fix bug

[33mcommit 622594d74c2f50ba85586051fdde366208abb5ad[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 17 13:49:40 2014 -0700

    fix bug

[33mcommit 1fda6c3af0f2c74f70da8cddc8b69328bc1e1703[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 17 13:40:20 2014 -0700

    add log

[33mcommit a52f01f1a66a9b8dcae6fcd12807b8b0256c53e7[m
Merge: 6b3c30a 172fc16
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 17 13:22:17 2014 -0700

    merge with ddf-branch-0.9

[33mcommit 6b3c30a6427555e865ae59cd66c288ee99ca8812[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 17 13:19:15 2014 -0700

    add log message

[33mcommit 3aa2e2695d4a3befaa6d8f1afeb1512b3b62bc83[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 17 12:59:24 2014 -0700

    fix bug

[33mcommit b8d004be82b16f2159e4ee2985cf7b16bfd65ccd[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 17 11:14:04 2014 -0700

    fix bug

[33mcommit 0ff01fd17fdf71c763359c9724e963ba92956d98[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 17 11:10:37 2014 -0700

    add method to check for model
    
    returned from LoadModel

[33mcommit 172fc16b522fc0beb4cd5e026a662e707b977cc6[m
Merge: 7c5ce5a 32b307a
Author: khangpham <khangpham@adatau.com>
Date:   Tue Jun 17 22:47:46 2014 +0700

    Merged in vector-sd (pull request #117)
    
    VectorMean, VectorVar

[33mcommit 32b307afac67ebd728e19f306b4afd9854ad03e7[m
Author: khangich <khangich@gmail.com>
Date:   Tue Jun 17 18:26:26 2014 +0700

    add unit test

[33mcommit b7c61e481dcaa59d9e3566060349676c2c10ed3e[m
Author: khangich <khangich@gmail.com>
Date:   Tue Jun 17 18:26:12 2014 +0700

    work with RCLient, sd, mean, var

[33mcommit 8e79a21afa8e61b81b083b8bb822f30d6f20259f[m
Author: khangich <khangich@gmail.com>
Date:   Tue Jun 17 16:25:48 2014 +0700

    vector correlation

[33mcommit d1ab6d61782153184f34499b83d7286e11288a9f[m
Author: khangich <khangich@gmail.com>
Date:   Tue Jun 17 15:54:59 2014 +0700

    pass unit test vectorMean

[33mcommit 5e75b01ba5b2e23308b85c3641c380fe3ce79158[m
Author: khangich <khangich@gmail.com>
Date:   Tue Jun 17 14:39:51 2014 +0700

    VectorVariance pass unit test

[33mcommit 3dbaef5e1a41c7d55e723bebbde869329d31f3ac[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Jun 16 23:13:07 2014 -0700

    close BufferedWriter stream

[33mcommit 1fa03d0ec21b947300d457a24355b06078372ad2[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Jun 16 18:09:47 2014 -0700

    add method localFileExists

[33mcommit e05e86627f20dee432ec112351e8da030616889d[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Jun 16 17:52:27 2014 -0700

    fix throwing exception

[33mcommit 64b14b3b8ebc06cbc48569d92ca096ae8646c386[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Jun 16 17:48:19 2014 -0700

    fix throwing exception

[33mcommit c1397122ceb68d762ff2cb4af2d73bfbad335420[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Jun 16 17:02:50 2014 -0700

    write file to hdfs

[33mcommit c0348824c6532c429146a375cd9b8fb0e5818b52[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Jun 16 15:11:05 2014 -0700

    WIP
    
    add pa method for persist model

[33mcommit 24c3af56940fb3927f18b15c55aaf36639fceeb5[m
Merge: 9ae4c2c 7c5ce5a
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon Jun 16 12:25:41 2014 -0700

    Merge branch 'ddf-branch-0.9' of https://bitbucket.org/adatao/DDF into bhan-missingDataHandling
    
    Conflicts:
    	core/src/main/java/com/adatao/ddf/DDFManager.java

[33mcommit ba6b7191d7df2eb97d3bd40dca1fcf6e240a2782[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Jun 16 11:37:22 2014 -0700

    add method to serialize/deserialize model
    
    to/from DDF

[33mcommit 9ae4c2cf5f6ccc50e5f08369a38e754a9c4def4c[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon Jun 16 04:52:07 2014 -0700

    passed default dropNA testcase

[33mcommit 7f0a6c5cc6c398996b2b97ff2bb1d91f853b95c1[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon Jun 16 04:33:11 2014 -0700

    allow inplace update

[33mcommit dd2f79b732729e30a37907d4878f8ceb2dd6a567[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon Jun 16 04:32:09 2014 -0700

    add default method dropNA() in DDF

[33mcommit bd3cc1d7fc7f10395b7008fe70d7d6388cea01a1[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon Jun 16 04:30:40 2014 -0700

    implement dropNA cases

[33mcommit 7c5ce5a6a11beb7fcf4c056f7031f35cc4330059[m
Merge: 3d5d7cb e8711e9
Author: khangpham <khangpham@adatau.com>
Date:   Mon Jun 16 14:27:05 2014 +0700

    Merged in model-has-dummy2 (pull request #116)
    
    return model to clients, fix projection at PA level

[33mcommit e8711e9b0aa4f20819dfeafcfdd0760b72ba170f[m
Author: khangich <khangich@gmail.com>
Date:   Mon Jun 16 13:04:34 2014 +0700

    remove printout

[33mcommit 16acd18eac1a96f0f9938f026b6d5b7fd488d695[m
Author: khangich <khangich@gmail.com>
Date:   Mon Jun 16 13:03:54 2014 +0700

    pass unit tests

[33mcommit 2a8b388fe961114ff6392d8a84d1f5e243dc5340[m
Author: khangich <khangich@gmail.com>
Date:   Mon Jun 16 11:09:01 2014 +0700

    work in progress, bug on projection

[33mcommit 3d5d7cbe9f5de5cd4f6c53e646933320089ae349[m
Merge: 3ebbbdb 3449826
Author: khangich <khangich@gmail.com>
Date:   Sun Jun 15 08:02:51 2014 +0700

    resolve conflict

[33mcommit 3449826111bd71d5af6d18602093df7fb74a1cdc[m
Merge: 2c18096 74af533
Author: khangpham <khangpham@adatau.com>
Date:   Sun Jun 15 08:00:44 2014 +0700

    Merged in ddf-0.9.2 (pull request #113)
    
    ddf with official shark + spark, runnable on yarn cluster

[33mcommit 3ebbbdb9639075c02eef80c6e8682560fbb40b1d[m
Author: khangich <khangich@gmail.com>
Date:   Sat Jun 14 18:31:13 2014 +0700

    projection before model

[33mcommit 74af53339a21813c604293d8af24303d15e893da[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Jun 12 16:36:13 2014 -0700

    clean up code

[33mcommit d30e1194bd3c707a6f710b6d0b7dee04ca5e63e5[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Jun 12 16:34:11 2014 -0700

    clean up code

[33mcommit 65843c9cb5a08984e218010d917260121b7bf932[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Jun 12 15:18:14 2014 -0700

    add comment

[33mcommit 5b1939811c5058754ef228e14a540eac3eeef9d5[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Jun 12 15:15:55 2014 -0700

    remove unused variable

[33mcommit 697833dd7f3737ac273ecf9ebbe9d7b08bd3b25d[m
Author: root <root@ip-10-16-144-216.ec2.internal>
Date:   Thu Jun 12 21:50:19 2014 +0000

    add script to build and deploy ddf

[33mcommit 59c9c34f2991a11c36262cb8fad14962007b2c37[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Jun 12 12:49:31 2014 -0700

    add method to create SharkContext
    
    with kryo.registrator

[33mcommit d62f1b32848f6601888f735d110089dc59973615[m
Merge: 11a3179 7feb4aa
Author: root <root@ip-10-16-144-216.ec2.internal>
Date:   Thu Jun 12 09:05:34 2014 +0000

    Merge branch 'ddf-0.9.2' of https://bitbucket.org/adatao/ddf into ddf-0.9.2

[33mcommit 11a3179a9e5c56988390650ff3ed50fb160b9bbb[m
Author: root <root@ip-10-16-144-216.ec2.internal>
Date:   Thu Jun 12 09:05:21 2014 +0000

    dont hardcode master's ip

[33mcommit 7feb4aa82af1b2e6eb5cb27bbafce77f2f8e08d5[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Jun 12 02:03:50 2014 -0700

    add spark.kryo.registrator to params

[33mcommit 4205d60f51e7e601ec5748c09512afa9a00b0619[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Jun 11 17:39:07 2014 -0700

    add log for SparkDDFManager

[33mcommit fbe57d7d9e400b3e5c11dbbf6b3503916fbaa7a3[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Jun 11 17:16:25 2014 -0700

    fix RootBuild

[33mcommit a66c9ea8ae92c47d32150d29e3dc71fb0607fea9[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Jun 11 14:48:38 2014 -0700

    use unpatched shark + spark's version

[33mcommit 7a8cacdfc19e8b30377d9e016f81f91ee235e5ab[m
Author: khangich <khangich@gmail.com>
Date:   Wed Jun 11 17:58:10 2014 +0700

    wip

[33mcommit 53826f3335ca57b0edd5e56f745ab839abda3dba[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 10 04:58:57 2014 -0700

    fix bug in MLSupporter

[33mcommit a6439ce49f104a2d9440af0fb553083cfd9e6b2a[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 10 04:45:43 2014 -0700

    add log

[33mcommit 609699816009ef5e0bddfe76bb50ae2ad113a2b4[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 10 04:45:23 2014 -0700

    add log message

[33mcommit 5047c225449da56a7a85704a521bcda5646a77fa[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 10 04:29:03 2014 -0700

    add log message for loadDDF

[33mcommit 69683323d8bd16206ba929d7b3c8d5002bdc23c2[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 10 04:12:16 2014 -0700

    check for null AliasName

[33mcommit 6ca6a17d48cecf1e056f3b02ae0a02131773d0b6[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 10 03:52:54 2014 -0700

    add log

[33mcommit bf0a58329600299a9774ea04c72fcc8b3d2d56df[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 10 03:44:35 2014 -0700

    add log message for BasicDDF

[33mcommit 892718f35bf9a0165f1581b5a3fd28865c735084[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 10 03:19:42 2014 -0700

    add methods for persisting DDF:
    
    LoadDDF, PersistDDF, Serialize2DDF, Deserialize2Model
    method to persist ddf in BasicDDF and method to loadDDF in DDFManager

[33mcommit 2c180967ecacbb33759b43781730a02cd66e6b0a[m
Merge: 81b33a3 fa5c862
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue Jun 10 00:47:51 2014 -0700

    Merge branch 'ddf-branch-0.9' of https://bitbucket.org/adatao/DDF into ddf-branch-0.9

[33mcommit 81b33a36e12ba5d532b3aa075132733e3382c88d[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue Jun 10 00:46:46 2014 -0700

    getDDF returns a DataFrame with mutability info

[33mcommit 9b8bc3d803c10785dd7e09736659331b7ddf59ef[m
Merge: c49fedc f3c39c8
Author: khangich <khangich@gmail.com>
Date:   Tue Jun 10 11:06:04 2014 +0700

    Merge branch 'support-join' into ddf-branch-0.9-yarn

[33mcommit fa5c862c4bd21e01e539bd7f66fd26ef7eb5ef0a[m
Merge: c966add d05fe16
Author: khangich <khangich@gmail.com>
Date:   Tue Jun 10 11:04:28 2014 +0700

    Merge branch 'ddf-branch-0.9' of bitbucket.org:adatao/ddf into ddf-branch-0.9

[33mcommit c966add13938ac0c8999bcbcf6343d5eceb9a101[m
Author: khangich <khangich@gmail.com>
Date:   Tue Jun 10 11:04:12 2014 +0700

    add test file

[33mcommit 71b3cc09f67e4a989f01ce9988c16a8a77468340[m
Merge: 4e05dd9 f3c39c8
Author: khangpham <khangpham@adatau.com>
Date:   Tue Jun 10 10:58:52 2014 +0700

    Merged in support-join (pull request #111)
    
    Support join

[33mcommit c49fedc477e7dcf19bb3b6f4e85d733c6c875646[m
Merge: 48dd964 9c94174
Author: khangich <khangich@gmail.com>
Date:   Tue Jun 10 09:53:56 2014 +0700

    Merge branch 'fix-y-npe' into ddf-branch-0.9-yarn

[33mcommit 9c9417423851cf0d94b94f66d053275ec7370ff6[m
Author: khangich <khangich@gmail.com>
Date:   Tue Jun 10 09:48:13 2014 +0700

    handle NPE in RH

[33mcommit e02ee830c641c729e07d837e2cc82526ea2d7d88[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon Jun 9 19:30:39 2014 -0700

    uri for model

[33mcommit d05fe16373097b60effb65a64147e9a62b307981[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon Jun 9 14:17:14 2014 -0700

    refactor join, test passed

[33mcommit 8b55d6d46895c7d47ac3c84048e32aa07f1d30c2[m
Merge: 288ba94 f3c39c8
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon Jun 9 13:39:57 2014 -0700

    Merge branch 'support-join' of https://bitbucket.org/adatao/DDF into ddf-branch-0.9

[33mcommit 288ba94a6bfce1d88048634adbd6072f5f75c7e6[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon Jun 9 11:46:23 2014 -0700

    pa test passed

[33mcommit 7c847b51d3db86d8aedd915f90250cb6a2eaded3[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon Jun 9 08:45:08 2014 -0700

    pass unit test

[33mcommit f3c39c8028b8335215d039d28630486b9fbd6f57[m
Author: khangich <khangich@gmail.com>
Date:   Mon Jun 9 21:57:34 2014 +0700

    pass pa unit tests

[33mcommit 07467a48a3b7501edfe2158b24a8bc95cf33bcdd[m
Author: khangich <khangich@gmail.com>
Date:   Mon Jun 9 18:08:42 2014 +0700

    wip can run

[33mcommit 1e82cd23873e0557b4c656bb43b3821f33029f43[m
Merge: dbedf13 d2dd4e4
Author: khangich <khangich@gmail.com>
Date:   Mon Jun 9 13:45:20 2014 +0700

    Merge branch 'ddf-branch-0.9' into support-join

[33mcommit d2dd4e47f260e1588edcb8f70be2b1b8c03e2397[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Sun Jun 8 23:44:02 2014 -0700

    getURI executor

[33mcommit dbedf13d3ff39be662625804485e6edab5f8cad2[m
Author: khangich <khangich@gmail.com>
Date:   Mon Jun 9 13:41:03 2014 +0700

    join wip

[33mcommit 26d547cfc4f2a8e727e12caa86011f8ca2c1908b[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Sun Jun 8 22:51:31 2014 -0700

    using ddf namespace in uri

[33mcommit 42613d69e5768dee4e15a7f87578c69d30309192[m
Merge: 7b94b68 21c5ac2
Author: bhan <binhhan.gt@gmail.com>
Date:   Sun Jun 8 18:44:34 2014 -0700

    Merge branch 'ddf-branch-0.9' of https://bitbucket.org/adatao/DDF into ddf-branch-0.9

[33mcommit 7b94b68b82761da6938af1822e030d80ddfa68ec[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Sun Jun 8 18:42:56 2014 -0700

    add groupBy pa-executor

[33mcommit 3bed77fa5b48669908c9f167acb2d06b62883a68[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Sun Jun 8 18:41:46 2014 -0700

    add groupBy api

[33mcommit e2e0fd740cfe8486c8819b78460bd9d6c2ccb5a1[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Sun Jun 8 17:43:39 2014 -0700

    add groupBy to ddf

[33mcommit 21c5ac2eea7531b9efd2e15520946d1f306a049c[m
Merge: 27d7593 fd6a354
Author: Binh Han <bhan@adatao.com>
Date:   Sun Jun 8 15:45:49 2014 -0700

    Merged in ddh-empty-ddf (pull request #110)
    
    add method to check for empty DDF when getting Representation

[33mcommit 48dd96416e95420c15a85d165132e8c1e5af863a[m
Merge: 0b9600f 27d7593
Author: huandao0812 <huan@adatau.com>
Date:   Fri Jun 6 16:12:08 2014 -0700

    Merge branch 'ddf-branch-0.9' into ddf-branch-0.9-yarn

[33mcommit fd6a3544b64057085c97f2a5dbd3d48e1b9f4bf3[m
Merge: 973a46a 27d7593
Author: huandao0812 <huan@adatau.com>
Date:   Fri Jun 6 15:56:44 2014 -0700

    Merge branch 'ddf-branch-0.9' into ddh-empty-ddf
    
    Conflicts:
    	spark/src/test/scala/com/adatao/spark/ddf/content/RepresentationHandlerSuite.scala

[33mcommit 973a46a95795c3662b677bbe8d0c14157f6723e9[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Jun 6 15:55:42 2014 -0700

    check for empty DDF before get representation

[33mcommit 27d759343c20f905c67470cbcbafb17629f38696[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Jun 6 15:43:37 2014 -0700

    add test for RepHandlerSuite
    
    make sure can do sql queries after TransformNativeRserve

[33mcommit 0b9600f72b107b06283f5c59979972dd70f85c69[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Jun 5 14:28:50 2014 -0700

    add protobuf 2.5.0 dependency

[33mcommit 0558510aa4c070a19774c6cb1675f2f7a92234f6[m
Merge: 2fa4fcc ca3fa2f
Author: root <root@ip-10-16-145-150.ec2.internal>
Date:   Thu Jun 5 02:41:04 2014 +0000

    Merge branch 'ddf-branch-0.9' into ddf-branch-0.9-yarn

[33mcommit ca3fa2f9f1dfe234565ccefa928c9ce3fac878bb[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Jun 4 19:32:37 2014 -0700

    demo ddf uri

[33mcommit 2fa4fcc10ba7afc3d3b7b1a0edf96758bc099c4e[m
Merge: bbedcba 5158918
Author: huandao0812 <huan@adatau.com>
Date:   Wed Jun 4 17:16:26 2014 -0700

    Merge branch 'ddf-branch-0.9' into ddf-branch-0.9-yarn

[33mcommit bbedcba616eefa38b2db4cb8a4a4fa0b103ebe5b[m
Merge: 9534335 e1177ea
Author: root <root@ip-10-16-145-150.ec2.internal>
Date:   Wed Jun 4 23:31:07 2014 +0000

    Merge branch 'ddf-branch-0.9-yarn' of https://bitbucket.org/adatao/ddf into ddf-branch-0.9-yarn
    
    Conflicts:
    	project/RootBuild.scala

[33mcommit 953433588bf991861224bf6aaca69fdcb7f8c73a[m
Author: root <root@ip-10-16-145-150.ec2.internal>
Date:   Wed Jun 4 22:45:21 2014 +0000

    add proper spark-class and exe/spark-class, fix pa-env.sh

[33mcommit e1177eac1f2d27067b6ea3483bd8caeb61c74e1e[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Jun 4 15:43:48 2014 -0700

    fix merge strategy

[33mcommit 5158918acdc8cdf9293a18b653217c254d24dfb2[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Jun 4 11:07:18 2014 -0700

    replace hyphen in ddfname with underscore

[33mcommit 6e7f82dcf46359d7abfa0e837abf8bdc99dcc843[m
Merge: e5c8531 bb5f3ee
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Jun 4 10:53:20 2014 -0700

    Merge branch 'ddf-branch-0.9' of https://bitbucket.org/adatao/DDF into ddf-branch-0.9
    
    Conflicts:
    	pa/src/main/java/com/adatao/pa/spark/execution/GetModel.java

[33mcommit 62b163eea0a36cf9ea4329a891e8476038767c9f[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Jun 4 10:51:19 2014 -0700

    uncomment install-jars.sh comment

[33mcommit bb5f3ee2caa9970320433cfa58f8984c649e9379[m
Author: khangich <khangich@gmail.com>
Date:   Wed Jun 4 16:08:35 2014 +0700

    pass binning, regression, ignore on regresion under spark

[33mcommit 7557fdd4c1f939192174a478e9fc1306f7719139[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Jun 4 01:30:05 2014 -0700

    fix rootbuild

[33mcommit 90f5a1fe6b786696382b2d2559ecfc95fda58bcd[m
Author: khangich <khangich@gmail.com>
Date:   Wed Jun 4 15:09:38 2014 +0700

    pass BinningSuite

[33mcommit 9f31ff72267e2b810639e1033351082ac7add837[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Jun 4 01:07:35 2014 -0700

    use adatao's version

[33mcommit 8a0791b94b3749682e82b6c5c16d30aa9be7fa58[m
Author: khangich <khangich@gmail.com>
Date:   Wed Jun 4 12:15:46 2014 +0700

    update TransformNativeRserveSuite

[33mcommit 731384713edf5e14e69cb3a3eeb7dfbca13ce034[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 3 17:55:54 2014 -0700

    add method to get RDD[Array[Double]]
    
    from RDD[Array[Object]]

[33mcommit b61359fe8a499111a4ac6633af3c7010b9ed56d6[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 3 17:33:56 2014 -0700

    reformat code

[33mcommit d96f2ab54b2affc0e7849dbe069406cefbee4b27[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 3 16:31:11 2014 -0700

    uncomment tests in CVSuite

[33mcommit 8cc783a836b3eba251d0ea8466036c39a45e4223[m
Merge: ad192f6 fe80ef1
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 3 16:07:40 2014 -0700

    Merge branch 'ddf-branch-0.9' of https://bitbucket.org/adatao/DDF into ddf-branch-0.9
    
    Conflicts:
    
    spark/src/test/scala/com/adatao/spark/ddf/content/RepresentationHandlerS
    uite.scala

[33mcommit ad192f679c31f041bfd76caa9f2ee43942b3781e[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 3 15:56:20 2014 -0700

    fixe CreateSharkDataFrameSuite
    
    fix MapReduceNative

[33mcommit fe80ef1acb704202d78daae8b9a48f9b45d79785[m
Author: khangich <khangich@gmail.com>
Date:   Tue Jun 3 23:19:31 2014 +0700

    handle null cell value in rh

[33mcommit fad1be19d9825b5d594a4a5f34578a103ddd5275[m
Merge: 97f509a 6b257be
Author: khangich <khangich@gmail.com>
Date:   Tue Jun 3 16:31:36 2014 +0700

    resolve conflict

[33mcommit 97f509a1c9ca7867ef48124c78ece973a6492afa[m
Merge: b25efcb 44a0110
Author: khangich <khangich@gmail.com>
Date:   Tue Jun 3 16:29:39 2014 +0700

    fix conflict

[33mcommit 6b257beb011c0b7503467c76be6d35c0e327eb24[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Jun 3 02:24:33 2014 -0700

    pass 3 of CreateSharkDataFrameSuite

[33mcommit b25efcbd94252c340b1a68ec105cb3a26259bafe[m
Author: khangich <khangich@gmail.com>
Date:   Tue Jun 3 15:47:54 2014 +0700

    temporary wip

[33mcommit 44a0110b09dc154f3e95801d59218b00a08c7d78[m
Merge: 1bab018 200ccac
Author: huandao0812 <huan@adatau.com>
Date:   Mon Jun 2 23:47:28 2014 -0700

    Merge branch 'ddf-branch-0.9' of https://bitbucket.org/adatao/DDF into ddf-branch-0.9
    
    Conflicts:
    
    spark/src/test/scala/com/adatao/spark/ddf/content/RepresentationHandlerS
    uite.scala

[33mcommit 1bab018cf7c17cb9b024b706f78ac5824ac8b2df[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Jun 2 23:46:42 2014 -0700

    fix netty version

[33mcommit a3c8d85ea71b04d15bf806fe2beadfc2f94ae81b[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Jun 2 23:18:01 2014 -0700

    fix newTableName()
    
    fix CreateSharkDataFrameSuite test #1 & #2

[33mcommit 5fa95b7fe805d9230d17854b978477b0d097961d[m
Author: khangich <khangich@gmail.com>
Date:   Tue Jun 3 11:15:15 2014 +0700

     remove log

[33mcommit 200ccac0e728b7b88ac0d7f61cd5bb1691822f3f[m
Author: khangich <khangich@gmail.com>
Date:   Tue Jun 3 10:53:28 2014 +0700

    get dummy coding in lmgd, fix unit test

[33mcommit c0fcedb85a263fb9bed53940244b15317eb87e6d[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Jun 2 19:14:29 2014 -0700

    fix pom, and rootbuild ddf version to 0.9

[33mcommit df15910ff5b2d74a677c858020bed558228aaa77[m
Author: khangich <khangich@gmail.com>
Date:   Sun Jun 1 19:45:51 2014 -0700

    disable MatrixVector test

[33mcommit 2d8091ce3f01a7ab23646492de484118b668fa17[m
Author: khangpham <khangpham@adatau.com>
Date:   Fri May 30 17:49:04 2014 -0700

    stop installing old hive patch

[33mcommit 86dc53887ed2d981e05976668c930cd3b4ebd265[m
Merge: 705b4c8 edceced
Author: khangich <khangich@gmail.com>
Date:   Fri May 30 04:17:04 2014 -0700

    Merge branch 'ddf-branch-0.9' of bitbucket.org:adatao/ddf into ddf-branch-0.9

[33mcommit 705b4c852fe4de96cb4f4ca6b22c074941cad17c[m
Author: khangich <khangich@gmail.com>
Date:   Thu May 29 18:07:31 2014 -0700

    remove printout to Log.info

[33mcommit edceced7671129ac8a40c0f6067d4dea4fc8a3c6[m
Merge: e86253b c8c3742
Author: huandao0812 <huan@adatau.com>
Date:   Thu May 29 13:00:53 2014 -0700

    Merged in ddh-TablePartition (pull request #109)
    
    Add method to create RDD[TablePartition]

[33mcommit c8c3742db1afe903f803f350b9abfcbb603292d8[m
Merge: 21e4eac e86253b
Author: huandao0812 <huan@adatau.com>
Date:   Thu May 29 12:21:05 2014 -0700

    Merge branch 'ddf-branch-0.9' into ddh-TablePartition
    
    Conflicts:
    	spark/src/test/scala/com/adatao/spark/ddf/content/RepresentationHandlerSuite.scala

[33mcommit def9d435e21e374c6becfc1dad40a4d53c12d3cf[m
Author: khangich <khangich@gmail.com>
Date:   Thu May 29 12:15:13 2014 -0700

    manual projectiom

[33mcommit 21e4eac5c40c089dd8f819194aaabda2b10c694a[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed May 28 20:08:50 2014 -0700

    rename to CanConvertToTablePartition
    
    add column types

[33mcommit e86253b43eccd07bf39a549616a55ce6630eea35[m
Author: khangich <khangich@gmail.com>
Date:   Wed May 28 20:00:42 2014 -0700

    ignore loadHiveTable, no longer support LoadTable executor

[33mcommit cfa37f81c24d36f087bb1ac8eabfa7ee3e0731b0[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed May 28 19:45:00 2014 -0700

    add error message

[33mcommit 24bc333ab5d9eb7ae47d30934b9716470ec83f37[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed May 28 19:13:16 2014 -0700

    clean up log message

[33mcommit 54cda3aa37deebea884e46a999875e4e14c75b6c[m
Author: khangich <khangich@gmail.com>
Date:   Wed May 28 18:52:06 2014 -0700

    unit test for stack overflow

[33mcommit 094b759ae8ede4dcc7667fcb207b453156fd1e15[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed May 28 18:28:00 2014 -0700

    get RDD[TablePartition] from RDD[Array[Object]]
    
    no need to get RDD[Seq[_]]

[33mcommit 1743658753874fb17abd6ea70c18119a034db9f0[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed May 28 17:37:08 2014 -0700

    add method to create TablePartition from RDD[Seq]

[33mcommit 0a6fadb2d317b0b1f1cd45d94027dbd37a70ab12[m
Author: khangich <khangich@gmail.com>
Date:   Wed May 28 01:05:13 2014 -0700

    delete unused code, refactor code

[33mcommit bb88f80f77c9546c92254c889815be1b8b6e2200[m
Author: khangich <khangich@gmail.com>
Date:   Wed May 28 00:23:50 2014 -0700

    format code, adataoexception, remove unused import

[33mcommit f3a7e2dfc6f7cdfae0824097815a06af81f87498[m
Author: khangich <khangich@gmail.com>
Date:   Tue May 27 23:30:36 2014 -0700

    fix Kryo registor in Shark, fix unit tests

[33mcommit e5c8531683147eb1fde392a414ab09057b408447[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue May 27 18:29:49 2014 -0700

    code cleaning

[33mcommit cf554862e16ed2608d204f54404fcba05ab805c7[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue May 27 10:23:25 2014 -0700

    remove rowsToTablePartition

[33mcommit b13878233aa8325e8e3484bf12a985f0537c006a[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon May 26 23:29:40 2014 -0700

    fix FactorSuite
    
    test failed do to scala 2.10.3 incompatitbility

[33mcommit 39cb056a7199c24dff344ce86cf974d03895ed98[m
Merge: 37b2331 bd750d6
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon May 26 18:29:21 2014 -0700

    Merge branch 'ddf-branch-0.9' of https://bitbucket.org/adatao/DDF into ddf-branch-0.9
    
    Conflicts:
    	spark/src/main/java/com/adatao/spark/ddf/etl/SqlHandler.java

[33mcommit bd750d6e5d4cce494315476ebacabda2607423ea[m
Merge: 1a815ca d6ffce1
Author: huandao0812 <huan@adatau.com>
Date:   Mon May 26 18:07:32 2014 -0700

    Merge branch 'ddf-branch-0.9' of https://bitbucket.org/adatao/DDF into ddf-branch-0.9

[33mcommit 1a815ca276b788b5bee61641aeebdff051077a0c[m
Merge: a46b16e 4e05dd9
Author: huandao0812 <huan@adatau.com>
Date:   Mon May 26 17:56:35 2014 -0700

    Merge branch 'master' into ddf-branch-0.9
    
    Conflicts:
    	spark/src/main/scala/com/adatao/spark/ddf/content/RepresentationHandler.scala

[33mcommit d6ffce1bdb9811e628039307dd78cecc8902f7fa[m
Author: khangich <khangich@gmail.com>
Date:   Mon May 26 17:54:42 2014 -0700

    ignore some insignificant tests, change hive-site

[33mcommit 37b2331b9b5c546c5ab1695e1390c267aa1afb12[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon May 26 16:36:16 2014 -0700

    remove patched code

[33mcommit a46b16ee0519576b3444e55aad092b339a9c6071[m
Author: khangich <khangich@gmail.com>
Date:   Mon May 26 16:31:02 2014 -0700

    change sqlhandler, no longer use Adatao Shark patch api

[33mcommit efd88f6f56d1c9d8ea15e6ca556011d0dd962097[m
Author: khangich <khangich@gmail.com>
Date:   Mon May 26 16:00:21 2014 -0700

    fix dependecy

[33mcommit a16b9c5957ceb99ecea7cea5669e115f0e054ca9[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon May 26 15:15:53 2014 -0700

    ddf-pa build success

[33mcommit 59b8126ea06df99492b1066e41016bdd420e39ce[m
Merge: b403813 e021677
Author: khangich <khangich@gmail.com>
Date:   Mon May 26 15:00:20 2014 -0700

    kmean test

[33mcommit b403813c89ae58149a4a88396f56a6a5704f2dc6[m
Author: khangich <khangich@gmail.com>
Date:   Mon May 26 14:57:16 2014 -0700

    update kmeanstest

[33mcommit e0216773870edf668e67594a7f52a337ef8ddc45[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon May 26 14:56:41 2014 -0700

    ddf-spark build sucess

[33mcommit 827dd9cca998e0a532626c6a6dbacbbe2b603a48[m
Merge: 9187d05 1ba43f9
Author: khangich <khangich@gmail.com>
Date:   Mon May 26 14:53:09 2014 -0700

    Merge branch 'ddf-branch-0.9' of bitbucket.org:adatao/ddf into ddf-branch-0.9

[33mcommit 9187d05c01f0364a0d550b7c92905b240877daf2[m
Author: khangich <khangich@gmail.com>
Date:   Mon May 26 14:52:45 2014 -0700

    get back method

[33mcommit 1ba43f9e99e872c3a8eff646ac9eff09d6c00f1d[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon May 26 14:50:41 2014 -0700

    revert (wip)

[33mcommit ffe6bc2ce24f8d6eb747ca35735614f22c47caf1[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon May 26 14:17:23 2014 -0700

    revert (wip)

[33mcommit 5a87075487b407d0b9ad74ffce952faa0ce4f899[m
Author: khangich <khangich@gmail.com>
Date:   Mon May 26 13:54:44 2014 -0700

    update RootBuild mllib, change LabelledPoint to 0.9 ..

[33mcommit 272b0dee530764e6a11ab281e091e15d6bc3cc6c[m
Author: khangich <khangich@gmail.com>
Date:   Mon May 26 12:56:06 2014 -0700

    branch 0.9 user spark0.9, shark0.9

[33mcommit cfc3847d2ebd04006d665bf26a86f3d1124624a0[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Sat May 24 17:30:46 2014 -0700

    remove warnings

[33mcommit c4f0cb7ea125c2cb391e62de11eda74b6828da7d[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Sat May 24 16:59:43 2014 -0700

    BUILD SUCCESS

[33mcommit 1e88c20357986ec3669b2c0f12f685a40f79a3e2[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Sat May 24 16:48:53 2014 -0700

    refactor pa, fix testsuites

[33mcommit bbd5a575d542a313702149ed8e5dced3295f59d3[m
Merge: 75112d1 62f0025
Author: bhan <binhhan.gt@gmail.com>
Date:   Sat May 24 15:24:29 2014 -0700

    testing

[33mcommit 75112d195577364822a304b98533ba4e76892067[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Fri May 23 21:29:22 2014 -0700

    fix pa tests(wip)

[33mcommit 62f0025f7cc163b3e8b122b87dde97b080e61fb1[m
Author: khangich <khangich@gmail.com>
Date:   Fri May 23 21:19:43 2014 -0700

    remove suites

[33mcommit 6820f695ed729884d0add6eb2ba4093949f9fc33[m
Author: khangich <khangich@gmail.com>
Date:   Fri May 23 19:31:29 2014 -0700

    remove unit test

[33mcommit 3cfbc321fe0124512bdf4380f89f3a1fb8412d5c[m
Merge: 7995265 75732fb
Author: khangich <khangich@gmail.com>
Date:   Fri May 23 19:22:22 2014 -0700

    Merge branch 'bhan-branch-1.0' of bitbucket.org:adatao/ddf into bhan-branch-1.0

[33mcommit 799526558b7f7fbe0df0a203ed628860c7c9def9[m
Author: khangich <khangich@gmail.com>
Date:   Fri May 23 19:22:17 2014 -0700

    delete file

[33mcommit 75732fbb3f1aa819b4ea51f86fbfae90e12a640d[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Fri May 23 19:21:33 2014 -0700

    pa fix

[33mcommit a5274c1c6d83aa854a291279781f503b4c1e81db[m
Author: khangich <khangich@gmail.com>
Date:   Fri May 23 18:54:24 2014 -0700

    remove import, remove unused executors

[33mcommit 76767c7b81545a80ec2094fd444c64f3b6fa6234[m
Author: khangich <khangich@gmail.com>
Date:   Fri May 23 18:01:14 2014 -0700

    remove/fix import

[33mcommit 8dc878df803e60c6bdc3463fb94d6be7e1e8ba5e[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Fri May 23 17:42:48 2014 -0700

    pa bug fix (wip)

[33mcommit 4e05dd92aa5b655db5b12db91e0eb4d78b1f8cdc[m
Merge: d310138 798db0b
Author: huandao0812 <huan@adatau.com>
Date:   Fri May 23 15:27:27 2014 -0700

    Merged in ddh-fix-repH (pull request #108)
    
    clean up import

[33mcommit 798db0bcd051fe2b5529d34861e7398e6f7bb41f[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri May 23 15:25:38 2014 -0700

    clean up import

[33mcommit d310138dc12428c72634bfa16ec6afcd8cb25ab3[m
Merge: 213f9fe 0a09784
Author: Binh Han <bhan@adatao.com>
Date:   Fri May 23 15:17:46 2014 -0700

    Merged in ddh-fix-repH (pull request #106)
    
    fix repHandler

[33mcommit 213f9fe1df688d354649213a5d183a42fd53793d[m
Merge: 7c3f92f c0aaaf4
Author: Binh Han <bhan@adatao.com>
Date:   Fri May 23 11:20:21 2014 -0700

    Merged in bhan-ddf-mutability (pull request #107)
    
    DDF mutability + name sharing

[33mcommit b8dc85dac21f9199b3f4a6ae40888ba66c861b11[m
Author: khangich <khangich@gmail.com>
Date:   Fri May 23 11:13:09 2014 -0700

    add snapshot

[33mcommit 18298ccadd6e46c1f45f63142ee9da13734c1c35[m
Author: khangich <khangich@gmail.com>
Date:   Fri May 23 11:02:38 2014 -0700

    fix jetty

[33mcommit fad2ba258d52ba1b5eb6694d4e09c3d1a7eed9d9[m
Author: khangich <khangich@gmail.com>
Date:   Fri May 23 10:40:27 2014 -0700

    spark-1.0 is no longer SNAPSHOT

[33mcommit 0a097847c1177a0b6d5ae577d26e6b73fc02aeef[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu May 22 17:02:02 2014 -0700

    fix test

[33mcommit 9b2ef0db174c0aac8d78cd4c22307f62118d8fca[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu May 22 16:42:18 2014 -0700

    add NULL handing
    
    + rowsToArraysDouble
    + rowsToLabeledPoints

[33mcommit c0aaaf4632da4e83bd4a1dd19644475780a3dc80[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu May 22 15:14:15 2014 -0700

    more tests passed

[33mcommit 062f45d977b3fdc76312d97ff736d626309c75fa[m
Merge: dd9e641 3aa1d74
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu May 22 14:33:35 2014 -0700

    Merge branch 'bhan-ddf-mutability' of https://bitbucket.org/adatao/DDF into bhan-ddf-mutability

[33mcommit dd9e6419839c01a7ba2e8ca4e0df00b6bfeb2f1b[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu May 22 14:31:22 2014 -0700

    test passed

[33mcommit 3aa1d7463b459bd51264467cfa48993cf747d9af[m
Author: khangich <khangich@gmail.com>
Date:   Thu May 22 12:30:43 2014 -0700

    fix return GetModel

[33mcommit fb26838d9ecc8081e74ad559590cfaf2ce935d02[m
Author: khangich <khangich@gmail.com>
Date:   Thu May 22 11:48:09 2014 -0700

    change return type

[33mcommit cb49549aba9306c507e55b9d993ca4c3de77a5a0[m
Merge: 7d9ac9a e648d8e
Author: khangich <khangich@gmail.com>
Date:   Thu May 22 11:28:05 2014 -0700

    Merge branch 'bhan-ddf-mutability' of bitbucket.org:adatao/ddf into bhan-ddf-mutability

[33mcommit 7d9ac9adb3a8fba77f5782c4aeb35c2182fa6dbf[m
Author: khangich <khangich@gmail.com>
Date:   Thu May 22 11:27:56 2014 -0700

    getmodel

[33mcommit e648d8e374edbfc908159e85e854bb1b67ae057a[m
Merge: 85427d8 f097d70
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu May 22 11:23:17 2014 -0700

    Merge branch 'bhan-ddf-mutability' of https://bitbucket.org/adatao/DDF into bhan-ddf-mutability

[33mcommit 85427d8a3863ab09e38296d4e01b8c03d641e450[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu May 22 11:21:30 2014 -0700

    add PA executor for DDF setMutable

[33mcommit f097d700d43b83942b6a099693d552399a40cf83[m
Author: khangich <khangich@gmail.com>
Date:   Thu May 22 07:49:38 2014 -0700

    sucessfully get/set ddf-name

[33mcommit d187487741aa81f7c8920d1e3d23ccb464feb6e0[m
Merge: 452899f 9b2445f
Author: khangich <khangich@gmail.com>
Date:   Thu May 22 07:19:53 2014 -0700

    Merge branch 'ddf-name' into bhan-ddf-mutability

[33mcommit 452899f341bee16c5a3e3b6ce1be4cddfebb696d[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed May 21 23:50:50 2014 -0700

    test

[33mcommit 9b2445ffdb4fbbeabaa9534746e145b6afe82e5c[m
Author: khangich <khangich@gmail.com>
Date:   Wed May 21 17:56:49 2014 -0700

    fix setDDFName

[33mcommit d50c64c097df854f364883132f133f55ae9082bc[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed May 21 17:55:35 2014 -0700

    fix repHandler
    
    put obj with typeSpecs to mReps after createRepresentation.
    Add test for CrossValidation,
    add test for RepresentationHandler

[33mcommit eb48999b1cb157ebe3160fe9f303142fa91b365d[m
Author: khangich <khangich@gmail.com>
Date:   Wed May 21 17:30:13 2014 -0700

    add setDDFName

[33mcommit d50fd7d9376acd85e3ef01386fdc0104dd05bb6a[m
Author: khangich <khangich@gmail.com>
Date:   Wed May 21 15:44:33 2014 -0700

    remvoe typo

[33mcommit 9ada93c149db3203a040a6df5f55da4fe25cedab[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed May 21 12:17:08 2014 -0700

    add mutable feature for DDF

[33mcommit 7b40396bcc300d9650176762f801dc3e585aaa81[m
Author: khangich <khangich@gmail.com>
Date:   Wed May 21 10:28:38 2014 -0700

    print out

[33mcommit dc4ba723e39d4a0416489de060a6dd81aa946062[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue May 20 23:57:46 2014 -0700

    fixing bugs (wip)

[33mcommit ff8c00c87996e579cad3dee1e00f4d226ff37c2d[m
Merge: de0857c 7c3f92f
Author: khangich <khangich@gmail.com>
Date:   Tue May 20 22:28:41 2014 -0700

    resolve conflicts

[33mcommit de0857cc126b14d839a519cebc969c89c5185ce2[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue May 20 22:03:23 2014 -0700

    fix in rootbuild.scala and nrow

[33mcommit 84db960c309e7c6aca44ede461e7526bc6bcca3e[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue May 20 21:17:17 2014 -0700

    add double[] to mllib vector util

[33mcommit 37126aa20ee3460282f91c74da3cd78c60fec5d1[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue May 20 18:58:34 2014 -0700

    Add RDD<Row> to RDD of mllib Vector

[33mcommit 37687df51bb222098416fc79f40129caba8556ae[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue May 20 18:18:16 2014 -0700

    Changes to implements Spark Java interfaces instead of abstract classes
    Remove obsolete ClassManifest from scala-2.9
    Changes to new Shark apis for ColumnDesc and MemoryDataManager

[33mcommit 68b611593104605e4327e759669561e7a9ff9d56[m
Author: khangich <khangich@gmail.com>
Date:   Tue May 20 14:43:51 2014 -0700

    add getddf by name executor

[33mcommit 7c3f92f1b1c69795f72954e5e4cf741fd80e482c[m
Author: khangich <khangich@gmail.com>
Date:   Tue May 20 11:38:43 2014 -0700

    disable HadoopFsShell

[33mcommit b72a7e506f2d721c2844c166ec2ea5c646b7424e[m
Author: khangich <khangich@gmail.com>
Date:   Tue May 20 10:09:36 2014 -0700

    change bigr to pa

[33mcommit ddd6ca96af11582f6ea66713cba51ba3e9ca042b[m
Author: khangich <khangich@gmail.com>
Date:   Mon May 19 22:12:25 2014 -0700

    remove kmeans2 -> kmeans

[33mcommit e0a3aed8b1dd4d3995d51ea8b71d4122eb3472c0[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon May 19 19:56:06 2014 -0700

    refactor ddf-spark to take changes of MLlib vector specification

[33mcommit be9889421855ea95f486e44439dbb96d01207509[m
Author: khangich <khangich@gmail.com>
Date:   Mon May 19 19:08:36 2014 -0700

    ignore RDirMapper

[33mcommit e7862780e59604c99083fca65e1ce9fc1ab31410[m
Merge: 965e63f c7988b1
Author: khangpham <khangpham@adatau.com>
Date:   Mon May 19 17:23:44 2014 -0700

    Merged in khang-fix-test-2 (pull request #105)
    
    Fix unit test

[33mcommit c7988b1e059ab3031d4ae1aeeabaff2019349b37[m
Author: khangich <khangich@gmail.com>
Date:   Mon May 19 17:21:49 2014 -0700

    fix CreateSharkDataFrameSuite

[33mcommit 62925323ff31ef056e870f2cdd519d1f5b99944c[m
Author: khangich <khangich@gmail.com>
Date:   Mon May 19 17:05:08 2014 -0700

    trim before compare

[33mcommit 3f446c208737479c4554901602176cccaac47bd9[m
Author: khangich <khangich@gmail.com>
Date:   Mon May 19 16:40:13 2014 -0700

    fix unit tests

[33mcommit 965e63f3ec7a76ab0a5f9186f663f30f4a2e92a4[m
Merge: a8171f4 32b35b1
Author: khangpham <khangpham@adatau.com>
Date:   Mon May 19 15:38:35 2014 -0700

    Merged in ckb-pa-tests (pull request #103)
    
    Fixed 10 more test cases in RegressionSuite.

[33mcommit 32b35b114e2f8654426f3569a7a56ab9f757e347[m
Author: khangich <khangich@gmail.com>
Date:   Mon May 19 15:36:01 2014 -0700

    reenable Binning Suite

[33mcommit 5af961c35657eb1b437ed1bd3ceeaa72f9b41aa8[m
Merge: b07ee2a a8171f4
Author: khangich <khangich@gmail.com>
Date:   Mon May 19 14:56:06 2014 -0700

    Merge branch 'master' into ckb-pa-tests

[33mcommit a8171f4248712671644d0012283d400befdc70d4[m
Author: khangich <khangich@gmail.com>
Date:   Mon May 19 14:48:44 2014 -0700

    remove scala-tools repo

[33mcommit 932417dea68b19a39590edb747891a1ffacfbc45[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon May 19 14:42:03 2014 -0700

    remove unused repos

[33mcommit b4b3edc1c8d30ec993ba625721d5f6bfc7020e9a[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon May 19 11:01:27 2014 -0700

    fix imports errors

[33mcommit 5fc052e7e186c82f3ccb6eef17a29ff686754d9b[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon May 19 02:13:50 2014 -0700

    resolving scala version conflicts

[33mcommit ff6b41859e510f9053b7ba9ddb8857bf5e3c3ec1[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon May 19 01:13:59 2014 -0700

    resolving shark-0.9's dependency conflicts

[33mcommit bf0b63ac16a4221c1d84bee58afa12eecd072b2f[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon May 19 00:58:03 2014 -0700

    [wip] port to spark-1.0 and shark-0.9.1

[33mcommit 36a2e9b074cb58a4c65557b0cae27ee00f713781[m
Merge: ebf4597 c99821d
Author: Binh Han <bhan@adatao.com>
Date:   Sat May 17 15:17:36 2014 -0700

    Merged in bhan-changes (pull request #104)
    
    TransformationHandler (cont.)

[33mcommit c99821d116c8b8ad7a3e7d29e2a2d908e0fd0970[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Sat May 17 07:32:55 2014 -0700

    changes to TransformationHandlerTest

[33mcommit 9d8da210928bbaf236590a82d4af27418c5debe8[m
Merge: 4ea108e ebf4597
Author: bhan <binhhan.gt@gmail.com>
Date:   Sat May 17 07:06:16 2014 -0700

    Merge branch 'master' of https://bitbucket.org/adatao/DDF into bhan-changes

[33mcommit 4ea108ec63a6923517fb584e2283aaf66e795533[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Sat May 17 07:05:18 2014 -0700

    passed transformHiveSuite

[33mcommit 4de536b65f4276d81deadca728196046cc522268[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Sat May 17 07:03:35 2014 -0700

    Merge branch 'bhan-transformHive', remote-tracking branch 'origin' into bhan-changes

[33mcommit 21d6ff5a4f7cff7cad4b144d5496d06d26dc472c[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Sat May 17 06:58:10 2014 -0700

    migrate transformHive

[33mcommit 57cd9b0cd45f52fd94526935e527a4418b09ce16[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Fri May 16 15:50:08 2014 -0700

    transformHive

[33mcommit b07ee2aa440dc5c69b3e147f866506bb79dd0955[m
Author: khangich <khangich@gmail.com>
Date:   Thu May 15 18:42:42 2014 -0700

    retire GetFactor --> GetMultiFactor

[33mcommit 2ebe1159df28452517b31353357a3017c4d196da[m
Author: khangich <khangich@gmail.com>
Date:   Thu May 15 17:58:55 2014 -0700

    left 6 unittests

[33mcommit d87f89101681d8d11d524e4b5fe542591583fadd[m
Author: khangich <khangich@gmail.com>
Date:   Thu May 15 17:11:11 2014 -0700

    fix numFeatures

[33mcommit 10c04d9b560744b9d5865ed2a024205ba3064e06[m
Author: khangich <khangich@gmail.com>
Date:   Thu May 15 15:52:39 2014 -0700

    change lamda to make it non singular

[33mcommit 1b3e397edf737ec3032529d2076d799175eded0c[m
Author: khangich <khangich@gmail.com>
Date:   Thu May 15 15:21:41 2014 -0700

    fix model numFeatures in IRLS

[33mcommit a9d81ea09034093985e80721388fe1ee72bd6b7b[m
Merge: 098369b 5f5c868
Author: khangich <khangich@gmail.com>
Date:   Thu May 15 14:29:16 2014 -0700

    Merge branch 'ckb-pa-tests' of bitbucket.org:adatao/ddf into ckb-pa-tests

[33mcommit 5f5c868194bcfee3d27bbdfb5ea7e31ac474301b[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Thu May 15 00:54:22 2014 -0500

    [WIP]

[33mcommit 5c401ee6c45f257cbe4706725e0197d21dfaace2[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Wed May 14 23:37:45 2014 -0500

    Get 10 more test cases works.

[33mcommit 73aaa6111dda49ce0d59dbac64f5ae398f81cd54[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Wed May 14 23:05:55 2014 -0500

    First regression working now.

[33mcommit ebf4597097edc77c69677f91a67f2221f111a4a2[m
Merge: 32fe415 c66c67e
Author: khangpham <khangpham@adatau.com>
Date:   Wed May 14 19:23:12 2014 -0700

    Merged in khang-fix-test (pull request #102)
    
    Fix MetricsSuite, CrossValidationSuite

[33mcommit c66c67e9180ea357416ce295b6dcf76e2065fecc[m
Author: khangich <khangich@gmail.com>
Date:   Wed May 14 19:17:05 2014 -0700

    fix CrossValidationSuite

[33mcommit 1af05a148d9bfca784e11b238c0ddd1572577229[m
Author: khangich <khangich@gmail.com>
Date:   Wed May 14 18:52:42 2014 -0700

    fix MetricsSuite

[33mcommit 32fe41515017c3f9a6e58b8ba5a5c24242ff54f8[m
Author: khangich <khangich@gmail.com>
Date:   Wed May 14 16:39:04 2014 -0700

    renable unit tests

[33mcommit 7045abe0432786d2b32aa7e929e253698aab28e7[m
Author: khangich <khangich@gmail.com>
Date:   Wed May 14 16:07:57 2014 -0700

    add resources file

[33mcommit 098369be38846c5c074e99f9fa028419d6703918[m
Merge: 037dc1c dc4f802
Author: khangich <khangich@gmail.com>
Date:   Wed May 14 16:05:39 2014 -0700

    Merge branch 'master' of bitbucket.org:adatao/ddf into ckb-pa-tests

[33mcommit 037dc1c6e2bb4c4121a00757669d45b4a2b3ad55[m
Author: khangich <khangich@gmail.com>
Date:   Wed May 14 16:04:59 2014 -0700

    add resources file

[33mcommit dca4e12300983a6f759a6c27f4f79bbfc59a1b17[m
Merge: 945fb50 dc4f802
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon May 12 17:17:17 2014 -0700

    Merge branch 'master' of https://bitbucket.org/adatao/DDF into bhan-ddf-mutability

[33mcommit 945fb505aed4fbfe231e3d1c29b5d0b6e32c6453[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon May 12 17:16:06 2014 -0700

    add UDF transfromation on the same DDF

[33mcommit accc5135a55cdb5607b769de6f0a58b3e2d8db26[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Mon May 12 15:39:27 2014 -0500

    Fixed FiveNumSummary method for string column and FiveNumSuite.

[33mcommit 7fb13b60d24f626218ec985a08a0a0032e03992c[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Mon May 12 14:12:10 2014 -0500

    [wip] fixing linear regression.

[33mcommit dc4f80298f58ffd39ad420b04fd70afc36cb386c[m
Merge: 88393d5 49f236b
Author: Binh Han <bhan@adatao.com>
Date:   Thu May 8 15:22:22 2014 -0700

    Merged in bhan-changes (pull request #101)
    
    Fix test suites, clean, comment codes

[33mcommit 49f236b0c6c7c3a1ab9607f212efcf5b02151876[m
Merge: f60bf95 88393d5
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu May 8 15:15:24 2014 -0700

    Merge branch 'master' of https://bitbucket.org/adatao/DDF into bhan-changes

[33mcommit f60bf95e31e02f9ba5f5f76e09300960d5e0ea1a[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu May 8 15:13:57 2014 -0700

    code comments, fix pa tests

[33mcommit 88393d564f8dde539fa8294d9e4d8a27458acef7[m
Merge: fe6d2fe a5e08ae
Author: Cuong Bui <cuongbuikien@gmail.com>
Date:   Thu May 8 00:00:19 2014 -0500

    Merged in ckb-pa-tests (pull request #100)
    
    Refactor PA Tests to DDF engine.

[33mcommit a5e08aeb72cc786961151e362cb0fdc5df74a6b2[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Tue May 6 11:04:38 2014 -0500

    Enable regression suite.

[33mcommit b009acf34a7914cbac75b86e83d1cd64ff55a519[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Mon May 5 10:55:08 2014 -0500

    Fixed SharedSparkContext trait.

[33mcommit d36f88a8138257aa11499e7dfa80ab5b54e4a4dd[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Mon May 5 10:03:35 2014 -0500

    Temporarily disable the TestLoadTable since this test now should be moved to spark and core module.

[33mcommit f3fb19b6e6a3d54bd18e64805130b5c0b40b8e84[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Mon May 5 03:12:40 2014 -0500

    default unknown type to string.

[33mcommit e401d7c24dd9ef0c70572838de39aed085502b11[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Mon May 5 02:16:09 2014 -0500

    Migrate LoadTable executor to a new method loadTable() in DDFManager to fix PA tests.

[33mcommit 2da4b5a39eee9f6208b264da23de70f758f21ec7[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Mon May 5 00:17:00 2014 -0500

    Fixed vector quantiles for corner cases of empty values.

[33mcommit 22086acabc01d3ef5da18beb94cae0126bf5b033[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Apr 24 17:04:43 2014 -0700

    use kmeans mllib, compute tot.withinss

[33mcommit fe6d2fe1e3c2e7a203d502ed9ea18c15ad811eb6[m
Merge: 687208c 385b097
Author: Cuong Bui <cuongbuikien@gmail.com>
Date:   Wed Apr 23 10:02:54 2014 -0500

    Merged in ckb-fix-quantile2 (pull request #99)
    
    Fixed the vector quantile after merge.

[33mcommit 385b09754d645f63165d8037cc289d564f416cc2[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Wed Apr 23 10:01:22 2014 -0500

    Fixed the vector quantile after merge.

[33mcommit 687208c5dffd672727ea565e00757d3f8436c828[m
Merge: 69723cc f18d151
Author: huandao0812 <huan@adatau.com>
Date:   Tue Apr 22 12:57:54 2014 -0700

    Merged in ddh-factor (pull request #98)
    
    throw proper exception for get factor

[33mcommit f18d15120679fa52fc2ee8cd2da1497140ee2c17[m
Merge: 221836d 69723cc
Author: huandao0812 <huan@adatau.com>
Date:   Mon Apr 21 12:13:39 2014 -0700

    Merge branch 'master' into ddh-factor
    
    Conflicts:
    	spark/src/main/scala/com/adatao/spark/ddf/content/GetMultiFactor.scala

[33mcommit 221836de020e709413eff7896ec23a3f1c8abc15[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Apr 21 12:06:32 2014 -0700

    throw proper exception for get factor

[33mcommit 69723cc1c9aa2654fdf67581033c87ce34c326ff[m
Merge: 89d1a3b c8d2fab
Author: Binh Han <bhan@adatao.com>
Date:   Sat Apr 19 13:07:08 2014 -0700

    Merged in clean-refactor (pull request #97)
    
    Clean up code, re-factoring and new unit tests

[33mcommit c8d2fabaecb947babbf1e1fc496b5ec0725ca85a[m
Author: khangpham <khangpham@adatau.com>
Date:   Sat Apr 19 11:14:52 2014 -0700

    refactor lm ormal equation

[33mcommit 902d95b3a9b42378be8d1603a68d4446bff0cab4[m
Merge: 2c8a85b 89d1a3b
Author: khangpham <khangpham@adatau.com>
Date:   Sat Apr 19 10:56:59 2014 -0700

    Merge branch 'master' into clean-refactor

[33mcommit 2c8a85b85f8fd57de504802a8ec2d0c5bc444715[m
Author: khangpham <khangpham@adatau.com>
Date:   Fri Apr 18 11:19:03 2014 -0700

    refactor and cleanup

[33mcommit fb8fd9e3a9528591013045dd29d2e41b7b00fcee[m
Author: khangpham <khangpham@adatau.com>
Date:   Fri Apr 18 10:58:14 2014 -0700

    reformat, use helper method to convert dataContainerID to ddf name

[33mcommit 2e0bdd0f912c9612f74df70d98864c074d3028cc[m
Author: khangpham <khangpham@adatau.com>
Date:   Fri Apr 18 10:45:51 2014 -0700

    add helper function to convert dcID to ddfID

[33mcommit a7413ac79441a4a340be3016cbce8bf34f969168[m
Author: khangpham <khangpham@adatau.com>
Date:   Fri Apr 18 10:36:27 2014 -0700

    reformat, remove projection in BinaryConfusionMatrix

[33mcommit 89d1a3b29b848a339a75b69ade8a69e9024cb735[m
Merge: e7b78ed 736f7c8
Author: Cuong Bui <cuongbuikien@gmail.com>
Date:   Thu Apr 17 11:26:42 2014 -0500

    Merged in ckb-fix-quantiles (pull request #96)
    
    Fixed vector quantiles

[33mcommit 736f7c8487d0aecd41c302b5f8132540a945dacc[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Thu Apr 17 11:25:43 2014 -0500

    merging.

[33mcommit 75421d5a0fd76a6116de417bef47c3d59c62de38[m
Merge: 421ec74 e7b78ed
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Thu Apr 17 11:19:41 2014 -0500

    Merge with master to resolve conflicts.

[33mcommit 421ec74b5c18599e2dc7dff5def57a259f6b5ad7[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Thu Apr 17 11:11:32 2014 -0500

    Fixed vector quantiles corner cases of 0.0 and 1.0

[33mcommit e7b78ede5d46901221a87de93726767e8a451755[m
Merge: 9f72b5c ee41611
Author: Binh Han <bhan@adatao.com>
Date:   Thu Apr 17 02:03:58 2014 -0700

    Merged in bhan-changes (pull request #95)
    
    Migrate Aggregate executor

[33mcommit ee41611069a8325b7eea07b79ee3e58c2a745423[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Apr 17 00:32:11 2014 -0700

    code cleanup, pass AggregateSuite test

[33mcommit 1dda55efed1576d40ee4a3b504d1246f2acaacdf[m
Merge: 620bade 9f72b5c
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Apr 17 00:27:33 2014 -0700

    Merge branch 'master' of https://bitbucket.org/adatao/DDF into bhan-changes

[33mcommit 620badea139252850f1a07b5ebfc73487ea7b845[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Apr 17 00:26:51 2014 -0700

    Rewrite Aggregate executor in java

[33mcommit 9f72b5c96dc351f32ff01e74366ad3b165957e33[m
Author: khangpham <khangpham@adatau.com>
Date:   Wed Apr 16 22:52:34 2014 -0700

    fix nFeatures

[33mcommit bb8f9e69a60a8f9582b6f46a7df406628313d3ba[m
Merge: 7d17d17 1d3670c
Author: khangpham <khangpham@adatau.com>
Date:   Wed Apr 16 21:57:02 2014 -0700

    Merged in dc-for-glm (pull request #94)
    
    dc for glm.gd, lm

[33mcommit 1d3670cda07fdca7405021f984392d4b155c6ead[m
Author: khangpham <khangpham@adatau.com>
Date:   Wed Apr 16 21:52:45 2014 -0700

    dc for glm.gd, lm

[33mcommit 7d17d17cf5111e95849dbb9f11d0fe7e13fff776[m
Merge: 3c1f93c 49dafc4
Author: Binh Han <bhan@adatao.com>
Date:   Wed Apr 16 20:19:14 2014 -0700

    Merged in bhan-MapReduceNative (pull request #91)
    
    Transform MapReduceNative

[33mcommit 49dafc4f09ac8391f43153c37523c32bca63cf64[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Apr 16 20:18:00 2014 -0700

    code cleanup

[33mcommit bfc8f2cfa60a60d0bec17c33e4760b4f91a39508[m
Merge: b9bc40e 29e88e9
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Apr 16 19:15:39 2014 -0700

    Merge branch 'bhan-MapReduceNative' of https://bitbucket.org/adatao/DDF into bhan-MapReduceNative

[33mcommit b9bc40edd8fe897d3068e56ce7e19dfbc9b93d9f[m
Merge: be95691 3c1f93c
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Apr 16 19:14:56 2014 -0700

    Merge branch 'master' of https://bitbucket.org/adatao/DDF into bhan-MapReduceNative

[33mcommit 3c1f93c67a0d03ff1e7691d8b8216fc22e5e9733[m
Merge: 2ea1467 a82ed1e
Author: Binh Han <bhan@adatao.com>
Date:   Wed Apr 16 19:10:40 2014 -0700

    Merged in bhan-changes (pull request #93)
    
    Small fix to binning

[33mcommit a82ed1ed1ad4ad70077e569093ec1f635bfbb5d2[m
Merge: b5ed23f 2ea1467
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Apr 16 19:03:07 2014 -0700

    Merge branch 'master' of https://bitbucket.org/adatao/DDF into bhan-changes

[33mcommit 2ea14679db837e3f3685543ed028e9e477e17217[m
Merge: e1750b1 3bec96b
Author: khangpham <khangpham@adatau.com>
Date:   Wed Apr 16 19:01:55 2014 -0700

    Merged in migration-dc2 (pull request #92)
    
    Merge unmerged dummy coding

[33mcommit 3bec96b8be3dfbab6d33d739e6744ee53774fed2[m
Merge: 286e964 e1750b1
Author: khangpham <khangpham@adatau.com>
Date:   Wed Apr 16 19:00:35 2014 -0700

    merge fixed dummy coding to master

[33mcommit b5ed23f08055d7620bf637b2db70008a50d12477[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Apr 16 18:35:15 2014 -0700

    more Rclient testcases for binning

[33mcommit 29e88e9fb6db6a670f496d2fb181e5542a648455[m
Merge: be95691 e1750b1
Author: Binh Han <bhan@adatao.com>
Date:   Wed Apr 16 17:25:39 2014 -0700

    Merged master into bhan-MapReduceNative

[33mcommit e1750b1a40d2d7a041934b82a04015ae5ac281b2[m
Merge: 9b7ba63 c40e8c4
Author: khangpham <khangpham@adatau.com>
Date:   Wed Apr 16 16:52:33 2014 -0700

    Merged in fix-quantile (pull request #90)
    
    Fix vector quantile on double column

[33mcommit c40e8c41b741d86f0ac9690dae620e9bcb788f84[m
Merge: e022f13 9b7ba63
Author: khangpham <khangpham@adatau.com>
Date:   Wed Apr 16 16:50:51 2014 -0700

    Merge branch 'master' into fix-quantile

[33mcommit e022f1359eb9997664c120f1f3ad1de24f5f9483[m
Author: khangpham <khangpham@adatau.com>
Date:   Wed Apr 16 16:49:48 2014 -0700

    fix regex

[33mcommit 48049fe59041138fce34fcbb7b2a0bbd97593d41[m
Author: khangpham <khangpham@adatau.com>
Date:   Wed Apr 16 16:43:32 2014 -0700

    fix vector quantile for probs contains 0.0 on double column

[33mcommit 9b7ba63bf9025b406f2c3d6d41581cf3d4126de1[m
Merge: 6190c54 3184f47
Author: Binh Han <bhan@adatao.com>
Date:   Wed Apr 16 15:21:45 2014 -0700

    Merged in bhan-changes (pull request #89)
    
    Sample2DataFrame Executor

[33mcommit 3184f47d7a12d68eda63e05999e58e62ed41ee4e[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Apr 16 15:20:30 2014 -0700

    Rclient tested

[33mcommit be95691fa1c82242a4c333ecb05dc760bb41fc88[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Apr 16 14:34:49 2014 -0700

    debug pa

[33mcommit 0ada59814db4dc9f4e765ab3bb6c5ff4d959245b[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Apr 16 12:30:42 2014 -0700

    set hive-site.xml to use hive metastore
    
    test failed because DDF create by randomSample was not backed by a
    shark table

[33mcommit 6190c542152d6a5ab0f5101eda782b804798fc6b[m
Merge: 3b0be14 b4f1e64
Author: khangpham <khangpham@adatau.com>
Date:   Wed Apr 16 12:02:18 2014 -0700

    Merged in debug-r2 (pull request #88)
    
    fix r2 score, do not include features

[33mcommit b4f1e645f524b35c868db41e580d7bafaece9510[m
Author: khangpham <khangpham@adatau.com>
Date:   Wed Apr 16 11:59:36 2014 -0700

    fix r2 score, do not include features

[33mcommit 6c87ec46dfecb25c485f687f31a8af4f0d581c88[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Apr 16 11:44:39 2014 -0700

    add executor for SampleDataFrame
    
    test not passed, still has bug

[33mcommit 3b0be14246b8c207f6fff9472259bad6f32def61[m
Author: khangpham <khangpham@adatau.com>
Date:   Wed Apr 16 10:00:55 2014 -0700

    roc fix ddf-id

[33mcommit f7b231eedb2e5b61bece9b70bff4c466d52b1f15[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue Apr 15 23:38:45 2014 -0700

    wip

[33mcommit 2fe0a24de5fd62b6c7dde528b48c62b811564282[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue Apr 15 23:36:46 2014 -0700

    passed unit test

[33mcommit dcae5ad25c2407d888b2b353229cc558b4914a94[m
Author: Nhan Vu <nhanitvn@gmail.com>
Date:   Tue Apr 15 23:23:03 2014 -0700

    changes to Sampling to use new api

[33mcommit 23dee9528165e659a2a77f3d120882a60f4a7cbe[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue Apr 15 18:33:32 2014 -0700

    changes to pa test

[33mcommit 74aa29f94b07308c56df2f322b691c456ea83665[m
Merge: 221eb14 7c411aa
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue Apr 15 18:15:24 2014 -0700

    Merge branch 'master' of https://bitbucket.org/adatao/DDF into bhan-MapReduceNative

[33mcommit 221eb14f4720939a3baa6ddb8eb6b1b46e136423[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue Apr 15 18:13:08 2014 -0700

    code completed, added test

[33mcommit 98317fc4ce407b092563817b0230a02aa5e11b09[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue Apr 15 18:10:14 2014 -0700

    MapReduceNative impl

[33mcommit 286e964a0bb38614be4456697ddac01400e5f2e9[m
Merge: 2d8d7c4 7c411aa
Author: khangpham <khangpham@adatau.com>
Date:   Tue Apr 15 17:23:11 2014 -0700

    Merge branch 'master' into migration-dc

[33mcommit 7c411aa33f39b393144ae9fcf9cf140e87e0a4d7[m
Merge: 9d7a6dc 4f3ad46
Author: khangpham <khangpham@adatau.com>
Date:   Tue Apr 15 17:18:43 2014 -0700

    Merged in migration-dc (pull request #85)
    
    Support dummy coding when build model

[33mcommit 2d8d7c419056c3b5bb650c39bca18eb6f92dcd96[m
Author: khangpham <khangpham@adatau.com>
Date:   Tue Apr 15 17:12:12 2014 -0700

    pass test

[33mcommit 6ead3d97d3ed4b00aadb0726891bad2907768057[m
Merge: 062b38b 9d7a6dc
Author: khangpham <khangpham@adatau.com>
Date:   Tue Apr 15 14:09:04 2014 -0700

    Merge branch 'master' into migration-dc

[33mcommit 9d7a6dcb25fdf5562c88bcf432ca4393380bdc14[m
Merge: d6ab20a f3a3f0c
Author: khangpham <khangpham@adatau.com>
Date:   Tue Apr 15 14:06:57 2014 -0700

    Merged in ddh-multi-factor (pull request #84)
    
    refactor PA GetMultiFactor executor to use DDF's

[33mcommit d6ab20ab902442b5173233e96d220b8cb2bd429f[m
Merge: 11b99c1 b36d96b
Author: Cuong Bui <cuongbuikien@gmail.com>
Date:   Tue Apr 15 14:28:35 2014 -0500

    Merged in ckb-fix-cm (pull request #86)
    
    Fixed the confusion matrix executor to work with "new" way of getting DDF model

[33mcommit b36d96b345fccac304ea8c8f2c9328a423f1cefc[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Tue Apr 15 14:12:41 2014 -0500

    Clean up the code.

[33mcommit 7c3091402fbeecb88be9c0bd2b6ddd6cafeb7c07[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Tue Apr 15 14:10:40 2014 -0500

    Fixed confusion matrix get model.

[33mcommit 062b38bd9c0713808e3ac52a787cbf92bfc0c958[m
Author: khangpham <khangpham@adatau.com>
Date:   Tue Apr 15 11:59:43 2014 -0700

    refactor generateDummyoding

[33mcommit 4f3ad469f577413c1f77bf24b9d6d9703b1dcbcc[m
Author: khangpham <khangpham@adatau.com>
Date:   Tue Apr 15 11:50:55 2014 -0700

    dummy coding reuse levelMap from Factor

[33mcommit f3a3f0cb07892ed3338c62e16ef620c483a9267f[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Apr 15 11:47:00 2014 -0700

    change get -> computeFactorLevelsAndLevelCounts

[33mcommit 2a945422dde1fcb4264000f802cf123581746a78[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Apr 15 11:14:56 2014 -0700

    refactor PA GetMultiFactor executor to use DDF's

[33mcommit ceab62f59c687d4211e65e069969492415ec3e80[m
Author: khangpham <khangpham@adatau.com>
Date:   Tue Apr 15 11:14:18 2014 -0700

    format

[33mcommit b2a94deccabd33211b7910f0dac8fcd39a75b5a5[m
Merge: a30e6f7 11b99c1
Author: khangpham <khangpham@adatau.com>
Date:   Tue Apr 15 11:09:32 2014 -0700

    remove hardcode, refactor logistic regression for sparse

[33mcommit a30e6f7c8fdec197b5903db4a49d7c86c7814fad[m
Author: khangpham <khangpham@adatau.com>
Date:   Tue Apr 15 11:00:08 2014 -0700

    reverse change in metadata

[33mcommit 40c8edce3a6722750a40651ba49948820efaa06d[m
Author: khangpham <khangpham@adatau.com>
Date:   Tue Apr 15 10:55:55 2014 -0700

    refactor and compiled

[33mcommit ec69a75d2a4388865b680a02e99a3c04ea935b3b[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Apr 15 10:30:48 2014 -0700

    delete LOG message

[33mcommit 49a478c71f2e82c40b17762b46c680a4b81033d6[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Apr 15 03:47:29 2014 -0700

    add method to get factor levels and level counts
    
    test passed

[33mcommit 11b99c1554097775c5fd05458773f2bdd77eeed1[m
Merge: 765911e 2898b6b
Author: Binh Han <bhan@adatao.com>
Date:   Tue Apr 15 00:51:23 2014 -0700

    Merged in bhan-transformScaleStandard (pull request #83)
    
    Transform ScaleStandard

[33mcommit 2898b6bef4b5e5b2b868b3806e963e512fb4e950[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue Apr 15 00:47:36 2014 -0700

    add pa test, passed

[33mcommit 07a59c1ffaa6f8ec834e64d01c76cb16d68e5a1c[m
Author: khangpham <khangpham@adatau.com>
Date:   Tue Apr 15 00:31:51 2014 -0700

    refactor

[33mcommit dff2f9a7c861203bf2c10ac80baaf3cf05399a25[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon Apr 14 23:25:07 2014 -0700

    ddf unittest passed

[33mcommit 93562d480124ea3d5e4051de7be2d5fdf6bb4176[m
Merge: b8f6a9b 765911e
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon Apr 14 22:55:20 2014 -0700

    Merge branch 'master' of https://bitbucket.org/adatao/DDF into bhan-transformScaleStandard

[33mcommit 765911e69ccf97e1a6dd102b20db66c8655437b3[m
Merge: 0b29552 32b526c
Author: Binh Han <bhan@adatao.com>
Date:   Mon Apr 14 22:51:37 2014 -0700

    Merged in bhan-transformScaleMinMax (pull request #82)
    
    Transform Scale MinMax

[33mcommit 32b526c2fe13a2b288498004e04554ca3fa9c23a[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon Apr 14 22:40:42 2014 -0700

    finish integration

[33mcommit eb70fb73c9eee3b00d15aca58a23fa8089d11d12[m
Merge: ad7c43e 0b29552
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon Apr 14 22:24:12 2014 -0700

    Merge branch 'master' of https://bitbucket.org/adatao/DDF into bhan-transformScaleMinMax

[33mcommit ad7c43ec46bcef065196c30c8853d737ecdd2780[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon Apr 14 22:23:20 2014 -0700

    pa tests passed

[33mcommit 63de46e60295c0c960f7239e9317bed1b99d8ec1[m
Author: khangpham <khangpham@adatau.com>
Date:   Mon Apr 14 20:16:20 2014 -0700

    working version

[33mcommit 0b295524abff08c2132973e9b095f454a4b89d34[m
Merge: 44672ac 6f37f30
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Mon Apr 14 22:03:01 2014 -0500

    Merged master into ckb-confusion-matrix to resolve PR #75

[33mcommit 6f37f30cbea905bcfd8932d6dc4e745bc1fbb595[m
Merge: ae9af81 fd84f62
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Mon Apr 14 21:23:33 2014 -0500

    Merge branch 'ckb-confusion-matrix'

[33mcommit df401ed75216ec82f2b6af194337adb0b7c0f1be[m
Merge: 993145b ae9af81
Author: khangpham <khangpham@adatau.com>
Date:   Mon Apr 14 12:38:09 2014 -0700

    merge with master

[33mcommit ae9af811ceed1a525d757aa3ce64659d3e3e995b[m
Merge: af8bc2a 840dae9
Author: khangpham <khangpham@adatau.com>
Date:   Mon Apr 14 11:38:07 2014 -0700

    Merged in ckb-glm-gd (pull request #81)
    
    Done with linear regression normal equation.

[33mcommit 840dae9e0102442a955fb72d29605bfbbc86e17c[m
Merge: 01b3183 af8bc2a
Author: khangpham <khangpham@adatau.com>
Date:   Mon Apr 14 11:36:33 2014 -0700

    resolve conflicts

[33mcommit 993145b8f60185bcc8e5ddf62d068e335c80e2b5[m
Author: khangpham <khangpham@adatau.com>
Date:   Mon Apr 14 10:14:49 2014 -0700

    wip 2

[33mcommit 01b318358d8efcd3a3b82ac53aa92dfdcb7758d8[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Mon Apr 14 11:09:06 2014 -0500

    Done with LogisticRegressionWithGD migration.

[33mcommit 3a785662422a47714746ce259357277ed7e84983[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Mon Apr 14 10:54:55 2014 -0500

    Done with LinearRegressionWithGD

[33mcommit 0cb23a6cb49328de4500c51f68c30a34a394a459[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Mon Apr 14 09:54:01 2014 -0500

    Done with migrating LinearRegressionNormalEquation.

[33mcommit cdb8681226388aabad497995ecfcd858b09d0b75[m
Author: khangpham <khangpham@adatau.com>
Date:   Mon Apr 14 00:41:48 2014 -0700

    wip

[33mcommit 0a342ba28017a3ad642a8093dffc6e43e838153b[m
Merge: 8a23542 af8bc2a
Author: bhan <binhhan.gt@gmail.com>
Date:   Sun Apr 13 16:03:10 2014 -0700

    Merge branch 'master' of https://bitbucket.org/adatao/DDF into bhan-transformScaleMinMax
    
    Conflicts:
    	core/src/main/java/com/adatao/ddf/DDF.java
    	core/src/main/java/com/adatao/ddf/etl/IHandleTransformations.java
    	core/src/main/java/com/adatao/ddf/etl/TransformationHandler.java
    	spark/src/main/scala/com/adatao/spark/ddf/content/RepresentationHandler.scala

[33mcommit af8bc2a555100b543d042e806fafc75df9b0cc81[m
Merge: c28242e ea92234
Author: Binh Han <bhan@adatao.com>
Date:   Sun Apr 13 15:16:10 2014 -0700

    Merged in bhan-transformNativeRserve (pull request #80)
    
    Transform Native Rserve

[33mcommit 8a23542762d3c1ffb6e2db5b66017b48ae8cc546[m
Merge: 3847547 c28242e
Author: bhan <binhhan.gt@gmail.com>
Date:   Sun Apr 13 13:11:31 2014 -0700

    Merge branch 'master' of https://bitbucket.org/adatao/DDF into bhan-transformScaleMinMax
    
    Conflicts:
    	spark/src/main/scala/com/adatao/spark/ddf/content/RepresentationHandler.scala

[33mcommit ea9223409d78a9467b2bf2cd24b829a84c45e0f6[m
Merge: 0005672 c28242e
Author: bhan <binhhan.gt@gmail.com>
Date:   Sun Apr 13 13:02:53 2014 -0700

    Merge branch 'master' of https://bitbucket.org/adatao/DDF into bhan-transformNativeRserve

[33mcommit c28242e3e2671c4c9637976cd29c6358e7c9d390[m
Merge: 9006f9d 290a6aa
Author: huandao0812 <huan@adatau.com>
Date:   Sat Apr 12 12:50:56 2014 -0700

    Merged in ddh-RepH-TablePart (pull request #78)
    
    add TablePartition to RepHandler in sql2rdd

[33mcommit 0005672c335717c18d6f89f6f544e93e6bc3c427[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Sat Apr 12 02:33:55 2014 -0700

    (wip) rclient integration

[33mcommit 6db4b94cd23034be7dcf433066d6dd713f18ac17[m
Merge: fbf16f2 9006f9d
Author: bhan <binhhan.gt@gmail.com>
Date:   Fri Apr 11 17:53:35 2014 -0700

    Merge branch 'master' of https://bitbucket.org/adatao/DDF into bhan-transformNativeRserve

[33mcommit 0a06b8360337a24119ffb30becb2c6cacc3d7eea[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Fri Apr 11 12:38:57 2014 -0500

    [wip]

[33mcommit 9290fd38fc8c84ad9ea85003a7f2da24d38b95cc[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Fri Apr 11 11:01:32 2014 -0500

    [WIP] Some how the toJson of the NQ model doesn't invoke vector.toJson

[33mcommit 9006f9d65a021c592f15e642f570f8fe1299ad1d[m
Merge: 9bbe068 570c01e
Author: nhanitvn <nhanitvn@gmail.com>
Date:   Fri Apr 11 00:01:44 2014 -0700

    Merged in nhan-logisticregression-irls (pull request #70)
    
    Move pA LogisticRegressionIRLS logic to DDF and refactor corresponding executor

[33mcommit 570c01e2abe9565dfd1629599716ec96b4b03b9c[m
Author: Nhan Vu <nhanitvn@gmail.com>
Date:   Thu Apr 10 23:59:41 2014 -0700

    handle confliction

[33mcommit 308fcf332f6ed2991aeedcbc42a105d56b0e6977[m
Merge: 18234e9 9bbe068
Author: Nhan Vu <nhanitvn@gmail.com>
Date:   Thu Apr 10 23:59:14 2014 -0700

    merge master

[33mcommit fbf16f29634c11455378d95ce86e5492aa4becbc[m
Merge: fa3fde5 9bbe068
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Apr 10 20:31:24 2014 -0700

    Merge branch 'master' of https://bitbucket.org/adatao/DDF into bhan-transformNativeRserve

[33mcommit fa3fde5e949bd876c566a28d755a7fffe009bd86[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Apr 10 20:30:10 2014 -0700

    unit test passed

[33mcommit 9bbe068454302bd102a9c827d657581c9b96a71e[m
Merge: 0cbb611 5be9f83
Author: khangpham <khangpham@adatau.com>
Date:   Thu Apr 10 19:23:29 2014 -0700

    Merged in migration-roc (pull request #79)
    
    Pass unit test

[33mcommit 5be9f83472783b535524f9920a220af9fbcb9454[m
Author: khangpham <khangpham@adatau.com>
Date:   Thu Apr 10 19:22:13 2014 -0700

    pass roc, pass ytrueypredict execution

[33mcommit 24fb4d0ce2b407d9ef74d36184d783b92bc81c99[m
Merge: 86a4310 4dda6f3
Author: khangpham <khangpham@adatau.com>
Date:   Thu Apr 10 16:21:34 2014 -0700

    wip

[33mcommit 290a6aa95a2fb60fe4abe7f1e4002021819cf0e4[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Apr 10 16:06:11 2014 -0700

    add test for RepresentationHandler

[33mcommit 1da39385a2571326b7a3cc9aa053b82dd2024cf6[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Apr 10 15:55:56 2014 -0700

    wip

[33mcommit 4371e344b01a1743cc2e4d2ad8182b472b74a510[m
Merge: 1b47d2c c109136
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Apr 10 15:21:04 2014 -0700

    Merge branch 'ddh-RepH-TablePart' of https://bitbucket.org/adatao/DDF into bhan-transformNativeRserve
    
    Conflicts:
    	spark/src/main/java/com/adatao/spark/ddf/etl/SqlHandler.java

[33mcommit 1b47d2c4643c8c85d317d595d7bda3e4b93341c6[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Apr 10 15:19:58 2014 -0700

    test (wip)

[33mcommit 86a4310a0b2306fc659a0cee8cd497254e916589[m
Merge: f41ca62 0cbb611
Author: khangpham <khangpham@adatau.com>
Date:   Thu Apr 10 14:39:40 2014 -0700

    Merge branch 'master' into migration-roc

[33mcommit f41ca624aa456c5755f108b8cc57462532c69d04[m
Author: khangpham <khangpham@adatau.com>
Date:   Thu Apr 10 14:34:58 2014 -0700

    add serialize to model

[33mcommit 4dda6f34ab0643c96a586d67ea1e28dd967a49e8[m
Author: khangpham <khangpham@adatau.com>
Date:   Thu Apr 10 14:19:35 2014 -0700

    compiled

[33mcommit c109136f2bd02a70190781468e0787ebab0c6462[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Apr 10 14:07:24 2014 -0700

    get TablePartition from SharkEnv

[33mcommit a6e6da4ae30eacb6b85ef4c38649ed8ecc5da50e[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Thu Apr 10 15:02:27 2014 -0500

    Fixing the serialization of Vector.

[33mcommit ff2e85fa045100c202cca3f6a0f20c2b32274b4f[m
Merge: 6b0273a 0cbb611
Author: huandao0812 <huan@adatau.com>
Date:   Thu Apr 10 12:11:04 2014 -0700

    Merge branch 'master' into ddh-RepH-TablePart
    
    Conflicts:
    	spark/src/main/java/com/adatao/spark/ddf/etl/SqlHandler.java

[33mcommit 6b0273a9473353d03f7da16a56422161cce22c24[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Apr 10 12:07:45 2014 -0700

    add test for Row and TablePartition

[33mcommit 0cbb611f9711cf3b75abbabaa1658ba832e0650e[m
Merge: bcd7b63 d34a975
Author: huandao0812 <huan@adatau.com>
Date:   Thu Apr 10 12:07:21 2014 -0700

    Merged in ddh-cv (pull request #69)
    
    add CrossValidation for DDF

[33mcommit 3b5a7fa09d35a07d206dceffae7463f330abbec7[m
Merge: ece099b e3b6eb9
Author: huandao0812 <huan@adatau.com>
Date:   Thu Apr 10 11:50:20 2014 -0700

    Merge branch 'ddh-fix-RepHandler' into ddh-RepH-TablePart

[33mcommit ece099b53153f53d8cb9db0881c78a7724c57f25[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Apr 10 11:48:24 2014 -0700

    add mapper to get TablePartition

[33mcommit b307cedbda7749a56af3601f81a9311e9245094f[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Apr 10 11:33:48 2014 -0700

    add TablePartition to RepHandler in sql2rdd

[33mcommit e3b6eb94263839dc25464104c3c3ce339aab8e37[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Apr 10 11:27:18 2014 -0700

    WIP

[33mcommit df293db2e60f7cd9cf47073b85c94ddb6d81d243[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Thu Apr 10 12:31:09 2014 -0500

    Fixed kryo registrator.

[33mcommit 847834925257acf1cfc3d4e174de680af6d9387d[m
Merge: f38c5d5 bcd7b63
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Apr 9 19:08:37 2014 -0700

    Merge branch 'master' of https://bitbucket.org/adatao/DDF into bhan-transformNativeRserve

[33mcommit f38c5d5b31d4caa063e1ec17b05aecac28a9bd3e[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Apr 9 19:08:09 2014 -0700

    rewrite TransformNativeRserve executor in java

[33mcommit bcd7b63809ecc82e5cb9d3643d7c02764adf9cbe[m
Author: khangpham <khangpham@adatau.com>
Date:   Wed Apr 9 18:45:29 2014 -0700

    fix hive-site.xml in sark to use /tmp/hive

[33mcommit 740ddb2c8a240a2c68f825e7662ee2de11d10cb2[m
Merge: c2010f5 9c6a6d7
Author: Binh Han <bhan@adatao.com>
Date:   Wed Apr 9 18:33:07 2014 -0700

    Merged in bhan-changes (pull request #77)
    
    Fix test cases to take FetchRows result as List<String>

[33mcommit 9c6a6d7059ce47dfbad232a0fcbdf045cdfed4ed[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Apr 9 18:31:35 2014 -0700

    fix test cases to take List<String> as FetchRow result

[33mcommit 3dc7474ca415d666b7d79f531941183313cda794[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Apr 9 18:26:20 2014 -0700

    add TransformFacade

[33mcommit 8d5ae6e45501a8b2de8614564b374ebb1ca319dc[m
Merge: 229571c c2010f5
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Apr 9 17:12:25 2014 -0700

    Merge branch 'master' of https://bitbucket.org/adatao/DDF into bhan-transformNativeRserve
    
    Conflicts:
    	core/src/main/java/com/adatao/ddf/DDF.java
    	spark/src/main/scala/com/adatao/spark/ddf/content/RepresentationHandler.scala

[33mcommit c2010f56bc6ddd165c3960d4f8dabb9082e77455[m
Merge: 7308c2d 49d888e
Author: khangpham <khangpham@adatau.com>
Date:   Wed Apr 9 17:09:03 2014 -0700

    Merged in migration-roc (pull request #76)
    
    migrate roc

[33mcommit b8f6a9b2dae4576e0c19be0f756f41df1222da07[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Apr 9 17:06:17 2014 -0700

    migrate TransformScaleStandard executor

[33mcommit 38475470f1318550ca040705b25322d44828b699[m
Merge: 61890d4 7308c2d
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Apr 9 17:04:28 2014 -0700

    Merge branch 'master' of https://bitbucket.org/adatao/DDF into bhan-transformScaleMinMax
    
    Conflicts:
    	core/src/main/java/com/adatao/ddf/DDF.java

[33mcommit 61890d4d049693a7988031d5b2c98c120d004b8e[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Apr 9 17:02:02 2014 -0700

    implement TransformScaleMinMax executor

[33mcommit 46843219132698737d5b701723e2407badfa07c2[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Apr 9 17:01:03 2014 -0700

    migrate transformScaleMinMax to ddf

[33mcommit 49d888ebfb4431764bad878ea337a0638a1becfc[m
Merge: ec3d0b9 7308c2d
Author: khangpham <khangpham@adatau.com>
Date:   Wed Apr 9 16:57:15 2014 -0700

    Merge branch 'master' into migration-roc

[33mcommit ec3d0b901802f764fc7cc241ed96bccfda3d49d1[m
Author: khangpham <khangpham@adatau.com>
Date:   Wed Apr 9 16:55:13 2014 -0700

    chang unit test

[33mcommit d2ff4b6320473047e2f0a3ae8afbb0cb9feb2ba6[m
Author: khangpham <khangpham@adatau.com>
Date:   Wed Apr 9 16:51:50 2014 -0700

    move Array[Array[double]] to Array[LabeledPoint]

[33mcommit 562631d7caec88ee3360fa768225bd20fc2b4e6e[m
Merge: 9af48fd 7f80845
Author: khangpham <khangpham@adatau.com>
Date:   Wed Apr 9 16:00:01 2014 -0700

    compiled

[33mcommit 7308c2d2f05ad162043832d06a7b303754103910[m
Merge: 7f80845 bdfa09d
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Wed Apr 9 14:11:45 2014 -0500

    Resolved conflicts and merged with ckb-pa-subset branch.

[33mcommit 44672accb8c388597c05c6cdb6fc7e39188a3d67[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Wed Apr 9 12:57:07 2014 -0500

    Done with confusion matrix.

[33mcommit d96df42d88c4607d710be34eb717dd09dac886ac[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Wed Apr 9 12:18:59 2014 -0500

    Completed the test case for confusion matrix.

[33mcommit 7f8084527851515289d00cbc42a145e4b8133554[m
Merge: 175350f 6fc6080
Author: khangpham <khangpham@adatau.com>
Date:   Wed Apr 9 10:16:13 2014 -0700

    Merged in residuals (pull request #74)
    
    Migrate residuals

[33mcommit 568e907179c12601f0633a7a1f8af83a1e465834[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Wed Apr 9 12:05:06 2014 -0500

    Fixed the MLSupporter error on serializable and add a unit test for confusion matrix.

[33mcommit 6fc60802537e80ea0ed67b37582444fced7fe83c[m
Merge: 4387e73 175350f
Author: khangpham <khangpham@adatau.com>
Date:   Wed Apr 9 09:48:28 2014 -0700

    Merge branch 'master' into residuals

[33mcommit 229571cca00008a5dfa0c13f57631fac05fedce3[m
Merge: ece0544 175350f
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Apr 9 00:48:49 2014 -0700

    Merge branch 'master' of https://bitbucket.org/adatao/DDF into bhan-transformNativeRserve

[33mcommit 175350fda5f12040f75fa8c3462f4aca815a1dda[m
Merge: 2bb8fcf 5369bc3
Author: Binh Han <bhan@adatao.com>
Date:   Tue Apr 8 18:16:15 2014 -0700

    Merged in bhan-changes (pull request #73)
    
    Migrate FetchRows executor

[33mcommit 5369bc37f45afe39854d707e05956064184f3d5c[m
Merge: 7380374 2bb8fcf
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue Apr 8 18:11:23 2014 -0700

    Merge branch 'master' of https://bitbucket.org/adatao/DDF into bhan-changes

[33mcommit 73803747b95dbe4e079845cb3245164cabaf1a98[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue Apr 8 15:20:50 2014 -0700

    migrate FetchRows executor

[33mcommit 2bb8fcfcaa3fae0b4b3b017966de90ec6c8dbcd7[m
Merge: 9b7add9 f42f9dc
Author: khangpham <khangpham@adatau.com>
Date:   Tue Apr 8 15:11:33 2014 -0700

    Merged in r2score (pull request #72)
    
    R2score migration

[33mcommit 9b7add937ff9592912d9b68a92b53a7e69e1731e[m
Merge: adee5aa afc6d40
Author: khangpham <khangpham@adatau.com>
Date:   Tue Apr 8 15:10:01 2014 -0700

    Merged in nhan-fix-sparkthread (pull request #71)
    
    There is no sparkContext, thus we have to shutdown ddfManager instead in stopMe()

[33mcommit f42f9dc3e4fc0ca98b8d389d01598832c13b3787[m
Author: khangpham <khangpham@adatau.com>
Date:   Tue Apr 8 15:04:34 2014 -0700

    remove println, add handle exception

[33mcommit afc6d40d7f0b684c1f1373a2773a1741e490ea5b[m
Author: Nhan Vu <nhanitvn@gmail.com>
Date:   Tue Apr 8 14:56:17 2014 -0700

    There is no sparkContext, thus we have to shutdown ddfManager instead in stopMe

[33mcommit cbeb5795c6e4bfe13238bf8d17a1007e4207f0db[m
Author: khangpham <khangpham@adatau.com>
Date:   Tue Apr 8 14:25:07 2014 -0700

    change mapPartition to map, use Kryo for TempValue object serialization

[33mcommit 18234e9ee86e57f68ca23f148416311e7d65bd19[m
Author: Nhan Vu <nhanitvn@gmail.com>
Date:   Tue Apr 8 11:53:58 2014 -0700

    Merge branch 'master' into nhan-logisticregression-irls
    
    Fix unit tests

[33mcommit 5cf0adea9e001d6fed4d0cedaa1a20b1153d0308[m
Merge: 1cf9c78 adee5aa
Author: Nhan Vu <nhanitvn@gmail.com>
Date:   Tue Apr 8 11:37:55 2014 -0700

    Merge branch 'master' into nhan-logisticregression-irls

[33mcommit 1cf9c78ad6e1287b104b425e026937cf359bfea9[m
Author: Nhan Vu <nhanitvn@gmail.com>
Date:   Tue Apr 8 11:36:07 2014 -0700

    Add Tests (spark, pa) and Test facilities
    
    To-do: move LossFunction-stuffs to a separate file

[33mcommit d34a975c344ce0c9a8f511cfe9d499e192487fae[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Apr 8 10:44:46 2014 -0700

    fix style

[33mcommit 082482641abaaf6d8212f0c2dba0c71978875b89[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Apr 8 10:16:58 2014 -0700

    delete unnecessary conversion

[33mcommit fd84f620dd1cad9af0651b7c2a3e26b641d4c372[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Tue Apr 8 12:12:40 2014 -0500

    switch to double[]

[33mcommit 763c34c7db0fd58f57dc89071b78990e48bad954[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Tue Apr 8 11:19:56 2014 -0500

    Debugging matrix vector tuple.

[33mcommit d1b82c5d6ebd406119e7fefa92966e3b297aba5e[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Tue Apr 8 09:52:25 2014 -0500

    Added unit test for linear regression normal equation.

[33mcommit 4f0c731d009b444017e10d2f7a7b0c198ee82eec[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Apr 8 02:32:55 2014 -0700

    add PA executor for CV
    
    fix RootBuild to use DDF KryoRegistrator

[33mcommit ece05449941a3e76ae78881676690e92abf7917a[m
Merge: 6a0309b adee5aa
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon Apr 7 20:05:34 2014 -0700

    resolve conflict

[33mcommit 4387e733526990367e1d4f027ea2e825d62a5199[m
Author: khangpham <khangpham@adatau.com>
Date:   Mon Apr 7 18:54:06 2014 -0700

    new constructor

[33mcommit 6a0309b5d59025cc6597f3cd33783674948def0e[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon Apr 7 18:32:40 2014 -0700

    migrate transformRserveNative executor

[33mcommit 6a681fac6322c8e907f458187840288e534e2104[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon Apr 7 17:59:50 2014 -0700

    transform native Rserve impl

[33mcommit 31a5416d345e5f19bf182334bc93ce498aff8ac0[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Mon Apr 7 19:52:08 2014 -0500

    LogisticRegressionGD compiled.

[33mcommit 2687331ba425f1bf2b8732568084243ff9092d18[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon Apr 7 17:51:59 2014 -0700

    support RDD[REXP] in RepresentationHandler

[33mcommit 5007805aa211214ba518e280b8fba0caea02ab80[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Mon Apr 7 19:29:17 2014 -0500

    [WIP] Linear regression GD compiled.

[33mcommit 66f402bd190f21b6acfb2b1c409175242c8b4e3d[m
Author: khangpham <khangpham@adatau.com>
Date:   Mon Apr 7 16:54:51 2014 -0700

    add sample debug script

[33mcommit d5875f2e092d0eceacf5b8d472a533e544379d33[m
Author: khangpham <khangpham@adatau.com>
Date:   Mon Apr 7 16:16:49 2014 -0700

    work with RClient

[33mcommit 024dd8701c46f81fbd0d7943358ed4ebda528394[m
Author: Cuong Kien Bui <cuongbui@adatao.com>
Date:   Mon Apr 7 16:29:49 2014 -0500

    [WIP] Working on LinearRegressionWithGD.

[33mcommit 0ed4b9c810e76a2c759d39176e7b5f40c7b9130e[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Mon Apr 7 15:31:59 2014 -0500

    [WIP] Migrate the PA executor for linear regression normal equation.

[33mcommit bce92f1e87159028dd47f0fc0fdc63cb7ad436b4[m
Author: Nhan Vu <nhanitvn@gmail.com>
Date:   Mon Apr 7 13:30:51 2014 -0700

    Refactor pA executor

[33mcommit 790f02c9fa7fbdd2bb29b3848bbcb0c9f207ccaa[m
Merge: 9598585 adee5aa
Author: huandao0812 <huan@adatau.com>
Date:   Mon Apr 7 13:27:18 2014 -0700

    Merge branch 'master' into ddh-cv
    
    Conflicts:
    	spark/src/main/java/com/adatao/spark/ddf/ml/MLSupporter.java

[33mcommit 959858535707d0af457a6d8a46db1675e6dd25c4[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Apr 7 13:22:07 2014 -0700

    WIP

[33mcommit 72a6ab5e2df4cf01aad9b7c48800202172970445[m
Merge: 6d166b8 0db99ad
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Mon Apr 7 14:08:51 2014 -0500

    Merged test2 branch from Khang to get Matrix and Vector classes.

[33mcommit f5f7c041a14d523da6fd427142ba7574177be7cc[m
Author: kpham <khangich@gmail.com>
Date:   Mon Apr 7 11:19:05 2014 -0700

    print debug

[33mcommit 6d166b8d9f3ffef5e189d390c61d1daed2b7b231[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Mon Apr 7 12:42:45 2014 -0500

    [WIP] LinearRegressionNormalEquation.scala.

[33mcommit 9af48fd2d64951878ce23977d530524b43c8c02f[m
Merge: 6ffe6b4 43b809e
Author: kpham <khangich@gmail.com>
Date:   Mon Apr 7 00:58:48 2014 -0700

    merged and commit

[33mcommit 43b809eb19843887e47bb8dd95078966a45249fb[m
Author: kpham <khangich@gmail.com>
Date:   Mon Apr 7 00:41:31 2014 -0700

    residual pass smoke test

[33mcommit f5b5168a32503818660d1c2d4b7dc376f0a47aa3[m
Author: kpham <khangich@gmail.com>
Date:   Sun Apr 6 21:55:55 2014 -0700

    fix ISupportMetricSupporter

[33mcommit bea2b585c5183f46b802d05693b8a930d92ce3d3[m
Author: huandao0812 <huan@adatau.com>
Date:   Sun Apr 6 19:00:13 2014 -0700

    add work for CrossValidation

[33mcommit a23878ac108233dbf35a101ee131bb0bf037d2a9[m
Author: kpham <khangich@gmail.com>
Date:   Sun Apr 6 12:25:19 2014 -0700

    add smoke test for r2score

[33mcommit 8fc5bd0f18c298d3fa1b78f589ac30e80cbc058e[m
Merge: 43e57e3 adee5aa
Author: kpham <khangich@gmail.com>
Date:   Sun Apr 6 11:46:05 2014 -0700

    Merge branch 'master' into r2score

[33mcommit 6ffe6b476779f610f67050f1ba86fc32de0f9149[m
Author: kpham <khangich@gmail.com>
Date:   Sun Apr 6 11:41:08 2014 -0700

    change roc signature, execution

[33mcommit 0915d429440abfc8076cd68d0c78c31b6619fc54[m
Author: kpham <khangich@gmail.com>
Date:   Sun Apr 6 11:13:44 2014 -0700

    wip roc

[33mcommit 2077ff1ef57019ba71dcf0f57fb1f80aeaf3fef6[m
Author: kpham <khangich@gmail.com>
Date:   Sat Apr 5 09:24:03 2014 -0700

    add roc metric

[33mcommit 410333b1365e374dbf59499609df7a1deef052d4[m
Merge: bfd4063 43e57e3
Author: kpham <khangich@gmail.com>
Date:   Sat Apr 5 09:11:47 2014 -0700

    resolve conflict

[33mcommit 43e57e373bf05538533ccf5547bd373d46b710f4[m
Author: kpham <khangich@gmail.com>
Date:   Sat Apr 5 09:04:16 2014 -0700

    change loadFile to use runSQL2RDDCmd

[33mcommit cff8b0e23c4f55105e1f560ef0560d90fccd6386[m
Author: kpham <khangich@gmail.com>
Date:   Sat Apr 5 09:01:16 2014 -0700

    remove comment, enable tests

[33mcommit adee5aa439e6ffd7526c586036de246aca31e1ce[m
Merge: 963b922 53213e2
Author: khangpham <khangpham@adatau.com>
Date:   Sat Apr 5 08:58:37 2014 -0700

    Merged in test2 (pull request #62)
    
    Migration LogisticRegression sparse

[33mcommit 53213e2e08a13f07a681a8216d616387c5f2dd04[m
Merge: 814fa1c 963b922
Author: kpham <khangich@gmail.com>
Date:   Sat Apr 5 08:58:01 2014 -0700

    Merge branch 'master' into test2

[33mcommit 963b9222635ae0e977bf848de4627f9773e3e531[m
Author: kpham <khangich@gmail.com>
Date:   Sat Apr 5 08:57:28 2014 -0700

    fix hive-site setting

[33mcommit 814fa1ce6416e44016b1220660b9b33467a7abb1[m
Author: kpham <khangich@gmail.com>
Date:   Sat Apr 5 08:45:10 2014 -0700

    remove println

[33mcommit bfd40631baa92a871b3b0f927f1ddb824d6e560d[m
Author: kpham <khangich@gmail.com>
Date:   Fri Apr 4 17:07:15 2014 -0700

    roc compiled

[33mcommit e7a7d865dad55af2555b87012c7d4ce215e74b67[m
Merge: afbae9f 25f0ccd
Author: kpham <khangich@gmail.com>
Date:   Fri Apr 4 14:43:49 2014 -0700

    Merge branch 'r2score' into migration-roc

[33mcommit 25f0ccdcb651d3c3418940debd2374a734f573c3[m
Author: kpham <khangich@gmail.com>
Date:   Fri Apr 4 14:36:24 2014 -0700

    residuals new execution

[33mcommit fb49126220a5c0263f18c15e64f48a995ab2577e[m
Merge: e609bc6 f60471e
Author: kpham <khangich@gmail.com>
Date:   Fri Apr 4 13:26:05 2014 -0700

    Merge branch 'r2score' into residuals

[33mcommit f60471e7a3d1bb498f5abb64f898928f59eaef16[m
Author: kpham <khangich@gmail.com>
Date:   Fri Apr 4 13:25:57 2014 -0700

    add comments

[33mcommit 5ebbc2bc95eb6d3a204d477f77a699cfd32f6a48[m
Author: kpham <khangich@gmail.com>
Date:   Fri Apr 4 13:24:21 2014 -0700

    MLMetricsSupporter and R2Score, compiled

[33mcommit cd4140ef070d3b2b93f5c4ea9909914d389b1353[m
Merge: d4e1a09 d0d004a
Author: Binh Han <bhan@adatao.com>
Date:   Fri Apr 4 10:55:53 2014 -0700

    Merged in bhan-subset-rowfiltering (pull request #68)
    
    Migrate subset-row filtering, integrate with Rclient

[33mcommit afbae9f45e023090e688d22c63bce42a08b56ed0[m
Author: kpham <khangich@gmail.com>
Date:   Fri Apr 4 10:12:11 2014 -0700

    wip compiled

[33mcommit c3419281ca83b0a9a857550a95858d5ee78cbb07[m
Author: kpham <khangich@gmail.com>
Date:   Fri Apr 4 09:04:25 2014 -0700

    init

[33mcommit 79a5dfe183e2e256d1f79d11418ba2110b6cacd4[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Apr 3 18:40:12 2014 -0700

    add CV for RDD[Array[Object]]

[33mcommit e609bc6ba9a264b46e7a7539650ce4c2617df071[m
Author: kpham <khangich@gmail.com>
Date:   Thu Apr 3 17:16:40 2014 -0700

    residuals compiled

[33mcommit e0ba2624a94865d99691493bde92490e088e1713[m
Author: kpham <khangich@gmail.com>
Date:   Thu Apr 3 16:04:53 2014 -0700

    wip compiled

[33mcommit dc17a8cbe52c8a540111b13eb5dec27555285230[m
Author: kpham <khangich@gmail.com>
Date:   Thu Apr 3 14:58:20 2014 -0700

    wip getmodel

[33mcommit 2710d3763f81e51e2ed586ddc4391c3ba5e7e8d8[m
Author: kpham <khangich@gmail.com>
Date:   Thu Apr 3 14:53:58 2014 -0700

    getName in model

[33mcommit ae63a4b31db81b3b1d96c6d6d4438060a20879e7[m
Author: kpham <khangich@gmail.com>
Date:   Thu Apr 3 14:52:42 2014 -0700

    add support for getModel

[33mcommit 7cd8cf134bfa71da86f1f7f72d1ca535e92f249c[m
Author: kpham <khangich@gmail.com>
Date:   Thu Apr 3 14:41:10 2014 -0700

    wip 1

[33mcommit eb96e746c58e6a83b10febff500894c68605537b[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Apr 3 14:35:09 2014 -0700

    WIP

[33mcommit 0db99ad5066da3a722bb553a11afe214d8a39981[m
Author: kpham <khangich@gmail.com>
Date:   Thu Apr 3 14:17:15 2014 -0700

    remove comment

[33mcommit 5cf5ba9c80147d96705fac73e7f50bf536dc84d6[m
Author: kpham <khangich@gmail.com>
Date:   Thu Apr 3 14:14:44 2014 -0700

    reformat

[33mcommit 8cb9e6a8bf29b6d5b7ad7b145524d8c18d2bfdcb[m
Author: kpham <khangich@gmail.com>
Date:   Thu Apr 3 11:56:14 2014 -0700

    enable unit test

[33mcommit 09e37b57e82547c25281bb12250ffc7dca37d080[m
Author: kpham <khangich@gmail.com>
Date:   Thu Apr 3 11:54:58 2014 -0700

    remove println

[33mcommit f44b0b7ba6293bb49260501a9c9be5d06598cc66[m
Author: kpham <khangich@gmail.com>
Date:   Thu Apr 3 11:27:16 2014 -0700

    fix different dimension in Vector

[33mcommit d0d004a4833a072a37ff0e68766bee5501486b66[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Apr 3 10:48:00 2014 -0700

    fix bug in ExpressionDeserializer

[33mcommit 1feb6cf1020220234debc3651b7a951aeb390595[m
Merge: 000e8e5 d4e1a09
Author: kpham <khangich@gmail.com>
Date:   Wed Apr 2 15:34:29 2014 -0700

    merge with master, change hive-site

[33mcommit 734eb10fdb49efdaa8a53101432d040c5f190007[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Wed Apr 2 13:39:28 2014 -0500

    Done with binary confusion matrix executor.

[33mcommit eedc7d7c7e9a04858b3b74ccbc1c9569fde460fa[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Wed Apr 2 12:42:19 2014 -0500

    Move confusion matrix computation to MLSupporter.

[33mcommit 390467ac3e5d17cf656b7613073e96cc547c760a[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Apr 2 10:17:52 2014 -0700

    integrated with Rclient

[33mcommit b35537c7acd77c8e6aa80b97e504365a24830112[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Wed Apr 2 11:21:24 2014 -0500

    [WIP] still have to compute binaryConfusionMatrix with Metrics.scala.

[33mcommit 2d508dd5f47db90fbbae708307f43834e5d90532[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Wed Apr 2 10:31:52 2014 -0500

    [WIP] on going with binary confusion matrix.

[33mcommit bcca367dcaee382e7a5ef5c5c691c8ecd67c92d1[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue Apr 1 18:19:54 2014 -0700

    refactor subset executor

[33mcommit 10959c51d5add47f7975cd377554a0ae47d3d265[m
Merge: 7f4ce63 d4e1a09
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue Apr 1 16:11:15 2014 -0700

    Merge branch 'master' of https://bitbucket.org/adatao/DDF into bhan-subset-rowfiltering
    
    Conflicts:
    	core/src/main/java/com/adatao/ddf/content/ViewHandler.java

[33mcommit 7f4ce63f1f98f8d4e0041768d07d020f8ab707ca[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue Apr 1 16:08:08 2014 -0700

    add subset impl under ViewHandler

[33mcommit d4e1a09be5cb0b1702d6ba1e9f86bce21016637b[m
Merge: 2f62061 a00d0bc
Author: Binh Han <bhan@adatao.com>
Date:   Tue Apr 1 15:41:31 2014 -0700

    Merged in ddh-executor (pull request #67)
    
    add work for new PA Executor

[33mcommit a00d0bc5ac435e838a2f23d1ce446e8b1372bdf3[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Apr 1 09:45:00 2014 -0700

    change PA.runCommand to PA.runMethod

[33mcommit cf9e882b2aab3728934d85234ba1b29bfc244802[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Apr 1 09:35:57 2014 -0700

    add work for new PA Executor

[33mcommit 2f620619fbcf24d48bcb9244bc0d6be6b34425db[m
Merge: 099194c a59b678
Author: Binh Han <bhan@adatao.com>
Date:   Mon Mar 31 17:46:27 2014 -0700

    Merged in bhan-binning (pull request #66)
    
    Implement Factor and Binning executor, integrated with pa-Rclient

[33mcommit a749efb0d9d7f370752c32724470f560eafdb2db[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Sun Mar 30 14:37:54 2014 -0500

    Clean up unused code in PA executors.

[33mcommit e0380ee87a71a7508885215a3c6c7693cc20a0e0[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Sun Mar 30 14:23:17 2014 -0500

    Finished migration of logisticRegressionWithSGD.

[33mcommit 0ec5d76ef852a88f23ab2493d136599a8aeea922[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Sun Mar 30 13:41:47 2014 -0500

    Done with Kmeans executor migration.

[33mcommit 6055f62564252f85a4a3ab4d6647630916c571e9[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Sun Mar 30 12:56:22 2014 -0500

    Finished the linearRegressionWithSGD.

[33mcommit 06aba018b8f4cfe80206d8b6be0a00c4dbe4c084[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Sun Mar 30 11:38:42 2014 -0500

    [WIP] Almost done with linearRegressionWithSGD.

[33mcommit 6a42a40f17cf80186ccde2370b4604408e3a1445[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Sun Mar 30 09:38:10 2014 -0500

    [WIP] Missing testable data for logisticRegression.

[33mcommit 158e2a21f8836c6db4c956e55299ed34ffb48f80[m
Merge: bae91f0 bdfa09d
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Thu Mar 27 20:42:44 2014 -0500

    Merge branch 'ckb-pa-subset' into ckb-logisticRegressionSGD

[33mcommit a59b678975127030443516a4e3a7842aceb4d318[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Mar 27 18:39:35 2014 -0700

    finish work on Factor

[33mcommit 7b5d99fe82853f3dd148f8dd351f6376fa965579[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Mar 27 01:55:24 2014 -0700

    work on BinningSuite test

[33mcommit bdfa09d20530213bf5ebb812c6b766889b9bf69d[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Wed Mar 26 23:20:19 2014 -0500

    Fixed the pArray parameter name.

[33mcommit c0d0c73f9b17821ac41905a03bd0c5a7dce6b022[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Mar 26 17:37:25 2014 -0700

    fix some error messages

[33mcommit 70b453a973e285a898716b6c219337c5345c1ae5[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Mar 26 00:21:34 2014 -0700

    save factor into MetaInfo

[33mcommit bae91f023b23e19951311482913ee3fa693309af[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Tue Mar 25 21:55:28 2014 -0500

    [WIP]

[33mcommit 4337ad24c79ff09628f9234dc5d0c1a504bd69ba[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue Mar 25 19:14:45 2014 -0700

    subset Impl (wip)

[33mcommit 94ed4534479bb0a541cb0b9556d6d2ce6386f92b[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Tue Mar 25 20:34:54 2014 -0500

    Done with LogisticRegressionSGD on spark module.

[33mcommit a0d987b6c040463478a4bd7f1264fc0e53243580[m
Merge: 66490ff 76148c7
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Tue Mar 25 20:21:31 2014 -0500

    Merge branch 'ckb-pa-subset' into ckb-logisticRegressionSGD

[33mcommit 66490ff1feb6d1273437a548c09017dc6aa2619b[m
Merge: aef2dcf 4c22c3c
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Tue Mar 25 20:18:46 2014 -0500

    Merge branch 'ddh-demo' into ckb-logisticRegressionSGD

[33mcommit 000e8e5e7f99a59b11e89940c04f62804eeba826[m
Author: kpham <khangich@gmail.com>
Date:   Wed Mar 26 01:11:42 2014 +0700

    pass test, rclient

[33mcommit 4c22c3c31152c0ab79988c9ca2acb30e6dc4322f[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Mar 25 22:21:17 2014 +0700

    add work for YTrueYPred demo

[33mcommit 6dfa687cf4c14f177c23e78592a11e184e103001[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue Mar 25 02:50:12 2014 -0700

    implement computeLevelMap in Factor to compute levelMap from a list of levels

[33mcommit 17efb904dd5c163051d2f2657e8cb78941290fce[m
Merge: 30f5794 099194c
Author: huandao0812 <huan@adatau.com>
Date:   Tue Mar 25 16:44:08 2014 +0700

    Merge branch 'master' into ddh-demo

[33mcommit fc41a6d10fd7c2d74d0bccda367f5c813f736ef1[m
Author: kpham <khangich@gmail.com>
Date:   Tue Mar 25 16:15:41 2014 +0700

    update executor

[33mcommit aa6567d2d3407be5bc63231bca80939823154495[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue Mar 25 01:31:50 2014 -0700

    change binningType names to match with Rclient API

[33mcommit 1036a3f82e041bdd977c3dbab68034c59275029a[m
Merge: 75d5b76 099194c
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon Mar 24 23:42:39 2014 -0700

    Merge branch 'master' of https://bitbucket.org/adatao/DDF into bhan-binning
    
    Conflicts:
    	project/RootBuild.scala

[33mcommit 75d5b765e87ec94057ca6a330d0042412282d38b[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon Mar 24 23:31:10 2014 -0700

    update binning pA executor

[33mcommit 099194c3510adb958fa1a2f8f728e81f24f21469[m
Merge: 3be83e7 ababca5
Author: Binh Han <bhan@adatao.com>
Date:   Mon Mar 24 23:25:04 2014 -0700

    Passed Binning unitTest.
    Merged in ctn-factor (pull request #59)
    
    [FEATURE] Enable DDF to set one of its columns as a Factor, using DDF#setAsFactor()

[33mcommit 76148c72c7689bea4831977915281dadef1bc52a[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Tue Mar 25 00:10:19 2014 -0500

    Finished refactoring PA Vector quantile executor.

[33mcommit 146ff6240feef559ca876a34ecb2947df19dd008[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Mon Mar 24 23:36:43 2014 -0500

    Done with first try on subset.

[33mcommit eb76b0fe5c91e2be4055bbc4f068ae3db3162f85[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Mon Mar 24 23:16:03 2014 -0500

    Fixed some minor syntax errors for subset.

[33mcommit b5c383a563711601e6497a818c52ee5bca8a7b84[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Mon Mar 24 23:07:30 2014 -0500

    Refactoring PA subset command. The very first case: filter by one column.

[33mcommit 9b7da0a09e5af3df24f803990c94ceccb0935b6d[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Mon Mar 24 22:16:42 2014 -0500

    Added get vector quantiles for single column DDF.

[33mcommit 6d9ab6e27b6734a5cc8b6490176e257b4945ee8d[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Mon Mar 24 21:49:18 2014 -0500

    Added test case for quantiles.

[33mcommit 610f4657e13e30476c56fb7d9de405baae2a5bec[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon Mar 24 19:35:25 2014 -0700

    binning executor returns new DDF

[33mcommit 15f4acf3e8334aa61bb0d63881724344c1185efc[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Mon Mar 24 19:49:41 2014 -0500

    Added number format exception checks.

[33mcommit 6938223d510a267f09f8cbf316dc6f0d2a374155[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Mon Mar 24 19:45:59 2014 -0500

    Added null checks for vector quantiles.

[33mcommit aef2dcfe5632b45970629ee385a5d54eec32fcdc[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Mon Mar 24 19:37:22 2014 -0500

    [WIP] LogisticRegressionModel.

[33mcommit ce5ee675af0560707f7453bd3a6f828ff76c4fd7[m
Author: kpham <khangich@gmail.com>
Date:   Mon Mar 24 20:51:15 2014 +0700

    prediction for glm sparse

[33mcommit 0b59d339aa542a21dff6cac4e7a87021dc120bdd[m
Author: kpham <khangich@gmail.com>
Date:   Mon Mar 24 11:30:27 2014 +0700

    change LogisticRegressionCRS model

[33mcommit f94373b286985630eb7a5534d70ca1172c3648d7[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Sun Mar 23 20:55:44 2014 -0500

    Implemented LogisticRegrestionWithSGD

[33mcommit 8c617881553bb4ab894493099efdec041a7d2548[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Sun Mar 23 20:15:27 2014 -0500

    Migrated Vector quantiles to DDF AStatisticsSupporter.

[33mcommit 3be83e7e3453adf96436a932fd0e83200d2cf662[m
Merge: b0a1809 14dc529
Author: Cuong Bui <cuongbuikien@gmail.com>
Date:   Sun Mar 23 17:58:49 2014 -0500

    Merged in ckb-fix-pa-dependency (pull request #63)
    
    Fixed the DDFSPARK_JAR env variable setting in RootBuild.scala for PA dependency.

[33mcommit 14dc5296913623726458e26268e81b5ac57999c1[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Sun Mar 23 17:57:43 2014 -0500

    Fixed the DDFSPARK_JAR env variable setting in RootBuild.scala for PA dependency.

[33mcommit 35b6014582dff0fe472fc9490b2523a0933ddadb[m
Author: kpham <khangich@gmail.com>
Date:   Sun Mar 23 22:31:33 2014 +0700

    delete sh file

[33mcommit c2dd1faa275cb962d99c55e187755513d44578b5[m
Author: kpham <khangich@gmail.com>
Date:   Sun Mar 23 22:28:11 2014 +0700

    change import

[33mcommit b0a18092f3d47daa46ee8a0164c6ca45b116b439[m
Merge: 1a25e56 b57699c
Author: huandao0812 <huan@adatau.com>
Date:   Sun Mar 23 13:40:53 2014 +0700

    Merged in ctn-RH-get (pull request #61)
    
    [FEATURE] Make RepresentationHandler#get(acceptableTypes) create the requested format as necessary

[33mcommit 8c35b8ccb8038d208c527922f9b3cdd542f6751f[m
Author: kpham <khangich@gmail.com>
Date:   Sun Mar 23 09:58:01 2014 +0700

    remove unused import

[33mcommit 5ba711ec9fa58c6b91b05401ca070989ef2f3084[m
Author: kpham <khangich@gmail.com>
Date:   Sun Mar 23 09:55:59 2014 +0700

    refactor, pass unit test

[33mcommit c0297150be3f40399533cc85eb7253059ad0ccc0[m
Author: kpham <khangich@gmail.com>
Date:   Sun Mar 23 08:11:45 2014 +0700

    pass unit test

[33mcommit b57699cd9ecd672aefacd12ffbc3f318f1addf6d[m
Author: huandao0812 <huan@adatau.com>
Date:   Sat Mar 22 22:34:07 2014 +0700

    fix bug

[33mcommit deffcd60dbff330a9afb64304b165076823284d1[m
Merge: 3b70ad5 ababca5
Author: bhan <binhhan.gt@gmail.com>
Date:   Sat Mar 22 08:30:01 2014 -0700

    Merge branch 'ctn-factor' of https://bitbucket.org/adatao/DDF into bhan-binning
    
    Conflicts:
    	core/src/main/java/com/adatao/ddf/DDF.java
    	core/src/test/java/com/adatao/ddf/analytics/AggregationTest.java

[33mcommit 3b70ad58965d66d5eec84d5674045dfb1c555193[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Sat Mar 22 08:22:08 2014 -0700

    move binning impl using sql to spark

[33mcommit 13b45a48cc40bbe840eeef9ffaed0b9ec1d82803[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sat Mar 22 00:14:51 2014 -0700

    [FEATURE] Make RepresentationHandler#get(acceptableTypes) create the requested format as necessary

[33mcommit 0c9ad6dc311cff2a83b9c848a39391950f3d7a30[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Fri Mar 21 23:22:24 2014 -0700

    Revert "add methods canCreate and canGet for RepresentationHandler"
    
    This reverts commit 093ed456504055b2ca742b404fb493c163b8c5e9.

[33mcommit dff2e926c8667b9ee32d16b51610c6918a15ff9b[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Fri Mar 21 23:21:22 2014 -0700

    Revert "fix indentation"
    
    This reverts commit c5289549ce5a7d57b1e60c6ae00f157930d67a06.

[33mcommit c5289549ce5a7d57b1e60c6ae00f157930d67a06[m
Author: huandao0812 <huan@adatau.com>
Date:   Sat Mar 22 09:56:37 2014 +0700

    fix indentation

[33mcommit 093ed456504055b2ca742b404fb493c163b8c5e9[m
Author: huandao0812 <huan@adatau.com>
Date:   Sat Mar 22 09:45:55 2014 +0700

    add methods canCreate and canGet for RepresentationHandler

[33mcommit a10eadec67f0f040f4a44c8ba07090b93c2437e4[m
Author: kpham <khangich@gmail.com>
Date:   Sat Mar 22 00:38:50 2014 +0700

    first working version of glm in ddf

[33mcommit ababca5ddc6e8fe6fc11d9d99299c7ca55f8b77d[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Fri Mar 21 02:22:28 2014 -0700

    [FEATURE] Enable DDF to set one of its columns as a Factor, using DDF#setAsFactor()
    
    Code complete, but untested.

[33mcommit f741a76e543c847aa23f14476774485f2aac9951[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Mar 20 23:45:33 2014 -0700

    fix spelling

[33mcommit 19052025374ca6bd6be9c8697fd4461f97dfb364[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Mar 20 17:31:49 2014 -0700

    add support for rdd<TablePartition> rep

[33mcommit f9d5c445f2bafbb825bd5c91dd7e75dc03121ab4[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Mar 20 17:29:20 2014 -0700

    (wip) get column binning with equal interval and equal freq

[33mcommit e55db91b536d9a28a18d518deaf2b259f6413d65[m
Merge: 7b26b10 1a25e56
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Mar 20 16:26:48 2014 -0700

    iMerge branch 'master' of https://bitbucket.org/adatao/DDF into bhan-binning
    
    Conflicts:
    	core/src/main/java/com/adatao/ddf/analytics/AStatisticsSupporter.java
    	core/src/main/java/com/adatao/ddf/content/IHandleViews.java
    	core/src/main/java/com/adatao/ddf/facades/ViewsFacade.java
    	spark/src/test/java/com/adatao/spark/ddf/StatisticsSupporterTest.java

[33mcommit 528fa737901aab32529e70645918f937b3a722e4[m
Author: kpham <khangich@gmail.com>
Date:   Fri Mar 21 00:09:04 2014 +0700

    wip serilization error

[33mcommit 2706657da82d0d506059bfe0e957c8dc3c7fe488[m
Author: kpham <khangich@gmail.com>
Date:   Thu Mar 20 22:23:23 2014 +0700

    add analytics package

[33mcommit 44f7f5144ee022741b88c1fb7fdbc6380a4648e0[m
Author: kpham <khangich@gmail.com>
Date:   Thu Mar 20 22:23:03 2014 +0700

    use Tuple2

[33mcommit 1a25e562c0ad386db562bf9534a02f1bc91f4b45[m
Merge: b06eab4 b03bb53
Author: Michael B. Bui <freeman@adatao.com>
Date:   Thu Mar 20 08:16:25 2014 -0500

    Merged in ctn-RH-get (pull request #58)
    
    [FEATURE] RepresentationHandler support for getting any of a list of acceptable types, and using that in applyModel()

[33mcommit b03bb53c4ec6a52338370e4166f1948ead957b6b[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Thu Mar 20 03:39:56 2014 -0700

    [FEATURE] RepresentationHandler support for getting any of a list of acceptable types, and using that in applyModel()

[33mcommit 30f57949d078b1a1d5e21022edede1854c360bb9[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Mar 20 14:40:53 2014 +0700

    WIP

[33mcommit 2421618e10ab05dd2ba374f637565eca7a328cce[m
Author: kpham <khangich@gmail.com>
Date:   Thu Mar 20 11:01:16 2014 +0700

    temporary wip regressionsuite

[33mcommit 7b26b105837050deb0cbe3c22952b9ae491933b5[m
Merge: f79a00b b06eab4
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Mar 19 19:42:27 2014 -0700

    Merge branch 'master' of https://bitbucket.org/adatao/DDF into bhan-integration
    
    Conflicts:
    	core/src/main/java/com/adatao/ddf/DDF.java
    	core/src/main/java/com/adatao/ddf/content/IHandleViews.java
    	core/src/main/java/com/adatao/ddf/content/ViewHandler.java
    	core/src/main/java/com/adatao/ddf/facades/ViewsFacade.java
    	project/RootBuild.scala
    	spark/src/test/java/com/adatao/spark/ddf/StatisticsSupporterTest.java

[33mcommit 3f09e2d28737b9040195f0ddbb4ee67e384b3499[m
Author: kpham <khangich@gmail.com>
Date:   Thu Mar 20 01:10:49 2014 +0700

    representation handler

[33mcommit 6a3ea9221c3279fe364711c86dfd49487c50fbf2[m
Author: kpham <khangich@gmail.com>
Date:   Thu Mar 20 01:08:33 2014 +0700

    refactor matrixvector

[33mcommit b06eab4f712fa512cdc3521486a8f9f154d830ff[m
Merge: 2ae1df7 9342cfb
Author: Binh Han <bhan@adatao.com>
Date:   Wed Mar 19 10:39:19 2014 -0700

    Merged in nhan-xtabs-contingency-table (pull request #57)
    
    add xtabs as a function of DDF

[33mcommit 9342cfbb49e32ca0fb7b0dbac2a87ef3f86cae21[m
Author: Nhan Vu <nhanitvn@gmail.com>
Date:   Thu Mar 20 00:03:17 2014 +0700

    remove xtabs code in Statistics-related files

[33mcommit fa6f30b02f4d2448cbab4514c6a74bd2c6aeebb3[m
Merge: 05fbe5c 2ae1df7
Author: Nhan Vu <nhanitvn@gmail.com>
Date:   Thu Mar 20 00:00:39 2014 +0700

    Merge branch 'master' into nhan-xtabs-contingency-table

[33mcommit 05fbe5c9a8c4e5486b78f19394854d48a167ba33[m
Merge: 3456990 3f398c8
Author: Nhan Vu <nhanitvn@gmail.com>
Date:   Wed Mar 19 23:44:31 2014 +0700

    merge bhan-aaggregation

[33mcommit 3456990d01e8f946a972b69009667a3eb569324c[m
Author: Nhan Vu <nhanitvn@gmail.com>
Date:   Wed Mar 19 23:37:23 2014 +0700

    add xtabs function to ddf
    
    re-factor Xtabs executor to use ddf.xtabs()

[33mcommit 2ae1df70333c57e8886d9c43955cb89e7bc37322[m
Merge: 78bb134 0c8cb91
Author: huandao0812 <huan@adatau.com>
Date:   Wed Mar 19 18:13:50 2014 +0700

    Merged in ddh-predict-refactor (pull request #51)
    
    [WIP] add work for predict

[33mcommit 0c8cb9175bfc7b49c925e01438376c3f53775962[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Mar 19 18:10:32 2014 +0700

    finish schema for MLSupporter.applyModel()
    
    add defensive copy for Schema.getColumns()
    and Column.clone()

[33mcommit cb872ce1456844ad04935e0377459da8dd0c8fa0[m
Merge: d29be88 c0b55a6
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Wed Mar 19 01:21:23 2014 -0700

    Merge branch 'ddh-predict-refactor' of https://bitbucket.org/adatao/ddf into ddh-predict-refactor
    
    Conflicts:
    	core/src/main/java/com/adatao/ddf/ml/Model.java

[33mcommit d29be88f047a1e2df810507ef879a14031e088c3[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Wed Mar 19 01:13:09 2014 -0700

    [REFACTOR] Make PredictMethod conform to pattern of ClassMethod
    
    Also make applyModel() accept hasLabel and includeFeatures as parameters.

[33mcommit 3f398c8e823610185d8b802553648425ca5836fe[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue Mar 18 23:11:04 2014 -0700

    placeholder for xtabs impl

[33mcommit c0b55a685df2a38f7c2c00e970eb42552e314a57[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Mar 19 12:26:58 2014 +0700

    use rawModel, fix exception message

[33mcommit 90100d09d130b1de71ec38fa911d6d2ef483b7e8[m
Merge: 1f512e1 78bb134
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue Mar 18 19:53:02 2014 -0700

    Merge branch 'master' of https://bitbucket.org/adatao/DDF into bhan-aggregation

[33mcommit 24b964f1491955bee5201dd0b310af12d48260a7[m
Author: Nhan Vu <nhanitvn@gmail.com>
Date:   Wed Mar 19 09:35:26 2014 +0700

    Initial re-factor works on Xtabs

[33mcommit fcdb8bbf6b40684917e5598908378d74690b0a50[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Mar 19 02:14:31 2014 +0700

    remove method PredictMethod.initializedMethod()

[33mcommit ef89d63102e39d0e02838af29e74b49223dc4608[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Mar 19 02:04:26 2014 +0700

    add static method PredictModel.fromModel(Object model)

[33mcommit 90861e94d079529e2bcbfd7bd1d797a4ed449409[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Mar 19 00:36:23 2014 +0700

    delete unused code

[33mcommit b14a2aceefb739c3bc0b486d841a902709590891[m
Merge: f5d9c25 78bb134
Author: huandao0812 <huan@adatau.com>
Date:   Wed Mar 19 00:29:24 2014 +0700

    Merge branch 'master' into ddh-predict-refactor

[33mcommit f5d9c2594dd5e4b58b94c1de7830deffe8d346e1[m
Author: huandao0812 <huan@adatau.com>
Date:   Wed Mar 19 00:27:48 2014 +0700

    delete Mode.equals as use case not justified

[33mcommit d540776fb98293d8e79d27dcb16938d41a624e40[m
Author: Nhan Vu <nhanitvn@gmail.com>
Date:   Tue Mar 18 23:55:43 2014 +0700

    add sql2txt() with maxRows param

[33mcommit 78bb134e84c4234bb6a33259cd3802f74001ee92[m
Merge: 10d59d1 70a018d
Author: huandao0812 <huan@adatau.com>
Date:   Tue Mar 18 19:57:50 2014 +0700

    Merged in ctn-ddf-name (pull request #54)
    
    [FEATURE] Introduce concept of user-friendly DDF name

[33mcommit 895cbd89586c528c85e381873b1e9e38a1849a26[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Mar 18 19:46:39 2014 +0700

    remove MLSupporter.predict()
    
    add package adatao.ddf.ml

[33mcommit 70a018d73152f7e618dad6977e5f4308e5a9f705[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Tue Mar 18 00:26:04 2014 -0700

    [FEATURE|MERGE] [Title]
    
    Introduce concept of RRD user-friendly name.

[33mcommit 10d59d1df0429cfa51f727a5794b4d2cddfd5c16[m
Merge: 6ab6ddb 199bc72
Author: Binh Han <bhan@adatao.com>
Date:   Mon Mar 17 22:38:04 2014 -0700

    Merged in ctn-persistence (pull request #50)
    
    [FEATURE] Added DDFManager.REGISTRY to handle global DDF registration and retrieval

[33mcommit 199bc72b50cabf9a529b2e71345ad9db44092479[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Mon Mar 17 19:35:39 2014 -0700

    [FEATURE] Provide DDF#getUri() to support globally unique DDF identification

[33mcommit 677eed10c5ee8f9f62b485920b8091cccde0f3cc[m
Merge: 253cb98 6ab6ddb
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Mon Mar 17 18:06:28 2014 -0700

    Merge branch 'master' into ctn-persistence

[33mcommit 6ab6ddb02b264790c288896c9485f4f69f5d4d66[m
Merge: beef06b 783c429
Author: Binh Han <bhan@adatao.com>
Date:   Mon Mar 17 16:43:32 2014 -0700

    Merged in ddh-refactor-SparkDDF (pull request #52)
    
    add method getJavaRDD for interoperability with Java

[33mcommit beef06b6ac392c340f075faf835283f141c8d945[m
Merge: 447659b 5a57058
Author: Binh Han <bhan@adatao.com>
Date:   Mon Mar 17 16:40:00 2014 -0700

    Merged in mbbui-lastmerge-bigr (pull request #53)
    
    last merge with bigr, with some cleanup

[33mcommit 5a570585a341e20fe71a5d0e5aad49eb774b338e[m
Author: Michael B. Bui <freeman@adatao.com>
Date:   Mon Mar 17 18:03:57 2014 -0500

    last merge with bigr, with some cleanup

[33mcommit 783c4297818b5ed89773549492d436eb7cc1676d[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Mar 17 00:46:43 2014 +0700

    add method getJavaRDD for interoperability with Java

[33mcommit 4643cfa7a4ab726108d4d02c1a5d02955239dc4c[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Mar 17 00:40:27 2014 +0700

    fix return type for method kMeans() and linearRegressionWithSGD()

[33mcommit 695fc14a7fb3ae7dd02a4896760961ec0e9faf82[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Mar 17 00:20:34 2014 +0700

    fix style, change method name getYTrueYPred -> getYTrueYPredict

[33mcommit a434808609d33f55f70659169ad055bb6589a5cf[m
Author: huandao0812 <huan@adatau.com>
Date:   Sun Mar 16 23:28:26 2014 +0700

    add work for DDF.ML.Predict(), DDF.ML.getYtrueYPred(), and model.predict()

[33mcommit df7ebfb93b90dd1a29373ba1154c6fac4940f2fb[m
Merge: db364dd 447659b
Author: huandao0812 <huan@adatau.com>
Date:   Sun Mar 16 09:51:55 2014 +0700

    Merge branch 'master' into ddh-predict-refactor

[33mcommit db364dd06d51a9890733c043ddde50493f016b82[m
Author: huandao0812 <huan@adatau.com>
Date:   Sun Mar 16 09:51:02 2014 +0700

    [WIP] add MLPredictMethod

[33mcommit 253cb9813b1861924a2d46c48c4cd21a2a9003e9[m
Merge: eb579ba 447659b
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sat Mar 15 16:27:30 2014 -0700

    Merge branch 'master' into ctn-persistence

[33mcommit 447659bc37a43f6ae7cd0e87104cc719adbda053[m
Merge: 2681a10 0dedad4
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sat Mar 15 09:50:47 2014 -0700

    Merged in ckb-ddf-pa (pull request #49)
    
    Finished the DDF/PA integration goals

[33mcommit eb579ba2eb29b3cff00b6e0256d61538c8e72005[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sat Mar 15 09:45:48 2014 -0700

    [MISC] Added TODO commment
    
    [Details]

[33mcommit 76d5ce918259d37b3e62ade7ad1e6354e6262a2a[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sat Mar 15 09:33:06 2014 -0700

    [FEATURE] Added DDFManager.REGISTRY to handle global DDF registration and retrieval
    
    Use
    
        manager.REGISTRY.register()
        manager.REGISTRY.unregister()
        manager.REGISTRY.retrieve()

[33mcommit 0dedad44c13867825d6f1daedd8835c9d39a8271[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Sat Mar 15 11:25:04 2014 -0500

    Ported fivenumsummary.

[33mcommit ff22dd3ad0a2ce511f19706f9b908cc44e3c2e98[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Sat Mar 15 10:44:42 2014 -0500

    Fixed nrow and quicksummary by creating a Shark table behind each DDF.

[33mcommit 5e22d20f11d26abae35ca97530fca5875c038272[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Fri Mar 14 22:50:46 2014 -0500

    Fixed the nrow.

[33mcommit 2681a104306519c7bc2ba31685e8f4d4962f1880[m
Merge: 03780ba a59cc2e
Author: Cuong Bui <cuongbuikien@gmail.com>
Date:   Fri Mar 14 21:45:40 2014 -0500

    Merged in bhan-ddf-pa (pull request #48)
    
    DDF-PA INTEGRATION

[33mcommit a59cc2e762d401a04ba3c785dbe77faa7aafeae9[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Fri Mar 14 19:25:56 2014 -0700

    Merged with branch master, resolve conflicts

[33mcommit 6554d674c4f6bad0a2e81dc73b1060781a494d47[m
Merge: ee25c09 03780ba
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Fri Mar 14 18:49:02 2014 -0700

    Merge branch 'master' into ctn-persistence

[33mcommit b00540b4aee7d946d6f64108dc872a0fb32b60e1[m
Merge: 0386aab 03780ba
Author: bhan <binhhan.gt@gmail.com>
Date:   Fri Mar 14 16:52:53 2014 -0700

    Merge branch 'master' of https://bitbucket.org/adatao/DDF into bhan-pa-test
    
    Conflicts:
    	pa/src/main/java/com/adatao/pa/spark/SparkThread.java
    	pa/src/main/java/com/adatao/pa/spark/execution/NRow.java
    	pa/src/main/java/com/adatao/pa/spark/execution/NaiveBayes.java
    	pa/src/main/java/com/adatao/pa/spark/execution/RandomForest.java
    	pa/src/main/java/com/adatao/pa/spark/execution/Sql2DataFrame.java
    	pa/src/main/java/com/adatao/pa/spark/execution/Sql2ListString.java

[33mcommit 0386aab9510fedf62c78cd0adbd546eef1781b25[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Fri Mar 14 14:16:17 2014 -0700

    redefine RSERVER_JAR to take ddf_pa*.jar

[33mcommit 92033ea662b998bbf5cfcd42213ba21388542dd3[m
Author: Nhan Vu <nhanitvn@gmail.com>
Date:   Sat Mar 15 03:52:35 2014 +0700

    Fix to return Rclient expected dataContainerID format and column types in lower case

[33mcommit 03780bab31447a9defd610dd3d440b5e3b169208[m
Merge: 2f41252 a18c309
Author: Binh Han <bhan@adatao.com>
Date:   Fri Mar 14 12:42:29 2014 -0700

    Merged in ctn-tweaks (pull request #47)
    
    [BUGFIX] RepresentationHandler.get() now requires a varargs list of data type, starting with container type

[33mcommit a18c3090e876d0ebb079bb5e6f4487788b53b827[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Fri Mar 14 11:22:29 2014 -0700

    [BUGFIX] RepresentationHandler.get() now requires a varargs list of data type, starting with container type
    
    So you must use
    
      this.getDDF().getRepresentationHandler().get(RDD.class, Object[].class);
    
    instead of
    
      this.getDDF().getRepresentationHandler().get(Object[].class);

[33mcommit f5614a9ddb75e2be28dbd9d99454cf1a3f50ee34[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Fri Mar 14 11:07:32 2014 -0700

    correct projectName for ddf_pa in RootBuild.scala

[33mcommit ee25c0908034ab09ea2d97f250fced6500eefdbd[m
Merge: 3c33ef4 2f41252
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Fri Mar 14 11:03:53 2014 -0700

    Merge branch 'master' into ctn-persistence
    
    Conflicts:
    	pa/src/main/java/com/adatao/ML/NaiveBayesModel.java
    	pa/src/main/java/com/adatao/ML/RandomForestModel.java

[33mcommit 6c33b422a7fa3c7e5e22868ff04a95258b8df639[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Fri Mar 14 10:53:51 2014 -0700

    add AdataoException

[33mcommit 2f41252c305ab8e8db75cdc8d8ac75f52d8e31a9[m
Merge: b29794a de2f7ce
Author: Cuong Bui <cuongbuikien@gmail.com>
Date:   Fri Mar 14 12:43:39 2014 -0500

    Merged in ctn-tweaks (pull request #45)
    
    [WIP] Making PA compile

[33mcommit de2f7ce6627387e66866cb2fa4c89d4705bc26f8[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Fri Mar 14 10:36:06 2014 -0700

    [HOTFIX] PA now compiles successfully. Please merge and popularize!

[33mcommit c4673be3b7d5066aa6cf0d6f1ad534213b9d26fa[m
Merge: f80f18e ed34407
Author: Binh Han <bhan@adatao.com>
Date:   Fri Mar 14 09:38:41 2014 -0700

    Merged in nhan-ddf-pa (pull request #46)
    
    Refactor SQL2ListString to use DDFManager for querying Shark

[33mcommit ed344075f02700908f6cc6e095cb849802089bfc[m
Author: Nhan Vu <nhanitvn@gmail.com>
Date:   Fri Mar 14 23:10:53 2014 +0700

    refactor SQL2ListSTring to use DDF library

[33mcommit f80f18ed64126af410563f0930481413feff86b5[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Fri Mar 14 09:06:30 2014 -0700

    small fix to correct containerId

[33mcommit fa2b80bfb56aa0f8c2db18ee402c6506913f6e34[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Fri Mar 14 02:04:19 2014 -0700

    [WIP] Making PA compile
    
    [Details]

[33mcommit 3c33ef4ea2ebc130977f5c639c1503991156d6b2[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Fri Mar 14 00:50:28 2014 -0700

    [WIP] Eclipse warning cleanup
    
    [Details]

[33mcommit 9cee2b90a042e0d152d6ac41c7d4dec76d8f2399[m
Merge: 05cf2cd 81e4873
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Mar 13 22:50:58 2014 -0700

    Merge branch 'ckb-fix-tests' of https://bitbucket.org/adatao/DDF into bhan-ddf-pa

[33mcommit 05cf2cd46f3720803a5bc89ae9137d9df013b944[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Mar 13 22:47:53 2014 -0700

    refactor QuickSummary execution

[33mcommit d321c355feb9b7452ed9ff889dd2a6bca75b63df[m
Merge: 71d54fa b29794a
Author: huandao0812 <huan@adatau.com>
Date:   Fri Mar 14 12:00:52 2014 +0700

    Merge branch 'master' into ddh-predict-refactor

[33mcommit 71d54fa9bf2433282b222e962d51fe1a89502c14[m
Author: huandao0812 <huan@adatau.com>
Date:   Fri Mar 14 11:51:35 2014 +0700

    add method predict(Object model) for SparkDDF

[33mcommit 00f9aabc26c1ee42002ebf4ec793a76e7befb758[m
Merge: f008854 81e4873
Author: Nhan Vu <nhanitvn@gmail.com>
Date:   Fri Mar 14 11:34:11 2014 +0700

    Merge branch 'ckb-fix-tests' into nhan-ddf-pa

[33mcommit 837a267eb46e93dd8499ae0fe5b4bbb0321cb830[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Mar 13 21:26:54 2014 -0700

    refactor Sql2DataFrame to get Sql2DataFrameResult from a DDF

[33mcommit 81e487338d12742916e4bacce50735e3df6d3e9c[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Thu Mar 13 23:18:57 2014 -0500

    Fixed RSERVER_JAR in RootBuild.scala and add resources for pa tests.

[33mcommit f008854e1cf1e3132c981a053fc0151762901f4a[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Mar 13 20:30:11 2014 -0700

    refactor NRow executor, unify containerId and ddfName

[33mcommit c48e0f3979f527cc0fed817d0c5a84f3d1037c09[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Mar 13 20:15:54 2014 -0700

    instantiate a DDFManager in SparkThread

[33mcommit 37687f0a62cd9740aff86f3fe28d6a7b531c3e9b[m
Merge: 7e7f62e b29794a
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Mar 13 19:52:35 2014 -0700

    Merge branch 'master' of https://bitbucket.org/adatao/DDF into bhan-ddf-pa

[33mcommit b29794abb0fc920c410111797e57c723931227ef[m
Merge: 0762d47 d1d750f
Author: Cuong Bui <cuongbuikien@gmail.com>
Date:   Thu Mar 13 20:59:57 2014 -0500

    Merged in bhan-quickfix (pull request #44)
    
    fix namespace for adatao.ML

[33mcommit d1d750f54e4dd7b11c0cb182c12038d681c01088[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Mar 13 18:27:12 2014 -0700

    fix namespace for adatao.ML

[33mcommit 46071f0df34468c41288ce6d1b3cb2e433958db5[m
Merge: feea5c3 0762d47
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Thu Mar 13 17:14:20 2014 -0700

    Merge branch 'master' into ctn-persistence

[33mcommit feea5c34dd9fd63b1f6efee7841f18141c2e7104[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Thu Mar 13 17:13:39 2014 -0700

    [FEATURE] RepresentationHandler for 'basic' engine

[33mcommit 0762d4745ad1d51948dcc2a889f5c0b0e40bed25[m
Merge: 8f43c45 578d754
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Thu Mar 13 17:12:34 2014 -0700

    Merged in ckb-fixing-pa (pull request #43)
    
    Refactor and fix pa server as a DDF sub-project

[33mcommit 578d754a2510879fa49a0771b2e8bd9b3c619317[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Thu Mar 13 16:51:08 2014 -0700

    [WIP] Getting rid of errors in PA-DDF within Eclipse

[33mcommit 7e7f62ea7060f5e40ae36260e1edac18bb8f3926[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Mar 13 15:54:56 2014 -0700

    DDFManger keeps track of existing ddfs

[33mcommit 868ede9e97f3f29083dbb3fc7315a2f6ac4cc6f1[m
Merge: 2bfbb8f 5c01993
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Mar 13 13:04:17 2014 -0700

    Merge branch 'ckb-fixing-pa' of https://bitbucket.org/adatao/DDF into bhan-ddf-pa
    
    Conflicts:
    	pa/src/main/java/com/adatao/pa/spark/execution/NaiveBayes.java
    	pa/src/main/java/com/adatao/pa/spark/execution/RandomForest.java
    	project/RootBuild.scala

[33mcommit 2bfbb8f0b8161eb94820bcc21b8a5dd85c16a29c[m
Merge: 1acd505 8f43c45
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Mar 13 12:54:08 2014 -0700

    Merge branch 'master' of https://bitbucket.org/adatao/DDF into bhan-ddf-pa

[33mcommit 5c019936d7108805e0d40ee006c5120aa8e3cab1[m
Author: Michael B. Bui <freeman@adatao.com>
Date:   Thu Mar 13 14:48:37 2014 -0500

    fix RootBuild.scala to use spark incubating

[33mcommit 1eb544562fadf674846a2bb669c0685153960102[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Thu Mar 13 12:26:40 2014 -0700

    [HOTFIX] Passing the torch to @freeman
    
    bin/sbt clean compile still broken

[33mcommit 1acd505ef98554707581294131602adff31db5af[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Mar 13 12:20:40 2014 -0700

    rename adatao.ML

[33mcommit f397762ae65d79db7433f72053419f9e3bf0d4b5[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Thu Mar 13 13:37:50 2014 -0500

    Fixed the start-pa-server.sh script.

[33mcommit 38e2df0786f13fb077fba26eeb902260d8706873[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Thu Mar 13 12:59:06 2014 -0500

    Done with refactor test classes.

[33mcommit f6c84181f14204af1f66f94d114005381c1fcb49[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Thu Mar 13 12:53:02 2014 -0500

    Almost done with test classes.

[33mcommit cf2bfe75716fe11333689ca561ec831ba9002b5e[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Thu Mar 13 12:48:46 2014 -0500

    Start refactoring test classes.

[33mcommit 716f369dd0b170a2abc57c24d9bbcba25a68192f[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Thu Mar 13 12:41:43 2014 -0500

    Done with fixing the main classes of PA server.

[33mcommit 2bdc41a3ade891c99c858b994a3ae75daf2e090d[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Thu Mar 13 12:12:25 2014 -0500

    Added dependencies for pa.

[33mcommit a9c4a8e67b8219ad615ac1707656a86cd032b409[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Thu Mar 13 11:56:47 2014 -0500

    Fixed the adatao.ML.

[33mcommit 47b264105439c274cc5a2a91913aa84c1cad8cd9[m
Author: Nhan Vu <nhanitvn@gmail.com>
Date:   Thu Mar 13 23:52:07 2014 +0700

    add pa and enterprise to root aggregate

[33mcommit b89f2c871353abdc4ff5b45bbc2c9d283b16611a[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Thu Mar 13 11:50:17 2014 -0500

    Fixed the import of adatao.ML.

[33mcommit 6d94ccb007f1e7a4d5f6359b657c0d7c2a52b834[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Thu Mar 13 11:45:48 2014 -0500

    Fixed the AdataoException import.

[33mcommit c97ce00be6065bf47ab129f31eb8283d032b0a96[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Thu Mar 13 11:27:31 2014 -0500

    Rename packages.

[33mcommit 38f67cf8b862191773d5c89722d75ac387acd76b[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Thu Mar 13 11:16:07 2014 -0500

    A script to refactor scala files.

[33mcommit c76086afeb8cd61cc71ccf37e66744e6a9bb8929[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Thu Mar 13 10:59:02 2014 -0500

    Restructure the scala package.

[33mcommit 5cc7380f0ea8cb4449c2858df92f0e30dfdf875f[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Thu Mar 13 10:28:52 2014 -0500

    Fixed the pom file generation.

[33mcommit 8f43c45f081563116486fdfc1253bda86fdcb14c[m
Merge: 732bcfd c8900c0
Author: huandao0812 <huan@adatau.com>
Date:   Thu Mar 13 17:00:02 2014 +0700

    Merged in ddh-RepHandler (pull request #41)
    
    fix MLSupporter and RepresentationHandler.scala

[33mcommit c8900c0bb0e320c481646d455009aa608118584e[m
Merge: d6c216a 732bcfd
Author: huandao0812 <huan@adatau.com>
Date:   Thu Mar 13 16:56:02 2014 +0700

    Merge branch 'master' into ddh-RepHandler

[33mcommit d6c216a6c0db76e3de904b982c51fa37766e423c[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Mar 13 16:55:14 2014 +0700

    reformat code

[33mcommit c8852211770d040ad127acbc61e1e8192f08e6d8[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Mar 13 02:53:34 2014 -0700

    add pa, enterprise modules in ddf/pom.xml

[33mcommit fae1705dc4269715d2651d7a2054177596c80c27[m
Merge: 08f66b8 732bcfd
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Mar 13 00:41:37 2014 -0700

    Merge branch 'master' of https://bitbucket.org/adatao/DDF into bhan-ddf-pa

[33mcommit 732bcfdde7d719f84ebe808361f9d420c5ad9987[m
Merge: de968bf 0d52ea6
Author: Binh Han <bhan@adatao.com>
Date:   Thu Mar 13 00:35:25 2014 -0700

    Merged in ctn-sqlite (pull request #42)
    
    [REFACTOR] Rename com.adatao.local.ddf to com.adatao.basic.ddf

[33mcommit 08f66b871e56661167296cff5dcf122be1269a53[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Mar 13 00:32:16 2014 -0700

    [PA-INTEG] WIP - move PA server to ddf/pa, change namespace, modify rootbuild.scala

[33mcommit 0d52ea6ad6444030e0baa2c7dc6ba06ed6b6dea8[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Thu Mar 13 00:16:59 2014 -0700

    [REFACTOR] Rename com.adatao.local.ddf to com.adatao.basic.ddf
    
    The word "local" is confusing since for PA the "local" server is still running in the cluster.

[33mcommit 1f512e113210e14b4213a19520fb460aa5f96569[m
Merge: 88b92bf de968bf
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Mar 12 22:35:13 2014 -0700

    Merge branch 'master' of https://bitbucket.org/adatao/DDF into bhan-aggregation
    
    Conflicts:
    	core/src/main/java/com/adatao/ddf/content/ViewHandler.java
    	spark/src/main/scala/com/adatao/spark/ddf/content/ViewHandler.scala

[33mcommit 3984af6263cccec100b9edcb8f7f7ba1f2472b16[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Mar 13 11:07:58 2014 +0700

    comment out test

[33mcommit 13a80f907bbb335fff306a24128baa0eafaa131e[m
Author: huandao0812 <huan@adatau.com>
Date:   Thu Mar 13 11:02:33 2014 +0700

    fix MLSupporter and RepresentationHandler.scala

[33mcommit f79a00b45c5ba9ad5e7fd0b8ee0a9ec6ecb84652[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Mar 12 19:11:52 2014 -0700

    integration test

[33mcommit de968bfa239decba5c190c5e8569999e95ac7d32[m
Merge: 3ff638b d733d35
Author: Binh Han <bhan@adatao.com>
Date:   Wed Mar 12 18:05:22 2014 -0700

    Merged in ctn-repo (pull request #40)
    
    [FEATURE] Introduce enterprise/ and pa/ sub-projects

[33mcommit d733d35828d93043453e4d97a17a3ad27dc1f5f7[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Mar 12 18:03:44 2014 -0700

    changes to Rootbuild.scala: add mysql-connector artifact, fix classpath

[33mcommit 0eb77df0f867553c0af83fd1ade7c1b082ea0099[m
Merge: a2647ed 3ff638b
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Wed Mar 12 17:53:48 2014 -0700

    Merge branch 'master' into ctn-repo

[33mcommit a2647edcd7206bf28d6cc431aead9321d4b7751a[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Wed Mar 12 17:51:36 2014 -0700

    [FEATURE] Introduce enterprise/ and pa/ sub-projects
    
    [Details]

[33mcommit 3ff638ba205315ac76431f3ae4414ef338c0825c[m
Merge: 3c2d769 69c7505
Author: Binh Han <bhan@adatao.com>
Date:   Wed Mar 12 17:50:26 2014 -0700

    Merged in ctn-repo (pull request #39)
    
    [BUGFIX] Scala subclass cannot refer to instance variable of superclass

[33mcommit 69c7505a88478824c719922497b0d42f4de0c31d[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Wed Mar 12 17:29:03 2014 -0700

    [BUGFIX] Scala subclass cannot refer to instance variable of superclass

[33mcommit 0ceb6222b6b4c9590e4372d8d5adab8617016b89[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Wed Mar 12 02:09:33 2014 -0700

    [BUGFIX] Just trying to make things build and unit tests pass

[33mcommit 3c2d76946c4d7b3744886f2b7c49a4e9653fae37[m
Merge: 239fe73 dc961d3
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Tue Mar 11 23:27:50 2014 -0700

    Merged in ctn-RepHandler (pull request #37)
    
    [WIP] Merge master and remove compile errors

[33mcommit dc961d3662e9ae1498bd5d67c8e15b4aa3bf3b62[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Tue Mar 11 23:26:04 2014 -0700

    [MERGE] Merge master and remove errors
    
    [Details]

[33mcommit d35bf2eb8e5da9945b7a7d9c0bffbe17280d535b[m
Merge: b17fa99 239fe73
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Tue Mar 11 23:20:11 2014 -0700

    Merge branch 'master' into ctn-RepHandler
    
    Conflicts:
    	spark/src/main/scala/com/adatao/spark/ddf/content/RepresentationHandler.scala

[33mcommit b17fa9929c847cb3fa31da20a33fa9af1d7a301c[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Tue Mar 11 23:16:49 2014 -0700

    [WIP] Merge master and remove compile errors
    
    [Details]

[33mcommit 239fe7388c62adf1593fe3e6ce2075bdb320e414[m
Merge: 48cdf90 f93fc01
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Tue Mar 11 22:39:45 2014 -0700

    Merged in ddh-ml-refactor (pull request #34)
    
    refactor RepresentationHandler to use ContainerType and UnitType to identify a Representation

[33mcommit f93fc014318bb5d899c021b4f072ecd24b7fc008[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Tue Mar 11 22:38:33 2014 -0700

    [MERGE] Merge master into ddh-ml-refactor

[33mcommit 48cdf90c505f66b71b2a1cf968d86020db23b42e[m
Merge: 173faaa 55a9119
Author: Binh Han <bhan@adatao.com>
Date:   Tue Mar 11 19:12:42 2014 -0700

    Merged in ctn-RepHandler (pull request #36)
    
    [REFACTOR] Modify IHandleRepresentations to accept typeSpecs as a varargs array of Class<?>

[33mcommit 88b92bff3a5c2036c81a2aaca33db05f11f9ed44[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue Mar 11 18:51:36 2014 -0700

    use ddf.sql2txt in fivenum

[33mcommit 6727f321e9d0e4a4c9dbda253697514407e41673[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue Mar 11 18:05:25 2014 -0700

    mv sql2txt from viewhandler to ddf

[33mcommit 55a911991a4edec492413ae89629ffcb4aeff52a[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Tue Mar 11 17:15:43 2014 -0700

    [REFACTOR] Modify IHandleRepresentations to accept typeSpecs as a varargs array of Class<?>
    
    This means that we specify data types as any number of Class<?>, e.g., {RDD.class, List.class, String.class}
    for RDD<List<String>>.
    
    Also temporarily remove support for iterators in IHandleViews, since we need to think that through more.

[33mcommit 859c94c6fc8405711183652c2416bac15f9ce56d[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue Mar 11 13:40:17 2014 -0700

    change output format of firstNrows, mean, median

[33mcommit 29438b14ee934709eeb77c457745fb1844d942a0[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Mar 11 22:44:26 2014 +0700

    add method predict for a point and array of points

[33mcommit 9f2f4b83c9ab24a0f49cfd0116131cafcb8fed03[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Mar 11 11:33:39 2014 +0700

    model.train will not do projection on DDF on behalf of user.

[33mcommit 44347becfb1fd035429b36dc7825420db2e29b6b[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon Mar 10 14:24:53 2014 -0700

    fix build error

[33mcommit b95133246f0dda1c5dc9dffc06888c9f7ff77bcf[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Mar 10 21:52:21 2014 +0700

    refactor MLSupporter.Model
    
    add resources/test/airlineBig.csv as airline.csv is too small to test
    ML algorithm (causing RDD empty partition error)

[33mcommit dabfe977823a5f99b313aef4dcfc328ee1ea1092[m
Merge: 3a9b690 173faaa
Author: bhan <binhhan.gt@gmail.com>
Date:   Sun Mar 9 19:50:28 2014 -0700

    Merge branch 'master' of https://bitbucket.org/adatao/DDF into bhan-aggregation
    
    Conflicts:
    	core/src/main/java/com/adatao/ddf/analytics/AggregationHandler.java

[33mcommit 3a9b69006efb748048ec3dc4c9aeda43251b3414[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Sun Mar 9 17:47:15 2014 -0700

    clean debug code

[33mcommit 6790ebc78291997410daced35d7c53f4144cc235[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Mar 10 01:32:12 2014 +0700

    fix KmeansSuite

[33mcommit dd8c0df1d5049e2a706fe8c64e52796df74f2a67[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Mar 10 01:17:21 2014 +0700

    fix PersistenceHandlerTests

[33mcommit affa42c8dff2db94d66fec06cdc2fe3c9cac3d79[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Mar 10 00:59:29 2014 +0700

    add work for ml.predict
    
    add KmeansModel and LinearRegressionModel

[33mcommit 7b5a6d8a5d760db129c92d348cdfec856404e583[m
Merge: 54dd75f 173faaa
Author: huandao0812 <huan@adatau.com>
Date:   Sun Mar 9 12:26:35 2014 +0700

    Merge branch 'master' into ddh-ml-refactor

[33mcommit 54dd75faa478cd5abf9201a02601ab5e50c3e0ab[m
Merge: 0f0076f 95f57eb
Author: huandao0812 <huan@adatau.com>
Date:   Sun Mar 9 12:25:43 2014 +0700

    Merge branch 'ddh-ml-refactor' of https://bitbucket.org/adatao/DDF into ddh-ml-refactor
    
    Conflicts:
    	core/src/main/java/com/adatao/ddf/analytics/AStatisticsSupporter.java
    	core/src/main/java/com/adatao/ddf/analytics/ISupportStatistics.java
    	ddf-conf/ddf.ini

[33mcommit 0f0076fc522a417bea242a6d57561217e44d6d79[m
Author: huandao0812 <huan@adatau.com>
Date:   Sun Mar 9 12:20:02 2014 +0700

    refactor RepresentationHandler to use ContainerType and UnitType to identify a Representation
    
    fix MLSupporter

[33mcommit 173faaaa2aeb5ebd7794df5494e456ff3603d62b[m
Merge: 9060a88 95f57eb
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sat Mar 8 01:49:50 2014 -0800

    Merged in ddh-ml-refactor (pull request #28)
    
    refactor MLDelegate and ISupportML,

[33mcommit 0e88b892a0e731b763d39020719648512b940645[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Fri Mar 7 17:36:13 2014 -0800

    RSupporter for R aggregate formula

[33mcommit 37b2d0df0bc02c9fdb3b42ca4524877dc21a0f77[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Mar 6 19:14:53 2014 -0800

    add fivenumsum testcase

[33mcommit 297efc3f1a6c5283031cfbbd06342d31ff6d9c8c[m
Merge: c547b09 0a332ea
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Thu Mar 6 18:48:51 2014 -0600

    Merge Binh fix and new tests.

[33mcommit 0a332ea706a5ce234763c54234c0d95a76115d94[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Mar 6 16:38:36 2014 -0800

    change to numUnaggregatedFields in AggregateResult

[33mcommit c547b0931514a238082bd43283d2b211675b76c3[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Thu Mar 6 18:13:20 2014 -0600

    Fixed the aggregation bug.

[33mcommit 687071ac366198ec3e95167ed38a103ce1e367c4[m
Merge: c49bd0e 18537ee
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Mar 6 16:05:05 2014 -0800

    Merge branch 'bhan-aggregation' of https://bitbucket.org/adatao/DDF into bhan-aggregation

[33mcommit c49bd0e7aed98fd51ea550bef6a4963f52fecc78[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Mar 6 16:04:09 2014 -0800

    syntax fix

[33mcommit 6c393d3fe7f8523be4dc458fb633bd3e233c013f[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Thu Mar 6 17:33:35 2014 -0600

    Almost done with table name.

[33mcommit 2664114e4c8c6aa879d8afc154b7f17657caa768[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Thu Mar 6 16:38:42 2014 -0600

    Fixed the TablePartition to Row issue.

[33mcommit ad9635aaeaae9de03e3e20c4aa428a03b84acbd9[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Thu Mar 6 15:47:02 2014 -0600

    [WIP] fixing the null table name.

[33mcommit eb0627a29966f7eb9c5d067b4b6fdf4fa159a310[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Thu Mar 6 14:22:44 2014 -0600

    Fixed a small error on regular expression for AggregationHandler.

[33mcommit 5c9b344bd3bd12ddae085dd3795d8b973c9bc90d[m
Merge: b3da2f0 18537ee
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Thu Mar 6 14:09:25 2014 -0600

    Merged with bhan-aggregation branch.

[33mcommit b3da2f0043056c714b8466aee7a9eb9b3682e13c[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Thu Mar 6 13:53:18 2014 -0600

    Fixed the IHandleViews error.

[33mcommit 5380e93b238e65d62d85896009174f3d5a2cab8f[m
Merge: f3d223e 9060a88
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Thu Mar 6 13:44:59 2014 -0600

    Merge branch 'master' into ckb-integration4

[33mcommit 9060a88d93de7214828fed1df2d770b5fa2117fc[m
Merge: 13f798c d548991
Author: Cuong Bui <cuongbuikien@gmail.com>
Date:   Thu Mar 6 10:52:45 2014 -0600

    Merged in ckb-fix-asm (pull request #32)
    
    Fixed the asm dependency issue.

[33mcommit d548991f4c353a6756e587190bd0462f5d3ff404[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Thu Mar 6 10:49:49 2014 -0600

    Fixed the asm dependency issue.

[33mcommit 18537eee909d72741c1fd8165c38acf9e6447bb2[m
Author: Nhan Vu <nhanitvn@gmail.com>
Date:   Thu Mar 6 23:16:21 2014 +0700

    fix spelling of AggregationHandler

[33mcommit f3d223e89a00afd13bd84e64afd1f56b66be36a7[m
Merge: ce2a2e0 13f798c
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Thu Mar 6 09:29:38 2014 -0600

    Merge branch 'master' into ckb-lr1

[33mcommit 95f57ebe446d1a0601d034c749096e839f5ae20c[m
Merge: de708ca 13f798c
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Wed Mar 5 21:08:16 2014 -0800

    Merge branch 'master' into ddh-ml-refactor
    
    Resolved Conflicts:
    	core/src/main/java/com/adatao/ddf/DDF.java
    	core/src/main/java/com/adatao/ddf/analytics/AggregationHandler.java

[33mcommit 13f798ca6ec4a6bf6505cf803e667171bbeb96de[m
Merge: ae8da6f 881ad94
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Wed Mar 5 21:01:05 2014 -0800

    Merged in ctn-persistence (pull request #29)
    
    [FEATURE] We can now persist(), load(), and unpersist() Models directly.

[33mcommit f17eec0cf48b574692afb61fb1b9f0476608e5df[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Mar 5 20:55:52 2014 -0800

    fix incompatible sample type

[33mcommit de708cac611ef79e728efa461f9290b503bb0a45[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Wed Mar 5 20:53:25 2014 -0800

    [FEATURE] Reflection-based invocation of ML algorithms

[33mcommit 6a2b21036714f1bafcce16016514711264b3fd0b[m
Merge: 6a330ce ae8da6f
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Mar 5 19:37:43 2014 -0800

    Merge branch 'master' of https://bitbucket.org/adatao/DDF into bhan-aggregation

[33mcommit 6a330ce528efae17d7bc41b6802202555f39e7ad[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Mar 5 19:30:41 2014 -0800

    add testcases

[33mcommit ce2a2e02987ec4d4597ca9612de44132f16d2201[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Wed Mar 5 20:24:20 2014 -0600

    [WIP] Added getColumnNames, getNumRows, etc and getFiveNumSummar to DDF Python client.

[33mcommit 067e5148b71a26e50feaac691a175316bd7c5c16[m
Merge: 9a4a89b ae8da6f
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Wed Mar 5 20:08:03 2014 -0600

    Merge branch 'master' into ckb-lr1

[33mcommit ae8da6fa8c60ca3fe3b89e0b44c6752dbb0214aa[m
Merge: 6f18815 17c60b1
Author: Binh Han <bhan@adatao.com>
Date:   Wed Mar 5 16:12:08 2014 -0800

    Merged in ctn-tweaks (pull request #31)
    
    [REFACTOR] Remove spark dependencies from core project

[33mcommit 17c60b1ffe24a012215b3524aadf6b1b5592b74d[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Wed Mar 5 16:04:29 2014 -0800

    [REFACTOR] Remove spark dependencies from core project

[33mcommit 7462c5a3aed82010479a1045b5531bf077c2189a[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Mar 5 15:51:30 2014 -0800

    [WIP] R-style API for aggregation

[33mcommit 846af898e3a407fdc78a14ef5eac3a068f9c97db[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Wed Mar 5 02:57:23 2014 -0800

    [REFACTOR] Use dataset type instead of just row type for representation indexing
    
    [Details]

[33mcommit 881ad94eb257d5534f6002c9c3c700e1b4da99a0[m
Merge: ea8d979 6f18815
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Tue Mar 4 23:37:37 2014 -0800

    Merge branch 'master' into ctn-persistence
    
    Resolved Conflicts:
    	core/src/main/java/com/adatao/ddf/facades/ViewsFacade.java

[33mcommit e9617f946eabbd68c9eef3eb47350fc7b3e24ee8[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Tue Mar 4 23:34:28 2014 -0800

    [WIP] About to refactor new simplified train() signature
    
    [Details]

[33mcommit 9a4a89b3285f6aa7fd62f19da492771448a00039[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Tue Mar 4 20:37:49 2014 -0600

    Convert the java list to Python native list for summary.

[33mcommit f0ac8048bd732b58247c8384f089c67c4ded9a69[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Tue Mar 4 19:56:09 2014 -0600

    Done with DDF summary.

[33mcommit 597b8ec7a7685b0a2b0e9394365bea2900e580ed[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Tue Mar 4 19:38:02 2014 -0600

    [WIP] Working on  Python proxy classes: DDFManager, DDF.

[33mcommit 6f1881503be20709e6e8222295a2d00d23bc6cc6[m
Merge: 8c6108d 1f7255f
Author: Binh Han <bhan@adatao.com>
Date:   Tue Mar 4 17:30:11 2014 -0800

    Merged in bhan-aggregation (pull request #27)
    
    [BasicStatistics] Compute fivenum Summary

[33mcommit 1f7255f1a499d973e4f002e507bf51619844a75d[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue Mar 4 17:27:05 2014 -0800

    Refactor to run 1 sqlcmd for mulptiple columns, move sql2ddf/txt to Viewhandler, add missing break

[33mcommit 64eedcd229968354389932c138b3ecb5cc20a125[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Tue Mar 4 11:22:40 2014 -0800

    [WIP] Use reflection to call static train method.

[33mcommit 1bfbd019db14c3b4631c5c3b4ce76bb65254f3fa[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue Mar 4 09:55:01 2014 -0800

    update ddf.ini

[33mcommit d88342dc80a77fb50ac2903213164df3d6ac66fd[m
Merge: 5e5d9b0 ea8d979
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Mon Mar 3 23:23:13 2014 -0800

    Merge branch 'ctn-persistence' into ddh-ml-refactor

[33mcommit ea8d979906882f7a93a53a7c738d12a1755a7a95[m
Merge: 655228e 8c6108d
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Mon Mar 3 23:18:40 2014 -0800

    Merge branch 'master' into ctn-persistence
    
    Resolved Conflicts:
    	core/src/main/java/com/adatao/ddf/DDF.java
    
    Renamed ViewsDelegate to ViewsFacade

[33mcommit 655228e43761b9af40a4aafa8fe5f8311d41ccaf[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Mon Mar 3 21:55:54 2014 -0800

    [FEATURE] We can now persist(), load(), and unpersist() Models directly.
    
    The interaction with DDF is hidden from the user.

[33mcommit 81a29ba09d85b5fc2dbe2c9c54067fc770cf3268[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon Mar 3 21:36:36 2014 -0800

    fix error message, check NULL result

[33mcommit 5e5d9b0cec58409a8b94f550f9df66dd21cc42c6[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Mar 4 12:04:17 2014 +0700

    refactor MLDelegate and ISupportML, add featureColumnIndexes and targetColumnIndex

[33mcommit c4ae79ccdba4b5058164a03ee44ceea6e9f598d2[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Mon Mar 3 22:50:04 2014 -0600

    WIP R client linear regression.

[33mcommit eec74802b35d7a0f25fc9e0d4fd861a9de8946b2[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Mon Mar 3 22:39:56 2014 -0600

    WIP for DDF R client, linear regression.

[33mcommit 07a5f2e27997e579d5061f556ae48038326d4484[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon Mar 3 19:08:23 2014 -0800

    compute fivenum Summary

[33mcommit 8c6108d7276696742adc2482bbfe183645ab0133[m
Merge: 596dd6c 86bc8c9
Author: Binh Han <bhan@adatao.com>
Date:   Mon Mar 3 15:31:10 2014 -0800

    Merged in bhan-views (pull request #26)
    
    DDF views including subset (project columns), fetch rows, sampling

[33mcommit 86bc8c9557f5ff3ca38de33e498ff09e46637881[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon Mar 3 15:26:21 2014 -0800

    Add ViewsDelegate, refactor sql2ddf execution and throw exception

[33mcommit e202705ac3dc96d8801713ca162411bdfc859890[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Mon Mar 3 14:13:09 2014 -0800

    [FEATURE] Make sure a persisted object's namespace/name are consistent with the DDF containing it

[33mcommit 918a9648f517a9bedacb1dbe6ec3fa0bb97e28af[m
Merge: 0d52154 596dd6c
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon Mar 3 14:10:09 2014 -0800

    Merge branch 'master' of https://bitbucket.org/adatao/DDF into bhan-views

[33mcommit 596dd6cb16f57cadfb13822aec9abb85c2ff754f[m
Merge: 6547c65 6f7132f
Author: Binh Han <bhan@adatao.com>
Date:   Mon Mar 3 11:36:51 2014 -0800

    Merged in ckb-fix-ddfini (pull request #25)
    
    Fixed ddf.ini to make tests run correctly

[33mcommit 0d521546c9f6db7501e3e8a83d1e603c8ab85df3[m
Merge: 90bd7ca 6547c65
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon Mar 3 11:32:39 2014 -0800

    Merge branch 'master' of https://bitbucket.org/adatao/DDF into bhan-views
    
    Conflicts:
    	core/src/main/java/com/adatao/ddf/content/ViewHandler.java
    	spark/src/main/java/com/adatao/spark/ddf/SparkDDF.java

[33mcommit 90bd7ca8e73d9f13a810f40f7ea4390bc5610ce1[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon Mar 3 11:22:05 2014 -0800

    subset by columns projections

[33mcommit 6f7132f52fe1a26b2e46e22b9d66dc90b0ae0458[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Mon Mar 3 12:11:59 2014 -0600

    Fixed a small bug in ddf.ini for basic stats summary.

[33mcommit eb2e5c5418273f97137b9cc1ba30e7cb92f30beb[m
Merge: 330fa86 6547c65
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Mon Mar 3 09:46:36 2014 -0600

    Merge branch 'master' into ckb-lr

[33mcommit 6547c65bc407cdfd3065319e8fe12e4a95f3b573[m
Merge: 954437c 2a10632
Author: huandao0812 <huan@adatau.com>
Date:   Mon Mar 3 11:39:27 2014 +0700

    Merged in ctn-ISupportML (pull request #24)
    
    [FEATURE] Use reflection to support call-by-name ML method invocation.

[33mcommit 330fa865c98f0a28da00bdf080b56a142397532f[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Sun Mar 2 21:06:10 2014 -0600

    Fixed the table name.

[33mcommit 2a10632e4b91a96441772da7b13661a504c3f08b[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sun Mar 2 18:16:16 2014 -0800

    [FEATURE] Use reflection to support call-by-name ML method invocation.
    
    Also introduce "DDF.ML" field to hold all ML-related delegates, thus
    avoiding polluting DDF with too many methods.
    
    See com.adatao.ddf.analytics.MLTests.java

[33mcommit f372c3e2e92aa9379235ca6f0e0397c95ebcdae3[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sun Mar 2 08:11:31 2014 -0800

    [REFACTOR] Move config stufff out of DDF into Config.java
    
    Also move some top-level classes to com.adatao.ddf.misc where they belong

[33mcommit 954437cb75b923add903a247ab2247d41a230b03[m
Merge: 9c63770 b41853a
Author: huandao0812 <huan@adatau.com>
Date:   Sun Mar 2 21:59:04 2014 +0700

    Merged in ctn-persistence (pull request #21)
    
    [FEATURE] Support PersistenceHandler.delete()

[33mcommit e2bfdaa72973deb7a5b2efb660d5ade289aefdb4[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Sun Mar 2 00:25:12 2014 -0800

    compute sampled DDF given sampling percentage

[33mcommit e8b2458211a26b2fdb5bf4214bd1b7a13912dea0[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Sun Mar 2 00:04:55 2014 -0800

    sampling given numSamples

[33mcommit b41853a07cbb179da0bdeafca62e6c4838e81cd2[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sat Mar 1 23:00:54 2014 -0800

    [FEATURE] Implemented PersistenceHandler.{duplicate,rename}

[33mcommit 27f36219d5679824d2e8a765afcccbba4e82af57[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Sat Mar 1 22:45:13 2014 -0800

    fetch rows

[33mcommit 03cd1e47df9655d0f57cc6b5f5efe0d495b4b52d[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sat Mar 1 22:37:22 2014 -0800

    [REFACTOR] Rename IMLSupporter to MLSupporter, since it's concrete.

[33mcommit 3b89d98c356078bc0e10c9e23a774638fadbe27b[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sat Mar 1 22:31:50 2014 -0800

    [FEATURE] Now a Model can be persisted.
    
    Also refactored a lot of the ML classes & interfaces into two files:
    
    ISupportML and MLSupporter

[33mcommit 642515a1b646b970882c78ef393fc9dda740d915[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sat Mar 1 13:07:12 2014 -0800

    [FEATURE] Complete support for LocalObjectDDF persistence.
    
    See PersistenceHandlerTests.test{Load,Save}DDF
    
    Next: create an abstract base Model class that can persist/unpersist itself.

[33mcommit 02802eccf6e503a89dec0a54fae2710e0888c77b[m
Author: Nhan Vu <nhanitvn@gmail.com>
Date:   Sat Mar 1 23:40:28 2014 +0700

    set adatao as function prefix, update tests

[33mcommit 7e0684096776214561ceeef91d8775c777d1d6ce[m
Author: Nhan Vu <nhanitvn@gmail.com>
Date:   Sat Mar 1 23:40:01 2014 +0700

    set adatao as function prefix

[33mcommit 079d7468c70dc0e8a2daab0257e1c5c06b9431d4[m
Author: Nhan Vu <nhanitvn@gmail.com>
Date:   Sat Mar 1 23:08:58 2014 +0700

    Edit title of the package

[33mcommit ee58033458daf8dab69c27d8a074eda669042b21[m
Author: Nhan Vu <nhanitvn@gmail.com>
Date:   Sat Mar 1 23:06:47 2014 +0700

    remove java source directory

[33mcommit 65e8a682f5f9f8731b2a33d00d7c6c7bac394513[m
Author: Nhan Vu <nhanitvn@gmail.com>
Date:   Sat Mar 1 23:05:02 2014 +0700

    initial test skeleton

[33mcommit 4743b9b57a8f1d4ce0fa10b97f68669e8b9b8d43[m
Author: Nhan Vu <nhanitvn@gmail.com>
Date:   Sat Mar 1 22:49:44 2014 +0700

    Initial code for DDFManager and DDF

[33mcommit 9c6377041da20b2bb1d3dbf9caf7935ce7bf45f8[m
Merge: 90c0a10 d1349cc
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sat Mar 1 06:45:39 2014 -0800

    Merged in nhan-fix-airlineWithNA (pull request #22)
    
    fix airlineWithNa.csv according to test usage

[33mcommit d1349ccbaec4187a27166634317bc8973352d4c6[m
Author: Nhan Vu <nhanitvn@gmail.com>
Date:   Sat Mar 1 17:52:26 2014 +0700

    rename airlineWithNa.csv to airlineWithNA.csv

[33mcommit 8b6f524131b774c5bb39c5ba0864abbb568825b5[m
Author: Nhan Vu <nhanitvn@gmail.com>
Date:   Sat Mar 1 17:27:47 2014 +0700

    fix airlineWithNa.csv according to test usage

[33mcommit 90c0a109328edf1909e6d79d0dcb3638adde460e[m
Merge: 5883b53 2ad2cc6
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Fri Feb 28 22:24:54 2014 -0800

    lgtm

[33mcommit 2854bcbe57550b731edd780d98e0cdf51b255ef8[m
Merge: a65a069 5883b53
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Fri Feb 28 22:23:35 2014 -0800

    Merge branch 'master' into ctn-persistence

[33mcommit a65a0699d159ea975f17077b22ce6f3b7987f84c[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Fri Feb 28 22:21:42 2014 -0800

    [FEATURE] Support PersistenceHandler.delete()
    
    Also make sure Schema tableName and DDF name are in sync.

[33mcommit 5883b53314b3f70f059af26ee027ffcdff6a382b[m
Merge: e50e32e c7f8047
Author: huandao0812 <huan@adatau.com>
Date:   Sat Mar 1 12:36:06 2014 +0700

    Merged in ctn-persistence (pull request #19)
    
    [FEATURE] Persist LocalObjectDDF to disk--write operation completed

[33mcommit 2ad2cc66bc12d18a79ad0e2eb4c17ac02d9f71dc[m
Author: Nhan Vu <nhanitvn@gmail.com>
Date:   Sat Mar 1 11:21:36 2014 +0700

    fix tableName hardcode

[33mcommit 8bfdff51cd85b5f5c540e7360e904cb02fbd4e66[m
Author: Nhan Vu <nhanitvn@gmail.com>
Date:   Sat Mar 1 11:18:33 2014 +0700

    create RStudio project

[33mcommit c7f80478ec0e47818b9a7d672886b2852d388512[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Fri Feb 28 18:47:54 2014 -0800

    [REFACTOR] Put back style/ and Java code style formatter

[33mcommit b5dddb65bd3de41c3526ad622a23eac73129332b[m
Merge: 4d37134 e50e32e
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Fri Feb 28 18:45:27 2014 -0800

    Merge branch 'master' into ctn-persistence
    
    Resolved Conflicts:
    	core/src/main/java/com/adatao/ddf/content/Schema.java

[33mcommit 4d37134dd14198dcbb3252059fda362c43552455[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Fri Feb 28 18:43:29 2014 -0800

    [FEATURE] Persist LocalObjectDDF to disk--write operation completed

[33mcommit e50e32e1a100b6cafc0f59e4d5253ae0673e2cc3[m
Merge: cf048a3 590a18e
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Fri Feb 28 17:12:35 2014 -0800

    lgtm
    
    Merged in bhan-aggregation (pull request #18)
    
    fix checking numeric condition, add facade methods for aggregation in DDF

[33mcommit 590a18e5404a725a8657dee5cb2a7632543a101d[m
Merge: 6322900 cf048a3
Author: bhan <binhhan.gt@gmail.com>
Date:   Fri Feb 28 17:04:30 2014 -0800

    resolve conflicts

[33mcommit 6322900b8d2875e351347939053cbc62e934f64e[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Fri Feb 28 16:55:20 2014 -0800

    fix import

[33mcommit 81006e275046d2157ad566822363b6574a6a9087[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Fri Feb 28 16:18:42 2014 -0800

    fix checking numeric condition, add facade method in DDF

[33mcommit cf048a3138c8b7a3752700ff9b8452320212add1[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Fri Feb 28 15:35:27 2014 -0800

    Revert "Merged in bhan-aggregation (pull request #16)"
    
    This reverts commit 61f3b284cadddd23c309cdfb17ecb606f6022864, reversing
    changes made to f1912582f3c2446d66a831e06cf5eab13cd2ad09.

[33mcommit 61f3b284cadddd23c309cdfb17ecb606f6022864[m
Merge: f191258 35e440e
Author: Binh Han <bhan@adatao.com>
Date:   Fri Feb 28 13:53:46 2014 -0800

    Merged in bhan-aggregation (pull request #16)
    
    Support aggregate function

[33mcommit 35e440edc7924cbc65e15fb99d603b16fd48d876[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Fri Feb 28 13:52:18 2014 -0800

    add getColumnNames()

[33mcommit c136c98f07c633ea7308fc734f8fa26d190124cb[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Fri Feb 28 10:53:31 2014 -0800

    [WIP] Started work on Local PersistenceHandler
    
    [Details]

[33mcommit 845cd72fcead9480d17448eb8b45b1244c83ae31[m
Merge: 178bf10 f191258
Author: bhan <binhhan.gt@gmail.com>
Date:   Fri Feb 28 09:16:33 2014 -0800

    resolve conflicts

[33mcommit 178bf10717a82bf2d154ccd923668e246adeb68e[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Fri Feb 28 09:10:35 2014 -0800

    resolve conflicts

[33mcommit f1912582f3c2446d66a831e06cf5eab13cd2ad09[m
Merge: fde3997 fb5d59c
Author: Binh Han <bhan@adatao.com>
Date:   Fri Feb 28 08:10:04 2014 -0800

    Merged in ctn-persistence (pull request #15)
    
    [FEATURE] Provide support for "local" DDF, in local memory and on local disk.

[33mcommit f3b65305be71c58fbeea26d992bfc4ccf7e46be3[m
Merge: 2bb25c6 fde3997
Author: bhan <binhhan.gt@gmail.com>
Date:   Fri Feb 28 08:00:31 2014 -0800

    Merge branch 'master', remote-tracking branch 'origin' into bhan-aggregation

[33mcommit 2bb25c67f77f08e96071735f8e651f90664ea452[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Fri Feb 28 07:59:09 2014 -0800

    move computeCorrelation() to DDF-core

[33mcommit fde3997d1dcdbe3e9ba237b770c7b6af6e48ee93[m
Merge: d9e23d7 49471e5
Author: nhanitvn <nhanitvn@gmail.com>
Date:   Fri Feb 28 17:05:25 2014 +0700

    Merged in ctn-tweaks (pull request #17)
    
    [BUGFIX] Fix bin/run-once.sh to include root-level mvn install

[33mcommit fb5d59c2a69da44271bbbe7f23710d4d391dac44[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Fri Feb 28 01:59:36 2014 -0800

    [HOTFIX] Fix syntax errors

[33mcommit 49471e56911aced5f163ba2ce45b7112a8cf4728[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Fri Feb 28 01:35:05 2014 -0800

    [BUGFIX] Fix bin/run-once.sh to include root-level mvn install
    
    per @nhanitvn's mvn-build solution

[33mcommit 1e592cd609d45c807c1bceefb89b7812c51f3ee9[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Thu Feb 27 21:53:13 2014 -0800

    [BUGFIX] Fix syntax errors.

[33mcommit 8df805e9d2a03799f8ee1346ff73db4a04744245[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Thu Feb 27 21:45:44 2014 -0800

    [REFACTOR] Bring AggregationHandler implementation to base API, because we can.
    
    Also note changes in style/Java-CodeStyle-Formatter.xml

[33mcommit 63d1deee30b3d159c78ea240ce3ff982e2182d35[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Thu Feb 27 20:04:20 2014 -0800

    [REFACTOR] Put style files under style/ top-level dir
    
    [Details]

[33mcommit 071a2ad02ae625344f4944ac2e9f975a907675d6[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Feb 27 17:02:06 2014 -0800

    fix codestyle, refactor

[33mcommit f59ba810911bc87317169c94cac9815f709e93be[m
Merge: 944e9ca d9e23d7
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Feb 27 10:41:26 2014 -0800

    merge with master

[33mcommit 944e9cae4d80497501742bdccf38f1e9414c6337[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Feb 27 10:16:16 2014 -0800

    add method call in DDF

[33mcommit f7946d4e2b31e2e325ea53a351db0bc64237ee12[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Thu Feb 27 04:13:29 2014 -0800

    [FEATURE] Provide support for "local" DDF, in local memory and on local disk.

[33mcommit d9e23d7ca057dfece8efb9d6a34addad0282cdd4[m
Merge: 82fb41e d3fa8a8
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Wed Feb 26 22:20:58 2014 -0800

    Merged in ctn-tweaks (pull request #14)
    
    [REFACTOR] Rename project DDF -> ddf, and adatao.unmanaged->com.adatao.unmanaged

[33mcommit d3fa8a8f452999d64cd19a9118ec6cfc1fd67622[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Wed Feb 26 22:03:17 2014 -0800

    [FEATURE] Introduce conf/ddf_env.sh for future use, also try to minimize cryptic, buried configs

[33mcommit a605815b376d23b9a9c845d1a3d1ba0f6d66f61a[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Wed Feb 26 18:30:24 2014 -0800

    [REFACTOR] Rename project DDF -> ddf, and adatao.unmanaged->com.adatao.unmanaged
    
    Also fix for bin/make-poms.sh's bug that was causing it to look in the wrong project dirs.

[33mcommit b79a26d8e12bd3a6f4bb24ebde8ccf8f178cbf83[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Feb 26 18:09:40 2014 -0800

    format aggregation result

[33mcommit c46bfb13c8ad7030860ac90cb59b2e570f1278a9[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Feb 26 15:55:39 2014 -0800

    IHandleAggregate implementation

[33mcommit 82fb41ee09620a40fb61104b102500604af04159[m
Merge: 11a338f 43805ec
Author: Binh Han <bhan@adatao.com>
Date:   Tue Feb 25 12:54:28 2014 -0800

    Merged in co-debug (pull request #12)
    
    [DEBUG] Various debugging changes during @ckbui and @ctn's co-working session

[33mcommit 11a338f0825fcb82f6de8f86688cdcb227cf2c61[m
Merge: 95d27ec 918fe07
Author: Binh Han <bhan@adatao.com>
Date:   Tue Feb 25 12:29:59 2014 -0800

    Merged in ctn-NA-support (pull request #11)
    
    [FEATURE] Introduce concept of NA/Missing values

[33mcommit 43805ec96588529844bccb53aabe9084a1ff388d[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Tue Feb 25 12:26:37 2014 -0800

    [HOTFIX] Must set the schema in the initialization of DDF
    
    [Details]

[33mcommit 918fe075a4a3fafc81d59bd0160b65d70f6e93a5[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Tue Feb 25 11:55:26 2014 -0800

    [TWEAK] With NA support, we no longer need to be tiptoeing around null values with RepresentationHandler
    
    [Details]

[33mcommit 782900e0a3845c97d7b2f9764c57adcaaeb2432b[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Tue Feb 25 11:49:25 2014 -0800

    [FEATURE] Introduce concept of NA/Missing values

[33mcommit 5154514aa6cb248a4a9881f1a0cb1823a77b6d0e[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Tue Feb 25 11:28:22 2014 -0800

    [HOTFIX] Make sure rowType isn't null going into RepresentationHandler
    
    [Details]

[33mcommit fa75fcdf23ee621271cdc072386d4b2edb1a4b14[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Tue Feb 25 11:01:31 2014 -0800

    [TWEAK] Change handler constructor signature from DDFManager to DDF,
    
    since we made this change in design several days ago.
    
    This illustrates the peril of supporting run-time configuration
    and construction. We didn't get a compile-time signal of this bug.

[33mcommit b9891eae61186956cb4bf48c39b2fb0fa3f4d4ca[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Tue Feb 25 10:58:36 2014 -0800

    [TWEAK] Actually, don't require a non-null DDFManager just to initialize a DDF.
    
    This way if this problem comes up later, it will be obvious and we will have a solid
    use case to decide what's the better design, to require every DDF to have a DDFManager,
    or not (that is, fix that problem use case).

[33mcommit 72bd25beefa1fa971b08b8c5f762e7cb140818c8[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Tue Feb 25 10:52:28 2014 -0800

    [HOTFIX] Make sure we get a non-null DDFManager set first, in DDF.initialize()

[33mcommit f04aa220bb09a95ea03287a191f8d7ed506c2eb3[m
Merge: 994563f 95d27ec
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Tue Feb 25 10:45:02 2014 -0800

    Merge branch 'master' into co-debug to get the latest DDF.getConfigHandler() feature
    
    Resolved Conflicts:
    	core/src/main/java/com/adatao/ddf/DDF.java

[33mcommit 95d27ec6b06016b5e7080c79f00bfa6e58401268[m
Merge: c0fe111 331ab33
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Tue Feb 25 10:37:17 2014 -0800

    Merged in ctn-ddf-config (pull request #8)
    
    [REFACTOR] Cleanly separate out and make globally available

[33mcommit 994563f2f1ff0513830fd6c7b406a1f8130b0670[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Tue Feb 25 10:24:42 2014 -0800

    [DEBUG] Added helpful INFO log messages re Config so we can debug more easily

[33mcommit c0fe111757b9e2c2d11da6c0e1f53e22b2ff79be[m
Merge: ae9654b e3c83cb
Author: huandao0812 <huan@adatau.com>
Date:   Tue Feb 25 23:16:55 2014 +0700

    Merged in ddh-kmeans (pull request #10)
    
    Fix AlgorithmRunner, refactor RepresentationHandler

[33mcommit e3c83cb83de4446ae98166270e1b1b3860aac82c[m
Merge: f39e546 ae9654b
Author: huandao0812 <huan@adatau.com>
Date:   Tue Feb 25 23:11:01 2014 +0700

    Merge branch 'master' into ddh-kmeans

[33mcommit ae9654b332df7d949001bf406d63c2e922d3cd87[m
Merge: d66378c ee6580e
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Tue Feb 25 06:14:59 2014 -0800

    Merged in bhan-stats (pull request #7)
    
    basic-stats fix

[33mcommit ee6580e69a5cbab672637a9318f7776c63b4fa64[m
Merge: 8aecd80 d66378c
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Tue Feb 25 06:00:20 2014 -0800

    Merge branch 'master' into bhan-stats
    
    Resolved Conflicts:
    	pom.xml

[33mcommit f39e54626da08fdac3a39ce35238ee3a5196a7f0[m
Merge: abab645 7dd06ef
Author: huandao0812 <huan@adatau.com>
Date:   Tue Feb 25 12:21:25 2014 +0700

    Merge branch 'master' into ddh-kmeans
    
    Conflicts:
    	spark/src/main/java/com/adatao/spark/ddf/SparkDDF.java

[33mcommit d66378c6680126a9d59055412237c79c75bd1199[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Mon Feb 24 22:48:19 2014 -0600

    Disable scalastyle at root level.

[33mcommit abab645af23282baddddd500591289f469c0ffaa[m
Merge: 857a832 8aecd80
Author: huandao0812 <huan@adatau.com>
Date:   Tue Feb 25 11:44:36 2014 +0700

    Merge branch 'bhan-stats' into ddh-kmeans
    
    Conflicts:
    	core/src/main/java/com/adatao/ddf/analytics/AAlgorithmRunner.java
    	core/src/main/java/com/adatao/ddf/content/IHandleRepresentations.java
    	spark/src/main/java/com/adatao/spark/ddf/SparkDDF.java
    	spark/src/main/java/com/adatao/spark/ddf/analytics/AlgorithmRunner.java
    	spark/src/main/scala/com/adatao/spark/ddf/content/RepresentationHandler.scala

[33mcommit 7dd06ef9dd6ae8310234e6289ecf6ec818a36921[m
Merge: fa88389 eb1ac90
Author: Cuong Bui <cuongbuikien@gmail.com>
Date:   Mon Feb 24 22:44:12 2014 -0600

    Merged in ctn-tweaks (pull request #9)
    
    [HOTFIX] Provide a way for SparkDDFManager to get a dummy SparkDDF without throwing exceptions

[33mcommit eb1ac9054715f9a925b106534668679cb1679e53[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Mon Feb 24 20:41:18 2014 -0800

    [HOTFIX] Provide a way for SparkDDFManager to get a dummy SparkDDF without throwing exceptions

[33mcommit 857a832a16f55ee128e8c5288e811dcde8525d1e[m
Author: huandao0812 <huan@adatau.com>
Date:   Tue Feb 25 11:28:17 2014 +0700

    Fix AlgorithmRunner, refactor RepresentationHandler
    
    fix KmeansSuite.

[33mcommit 8aecd80e708848a9d4e6a758e210c0e270a86aed[m
Merge: 74ab161 6d04291
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Mon Feb 24 20:23:33 2014 -0800

    Merge branch 'bhan-stats' of https://bitbucket.org/adatao/ddf into bhan-stats
    
    Resolved Conflicts:
    	core/src/main/java/com/adatao/ddf/DDF.java

[33mcommit 74ab161db24f4f9a5a84ec28ba4b0f47b4dea822[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Mon Feb 24 20:10:04 2014 -0800

    [HOTFIX] Undo bad merge to make sure @bhan's code doesn't overwrite master's

[33mcommit 331ab335b85cc6231513349fc678fd20d020adb8[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Mon Feb 24 17:44:34 2014 -0800

    [REFACTOR] Cleanly separate out and make globally available
    
    a DDF.getConfigHandler() that everyone can use for configuration management.

[33mcommit 6d042910573ed595cabffa3bc065103bfde59101[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon Feb 24 17:38:39 2014 -0800

    resolve conflicts

[33mcommit 545d658668b7aa14d0bc10bbf3cea8f144a081ec[m
Merge: 91b4034 fa88389
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon Feb 24 17:15:44 2014 -0800

    resolve conflicts

[33mcommit fa883897a62c58c17b8dab1a5769ad4f68f4a410[m
Merge: 197d8da a4451f1
Author: Binh Han <bhan@adatao.com>
Date:   Mon Feb 24 16:43:26 2014 -0800

    Merged in ctn-ddf-config (pull request #6)
    
    [REFACTOR] Clearer separation of responsibilities between DDF and DDFManager

[33mcommit 91b40347d19ba242cf9f0478f812d7d5bb78db03[m
Merge: 6190094 197d8da
Author: Binh Han <bhan@adatao.com>
Date:   Mon Feb 24 16:40:44 2014 -0800

    Merged master into bhan-stats

[33mcommit a4451f18a3f7320b8e0c26919ee2bb923b4e8ee9[m
Merge: 234fd15 197d8da
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Mon Feb 24 13:10:59 2014 -0800

    Merge branch 'master' into ctn-ddf-config

[33mcommit 234fd15b195e77d9b7e56a5295cb4caa8da51df6[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Mon Feb 24 13:10:18 2014 -0800

    [TWEAK] Report to the user which ddf.ini we are using, per @bhan's comment

[33mcommit 619009448ba05789b9cedb14e0c23ffb161ee9e1[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Feb 24 23:42:57 2014 +0700

    Make Java code work with RDD

[33mcommit 197d8da2efdf83d6bf473263341500455472affd[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Mon Feb 24 10:43:48 2014 +0000

    README.md edited online with Bitbucket

[33mcommit dd81e26164c01e16f5b369322d981db7c8b07caf[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Mon Feb 24 10:40:07 2014 +0000

    README.md edited online with Bitbucket

[33mcommit 24a73214b671c49e9824eb421c7fa1a4de637807[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Mon Feb 24 10:39:02 2014 +0000

    README.md edited online with Bitbucket

[33mcommit 23182a290435956134e13a6ddacdd59e18b4cc0c[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Mon Feb 24 10:37:38 2014 +0000

    README.md edited online with Bitbucket

[33mcommit 96e2c387b5fc96bfe5a9759551c6a743bbc3c6aa[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Mon Feb 24 10:36:52 2014 +0000

    README.md edited online with Bitbucket

[33mcommit a626c76ebf9d26b6f7fae9b5ccc5b7e3a04650c3[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Mon Feb 24 10:36:04 2014 +0000

    README.md edited online with Bitbucket

[33mcommit 06f1d688d927cc66739728d6e0b3406ca18d069f[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Mon Feb 24 01:27:43 2014 -0800

    [FEATURE] Arrange for namespace to come from DDFManager

[33mcommit fd30df31a6e0f9900e24d62cf2be7658dbfcfc8b[m
Merge: 0b6dabb 75e1876
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Mon Feb 24 01:04:43 2014 -0800

    Merge branch 'master' into ctn-ddf-config
    
    Resolved Conflicts:
    	core/src/main/java/com/adatao/ddf/ADDFManager.java
    	core/src/main/java/com/adatao/ddf/DDF.java
    	core/src/main/java/com/adatao/ddf/DDFConfig.java
    	core/src/main/java/com/adatao/ddf/analytics/IComputeBasicStatistics.java
    	core/src/main/java/com/adatao/ddf/content/IHandleRepresentations.java
    	spark/src/test/java/com/adatao/spark/ddf/SparkDDFManagerTests.java

[33mcommit 0b6dabb5395acbf4f20db1e207eaebfe7895bf17[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sun Feb 23 22:53:06 2014 -0800

    [REFACTOR] Clearer separation of responsibilities between DDF and DDFManager
    
    See
    
    https://docs.google.com/a/adatao.com/drawings/d/1eEcIwQn6Go9NeuVhgkUvkQpJA7UmPP6eRakUQw-Tawg
    
    Basic unit tests passed.

[33mcommit f722d9559ace9087faf12c2c3dba835663ef8348[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Feb 24 12:16:58 2014 +0700

    fix test file path

[33mcommit 90b41a25c698f32803378efccda27c8875f7a5f1[m
Merge: cd35a48 b03195b
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Sun Feb 23 22:56:56 2014 -0600

    Merged with binh's fixes.

[33mcommit cd35a48aa0b882e1332ad08b3e1c15a75e13d342[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Sun Feb 23 22:54:15 2014 -0600

    Fixed Representation handler.

[33mcommit b03195b65275d27b51b61fa36890a822d6863046[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Feb 24 11:05:15 2014 +0700

    delete LOG

[33mcommit da246a890384cfed117cc464329806bdb72c088c[m
Author: huandao0812 <huan@adatau.com>
Date:   Mon Feb 24 10:57:41 2014 +0700

    change ColumnType.INTEGER => INT to match with hive's type
    
    fix RepresentationHandler.scala

[33mcommit 29cf284776e6869edd75d207f0ffa68475d72acc[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Sun Feb 23 17:32:40 2014 -0800

    fix ddf.ini

[33mcommit 75e1876f86f7646b89fe1087a8ddfeb207755ea9[m
Merge: 46c01c9 b5aac18
Author: huandao0812 <huan@adatau.com>
Date:   Sun Feb 23 17:59:27 2014 +0700

    Merged in ctn-tweaks (pull request #4)
    
    Tweak documentation

[33mcommit b5aac18fa6bf5bdb4c3aea5fbee669d7a7bea638[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sun Feb 23 00:58:42 2014 -0800

    [TWEAK]
    
    [Details]

[33mcommit a1ccb4940125abf79ac0a229cbf6b0aec4751446[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sun Feb 23 00:54:48 2014 -0800

    [TWEAK] Tweaking markdown
    
    [Details]

[33mcommit f2cbd278118492fb9f518eac8759fd1ac799015d[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sun Feb 23 00:51:53 2014 -0800

    [TWEAK] Use table in directory layout documentation
    
    [Details]

[33mcommit 46c01c97877570013a65686d8eef0779fc8702e3[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sun Feb 23 08:47:26 2014 +0000

    README.md edited online with Bitbucket

[33mcommit 04352b756880bbf48051192c9a7fc09b181a3e40[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sun Feb 23 08:41:52 2014 +0000

    README.md edited online with Bitbucket

[33mcommit cef94b135707349ba38d5f896ad22a24dc10651e[m
Merge: d06f080 5062c36
Author: huandao0812 <huan@adatau.com>
Date:   Sun Feb 23 15:24:34 2014 +0700

    Merged in ctn-tweaks (pull request #3)
    
    [WIP] Improve landing documentation

[33mcommit 5062c36608659779caef2a54a27b529036f3c7e3[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sat Feb 22 23:53:30 2014 -0800

    [WIP] Improve landing documentation
    
    Also added contrib/ directory in anticipation of user-contributed code.

[33mcommit edadf1bce5010561ee35955456c90a94d96b45f4[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Sat Feb 22 19:20:09 2014 -0800

    fix formatter

[33mcommit de4fd1f10308d7ff3382740d24286dead88c02c7[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Sat Feb 22 18:49:40 2014 -0800

    changes to BasicStatisticsCompute, RepresentationHandler, DDF

[33mcommit bbeee5b46ddbd91b3945d3b9ad66283be61edb14[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sat Feb 22 15:44:54 2014 -0800

    [TWEAK] Reall to trigger a notification test with bitbucket
    
    [Details]

[33mcommit d06f0806abe3bf74cef57f31b6fab6a8725424f1[m
Merge: c5b97ac c33dc10
Author: Adatao Inc. <git@adatao.com>
Date:   Sat Feb 22 10:56:44 2014 -0800

    Merged in ctn-tweaks (pull request #2)
    
    [TWEAK] Make git delete-branch NOT depend on the existence of a local copy

[33mcommit c33dc10e9540e2ae448dc1690f24d5a0b395ae69[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sat Feb 22 07:42:55 2014 -0800

    [TWEAK] Make git delete-branch NOT depend on the existence of a local copy
    
    Did you know that if you put this bin/ in your path, then you can start
    having GIT super-powers like:
    
        % git create-branch my-new-branch # creates a new local branch and a remote tracking branch
    		% git cb my-new-branch # even easier
    
    		% git update-branches # updates all branches you've got checked out
    		% git ub
    
    		% git commit-all # commits with a standard message template
    		% git ca

[33mcommit c5b97ac418d2fa81aaca9d92bc78b873d15adf88[m
Merge: f784326 81d62bb
Author: Binh Han <bhan@adatao.com>
Date:   Fri Feb 21 15:06:28 2014 -0800

    Merged in ctn-changes (pull request #1)
    
    [BUILD] Give more user-friendly error messages in run-once.sh

[33mcommit 81d62bb0b74c44b014c1b42271aa5650acf799a5[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Fri Feb 21 13:22:31 2014 -0800

    [BUILD] Give more user-friendly error messages in run-once.sh

[33mcommit f784326103c62cf812e2a55050a6c6bd70fbdd56[m
Merge: 636bd34 b61fe78
Author: Christopher Nguyen <ctn@users.noreply.github.com>
Date:   Fri Feb 21 12:20:09 2014 -0800

    Merge pull request #41 from adatao/ddh-factor1
    
    add work for FactorSupporter

[33mcommit b61fe78978e4b2f8eb3bcf6f3aed3c69691f8073[m
Merge: e187ada 636bd34
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Fri Feb 21 12:19:24 2014 -0800

    Merge branch 'master' into ddh-factor1
    
    Resolved Conflicts:
    	core/src/main/java/com/adatao/ddf/DDF.java
    	spark/src/test/java/com/adatao/spark/ddf/SparkDDFManagerTests.java

[33mcommit 636bd3457ec5e8863c748e35cd9173855fcc5a0f[m
Merge: f112f4a 14f7e7d
Author: Christopher Nguyen <ctn@users.noreply.github.com>
Date:   Fri Feb 21 12:14:27 2014 -0800

    Merge pull request #42 from adatao/bhan-stats
    
    IComputeBasicStatistics implementation

[33mcommit e187ada5102285948578bebb5035095101183aa2[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Fri Feb 21 02:28:22 2014 -0800

    [FEATURE] Provide support for Levels in Factor

[33mcommit 8bc4c8f7c707e0767bd20484876f9e6e5f7d2b82[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Thu Feb 20 22:25:45 2014 -0800

    [FEATURE] Introduce general support for Vector and Factor

[33mcommit 14f7e7d7fffc3fcdc93d09c35d73aaa019341bc6[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Feb 20 09:37:52 2014 -0800

    add DDFUtils

[33mcommit dcc960de933f0149cd0bf03cadecfe9e7ee523aa[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Feb 20 00:11:13 2014 -0800

    [STATS] Changes to Summary, passed unit tests
    fix DDF.get/setDDFManager naming, IRepresentationHandle implementation

[33mcommit 6353c92e6fa7ef57b43458ae55fbf7e17e8e50d2[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Feb 19 15:43:26 2014 -0800

    change ASummary to Summary

[33mcommit 08a5c75cfc119038b7aa1e429e15e90ecbeae6b4[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Feb 19 15:39:39 2014 -0800

    IComputeBasicStatistics implementation

[33mcommit 96acee0061e4278c3ba7aaa71022b0655bcdd640[m
Merge: 05023d3 f112f4a
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Tue Feb 18 19:07:08 2014 -0800

    Merge branch 'master' into ddh-factor1

[33mcommit 05023d3c5bef97fb3b5d12213e2a360fbe0676fa[m
Author: huandao0812 <daoduchuan@gmail.com>
Date:   Wed Feb 19 09:30:47 2014 +0700

    add work for FactorSupporter

[33mcommit f112f4a2d5cf18ad53339e4b130e2d23ee9a37b2[m
Merge: 086822f ac31804
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue Feb 18 12:58:15 2014 -0800

    Merge pull request #40 from adatao/ctn-ddf-config
    
    [REFACTOR] Put configuration code in package-private class `DDFConfig.java`

[33mcommit ac318046e5e3ebedb696dd3facd0e5c077849eab[m
Merge: 8b3bc81 086822f
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Tue Feb 18 00:40:12 2014 -0800

    Merge branch 'master' into ctn-ddf-config

[33mcommit 8b3bc81ed95d952e95e19e24bbca45d655b1abb7[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Tue Feb 18 00:37:54 2014 -0800

    [REFACTOR] Put configuration code in package-private class
    
    `DDFConfig.java`.
    
    Also implement actual support for reading from `ddf.ini`.
    Just make sure you have an environment variable named `DDF_INI`
    pointing to this file.

[33mcommit 086822fb682deb102068de779aa4e0bdab108ce4[m
Merge: 925187e 32b23ce
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon Feb 17 22:02:58 2014 -0800

    Merge pull request #39 from adatao/ctn-ddf-config
    
    [REFACTOR] Use declarative configuration file to support choice of DDF engine, manager, and handlers.

[33mcommit 32b23ce5030845628e09ab72ef38ea5727373e9c[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Mon Feb 17 21:57:26 2014 -0800

    [REFACTOR] Change Map<String, String> to a Config.Section
    
    class, to make it more obviously meaningful.

[33mcommit b49d94e2a307450a0c0726b07174d8708a256704[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Mon Feb 17 17:41:31 2014 -0800

    [MISC] Comment out the DDF.setDDFEngine()
    
    since "spark" is already the default.

[33mcommit 83043515cd3b8aa6c34a9225f482353d58159ca5[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Mon Feb 17 17:37:29 2014 -0800

    [REFACTOR] Use declarative configuration file to
    
    support choice of DDF engine, manager, and handlers.
    This gives us three beautiful things:
    
    1. The ability to refactor a lot of the engine-specific code into the base API
    2. The true ability to specify, at run time, which component classes to load,
       without having to have it compiled in. This makes it easy to mix-and-match
    	 different component implementations.
    3. The ability for client code to be mostly engine-agnostic, except for the initial
       engine specification. In fact, even that line can be omitted if the user uses
    	 the default engine, "spark".
    
    Read it and weep!

[33mcommit 925187e2212be5bf6fbc29f72ab707a4d015b026[m
Merge: beb2feb adb0d72
Author: Huandao0812 <daoduchuan88@gmail.com>
Date:   Mon Feb 17 22:50:46 2014 +0700

    Merge pull request #36 from adatao/ckb-integration2
    
    Ckb integration2

[33mcommit adb0d726d0d027b7f9ae8bd836cb852220759db2[m
Merge: 92461a8 752be19
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sun Feb 16 23:34:58 2014 -0800

    [RESOLVE] Merge branch 'ctn-sql' into ckb-integration2
    
    Fixes to make ckb-intergration2 merge-able.
    
    Conflicts resolved:
    	spark/src/main/java/com/adatao/spark/ddf/SparkDDFManager.java
    	spark/src/main/java/com/adatao/spark/ddf/content/MetaDataHandler.java
    	spark/src/main/java/com/adatao/spark/ddf/content/SparkRepresentationHandler.java
    	spark/src/main/scala/com/adatao/spark/ddf/content/RepresentationHandler.scala
    	spark/src/test/java/com/adatao/spark/ddf/SparkDDFManagerTests.java

[33mcommit 752be197c6f3febbe3e98f840e0db62371e14c18[m
Merge: e03c9b8 beb2feb
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sun Feb 16 23:25:17 2014 -0800

    Merge branch 'master' into ctn-sql

[33mcommit e03c9b8fe5605804caca620e178f2cdfb96d8820[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sun Feb 16 23:24:17 2014 -0800

    [RENAME] No point generalizing to cmd2 when sql2
    
    is far more meaningful and a dominant use case.

[33mcommit beb2febf5e0a3d05b8e59104cf4d3843462562ec[m
Merge: d66751d ed3ffb3
Author: Christopher Nguyen <ctn@users.noreply.github.com>
Date:   Sun Feb 16 23:22:50 2014 -0800

    Merge pull request #37 from adatao/ctn-sql
    
    [RENAME] Drop the repetitive prefix Spark in front

[33mcommit ed3ffb347ca0f4ac4fb8794015fc721a3289756f[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sun Feb 16 22:42:43 2014 -0800

    [RENAME] Drop the repetitive prefix Spark in front
    
    of all the Spark handlers. It does make sense to keep it in front
    of `SparkDDF` and `SparkDDFManager`, since these are user-facing
    and we want the user to know what kind of DDFs they are dealing with.
    
    For the others, the naming is just too long, and there is little chance
    of conflict/confusion with the base API.

[33mcommit d66751dbf66d8985fa03b362bcda8cf2b523e173[m
Merge: edbf9eb 06df092
Author: Christopher Nguyen <ctn@users.noreply.github.com>
Date:   Sun Feb 16 22:16:41 2014 -0800

    Merge pull request #35 from adatao/ddh-fix1
    
    delete SparkRepresentationHandler.java

[33mcommit 06df0923578e25f86faab961f8cd0b5a2d9bf90a[m
Author: huandao0812 <daoduchuan@gmail.com>
Date:   Mon Feb 17 12:16:21 2014 +0700

    delete SparkRepresentationHandler.java

[33mcommit 92461a83076f5bcc289e38786708b488219344c5[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Sun Feb 16 19:57:40 2014 -0600

    Add proxy methods to python class.

[33mcommit 5a87738143877581297ea47b443eb559a3802714[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Sun Feb 16 19:19:58 2014 -0600

    Fixed the select count command.

[33mcommit edbf9ebf99222819c5d535b33793c3d73543977a[m
Merge: d4ca162 92dab9d
Author: bhan <binhhan.gt@gmail.com>
Date:   Sun Feb 16 16:29:30 2014 -0800

    Merge pull request #34 from adatao/ddf-load

[33mcommit 92dab9d920d7c804ad912bcafb90d8894dbe996f[m
Author: bhan <bhan@adatao.com>
Date:   Sun Feb 16 16:26:12 2014 -0800

    rename get/setDDFManager to get/setManager

[33mcommit 68622e7f4d2f98521c36683288efebc23c499416[m
Merge: 76f7876 3b351cd
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Sun Feb 16 17:51:29 2014 -0600

    Merged Binh's work on getColumns but still not working correctly yet.

[33mcommit 76f7876f8c5e1081108f3b755f83092566dfe3b4[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Sun Feb 16 17:24:45 2014 -0600

    Added some handlers for Spark.

[33mcommit 3b351cd50e9806818867f376bc9063e249c13343[m
Author: bhan <bhan@adatao.com>
Date:   Sun Feb 16 12:53:56 2014 -0800

    merge branch master into ddf-load

[33mcommit ae61f0207f3c45add1378fcc47d6515025689d2e[m
Merge: e9a02a9 d4ca162
Author: bhan <bhan@adatao.com>
Date:   Sun Feb 16 12:39:40 2014 -0800

    Merge remote-tracking branch 'origin/master' into ddf-load
    
    Conflicts:
    	core/src/main/java/com/adatao/ddf/content/AMetaDataHandler.java
    	core/src/main/java/com/adatao/ddf/content/IHandleMetaData.java
    	spark/src/main/java/com/adatao/spark/ddf/SparkDDFManager.java
    	spark/src/main/java/com/adatao/spark/ddf/content/SparkRepresentationHandler.java

[33mcommit e9a02a91ec033fb037f0765da0540a792942e170[m
Author: bhan <bhan@adatao.com>
Date:   Sun Feb 16 12:29:57 2014 -0800

    move getNumColumns to IHandleSchema, create DDF tablename if null

[33mcommit d4ca162eeebeccea5a9ce39ad2ee851976a9f833[m
Merge: 6f72d86 0f18322
Author: Christopher Nguyen <ctn@users.noreply.github.com>
Date:   Sun Feb 16 11:44:58 2014 -0800

    Merge pull request #30 from adatao/ddh-fix
    
    Ddh fix

[33mcommit 0f18322b327d0359f26495cebe58f861034d7901[m
Author: huandao0812 <daoduchuan@gmail.com>
Date:   Mon Feb 17 02:38:57 2014 +0700

    change Exception to DDFException

[33mcommit de5d547ca1c7fd33029025a885b48bdc9457afd3[m
Author: huandao0812 <daoduchuan@gmail.com>
Date:   Mon Feb 17 02:24:03 2014 +0700

    fix style

[33mcommit 1009fbc94cfedd8b3609428c1885fdb3b70f440b[m
Author: huandao0812 <daoduchuan@gmail.com>
Date:   Mon Feb 17 02:19:33 2014 +0700

    get rid of FLOAT, undo change in AMetaDataHandler
    
    fix code style, formatting

[33mcommit b43211b6b826b9ef516f025866092b28ab0ca191[m
Author: bhan <bhan@adatao.com>
Date:   Sun Feb 16 10:58:36 2014 -0800

    refactor Container to DDFManager

[33mcommit 419fe761ba288011c0a814b9d84a635ab41e8a34[m
Author: huandao0812 <daoduchuan@gmail.com>
Date:   Mon Feb 17 01:33:57 2014 +0700

    reformat code

[33mcommit 45ee4cdfda33dae87dc3bbbe298de76778ae3e47[m
Author: bhan <bhan@adatao.com>
Date:   Sun Feb 16 10:32:30 2014 -0800

    fix codestyle and refactor Container to DDFManager

[33mcommit 2bfd2eac9d8d49274c4331b9d8643ae72e6ee095[m
Author: huandao0812 <daoduchuan@gmail.com>
Date:   Mon Feb 17 01:32:03 2014 +0700

    reformat code

[33mcommit c437a335bbaa41de8a6305ff3aba65c982361ed1[m
Author: huandao0812 <daoduchuan@gmail.com>
Date:   Mon Feb 17 01:28:04 2014 +0700

    change ARunAlgorithms to AAlgorithmRunner

[33mcommit b0c33790445882bb08f2c1251a5a03b930f0dec3[m
Author: huandao0812 <daoduchuan@gmail.com>
Date:   Mon Feb 17 01:25:30 2014 +0700

    delete SparkMetaDataHandler.scala

[33mcommit 20be37ed7dbc8ecfd1fa438a3bccb6a9bd8c3d38[m
Author: huandao0812 <daoduchuan@gmail.com>
Date:   Mon Feb 17 01:23:43 2014 +0700

    fix formatting, change DDF.runAlgorithm() to DDF.train()

[33mcommit 9c0159648a52f32427d2c372acf7f9dc728f68d6[m
Author: bhan <bhan@adatao.com>
Date:   Sun Feb 16 09:54:01 2014 -0800

    Shark-based implementation of DDF getNumRows

[33mcommit aebc3a7f256f73b93da9536889b979f5fb8bdc12[m
Author: huandao0812 <daoduchuan@gmail.com>
Date:   Mon Feb 17 00:20:40 2014 +0700

    fix IAlgorithm mispell, fix formatting for Kmeans.scala
    
    refactor SparkMetaData.scala

[33mcommit c7b4f845b2c06587165347a33ce8ba266106456d[m
Author: huandao0812 <daoduchuan@gmail.com>
Date:   Sun Feb 16 23:32:35 2014 +0700

    add method runAlgorithm to DDF
    
    add ATestSuite to spark project, add KmeansSuite

[33mcommit 6cb528a24a3e907f25b2c384e19cb60522f4f29c[m
Author: huandao0812 <daoduchuan@gmail.com>
Date:   Sun Feb 16 14:08:30 2014 +0700

    delete RepresentationHandlerTestSuite

[33mcommit ddac03db9c3bbf4363cce11ba3e382d46f95e8ac[m
Author: huandao0812 <daoduchuan@gmail.com>
Date:   Sun Feb 16 13:42:36 2014 +0700

    add default constructor for Kmeans()

[33mcommit a66f091d9f34d32f1b732e6806ffda24f42f873e[m
Merge: ba28ac6 6f72d86
Author: huandao0812 <daoduchuan@gmail.com>
Date:   Sun Feb 16 12:45:14 2014 +0700

    Merge branch 'master' into ddh-fix
    
    Conflicts:
    	.gitignore
    	core/src/main/java/com/adatao/ddf/DDF.java
    	spark/src/main/java/com/adatao/spark/ddf/SparkDDFManager.java

[33mcommit 6f72d864650f7e778e0a4cc8ae1b48abb3368f1d[m
Merge: d0982c5 29d9098
Author: ckbui <cuongbuikien@gmail.com>
Date:   Sat Feb 15 20:59:29 2014 -0600

    Merge pull request #32 from adatao/ckb-integration1
    
    Java Client seems to work well

[33mcommit d0982c5392fdaf011e897e6e5dd09c24ba4f737c[m
Merge: 01582dc ec3ed9b
Author: Christopher Nguyen <ctn@users.noreply.github.com>
Date:   Sat Feb 15 18:25:10 2014 -0800

    Merge pull request #25 from adatao/ckb-pyclient
    
    Python Client setup is done

[33mcommit 29d9098d7d9ed36c9c079b6b351ebb7dbb50a04b[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sat Feb 15 17:00:07 2014 -0800

    [REFACTOR] Generalize to cmd2ddf and cmd2txt,
    
    put them under IHandleDataCommands.
    
    Both DDF and DDFManager implement this interface, with one slight difference.
    DDFManager's implementation is obvious, so what's DDF's?
    DDF does the same thing as DDFManager, except it does one more thing:
    it automatically inserts its table name into the command string,
    wherever it sees the regex pattern "<table>". This way the API user can
    do this:
    
       ddf1.sql2ddf("SELECT column1, column4 from <table>");
    
    which would operate the SQL command on `ddf1`

[33mcommit ec3ed9bc655b02e5696027dbe2b64d78d1d94ae6[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Sat Feb 15 18:56:25 2014 -0600

    Move spark_manager.py to spark_ddf.py

[33mcommit 72902aee1f11a9f10827ece8378e5e97fcead90e[m
Merge: 1b5ab49 01582dc
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sat Feb 15 16:00:28 2014 -0800

    Merge branch 'master' into ckb-integration1

[33mcommit 1b5ab493fbb00e93c1a842b89bdc42eb93e024fd[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Sat Feb 15 17:51:57 2014 -0600

    Removed system print out and catch exception when running sql commands.

[33mcommit fc33df6a95ff6845cbb944d58aafd474bfec3a49[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Sat Feb 15 17:12:20 2014 -0600

    add the load command to SparkDDFManager.

[33mcommit 8f75065cd444857da6c96af3a9b757be5a2847c6[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Sat Feb 15 17:00:49 2014 -0600

    Fixed class name errors and rename the file manager.py to spark_manager.

[33mcommit 01582dc041ae285e76c336cc9d768bd9799e1ec1[m
Merge: b9127c8 9580d40
Author: bhan <binhhan.gt@gmail.com>
Date:   Sat Feb 15 12:31:22 2014 -0800

    Merge pull request #33 from adatao/ddf-load
    
    [REFACTOR] Move SQL loading implementation to SparkDDFPersistenceHandler, where it belongs per our design

[33mcommit 9580d40536aa5f306350bf5d5bbe4e59b8d30ad7[m
Merge: 1a1066c b9127c8
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sat Feb 15 12:04:00 2014 -0800

    Merge branch 'master' into ddf-load

[33mcommit 1a1066c246fbf7b2b57c3d58aa5b821133870da6[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sat Feb 15 12:03:15 2014 -0800

    [REFACTOR] Move SQL loading implementation to
    
    SparkDDFPersistenceHandler, where it belongs per our design.

[33mcommit f654fd559a32e7a30c90ae46017a9dd13d814069[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Sat Feb 15 13:07:21 2014 -0600

    Rename the DDFManager to SparkDDFManager.

[33mcommit 804103c6bcf74911599a2839b78f512dcd2dba7c[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Sat Feb 15 13:04:48 2014 -0600

    Add log4j configuration file.

[33mcommit ba28ac64a83bf054163d579319e3f560efae8701[m
Author: huandao0812 <daoduchuan@gmail.com>
Date:   Sun Feb 16 01:33:03 2014 +0700

    delete commented out codes

[33mcommit 93726255ee79b5912ffb420eff457c71719e4ef7[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Sat Feb 15 12:24:11 2014 -0600

    Add configuration and test data for running sql queries.

[33mcommit 0df5f47fe0ec8b309ed3871f4847bfa806633d9c[m
Merge: dda0ea1 b9127c8
Author: huandao0812 <daoduchuan@gmail.com>
Date:   Sat Feb 15 23:26:36 2014 +0700

    Merge branch 'master' into ddh-fix
    
    Conflicts:
    	spark/src/main/java/com/adatao/spark/ddf/SparkDDFManager.java

[33mcommit 58ef62eb85adf3d762b2ae35db1e1bcfef955253[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Sat Feb 15 08:14:41 2014 -0600

    Added thrift dependencies for Shark engine.

[33mcommit 1686af95ddcc2eccec3b11b1e7a07bec13fd9fae[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Sat Feb 15 06:45:37 2014 -0600

    Fixed override errors.

[33mcommit b9127c8ad75361e1b37c5ab40db8eeadc3484d3e[m
Merge: d088512 7e7c099
Author: ckbui <cuongbuikien@gmail.com>
Date:   Sat Feb 15 05:13:16 2014 -0600

    Merge pull request #31 from adatao/ddf-load
    
    [REFACTOR] Use "raw" Spark Scala classes as much as possible

[33mcommit 7e7c09914ca984e74a3556910725a67942d0f9ff[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sat Feb 15 01:50:40 2014 -0800

    [REFACTOR] Use "raw" Spark Scala classes as much
    
    as possible, e.g., SharkContext instead of JavaSharkContext.
    
    Also make sure to instantiate a new SparkDDFManager when loading
    and instantiating a new SparkDDF.

[33mcommit 56f415305c265ff70d5dea2155b96f77ad6ee09a[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sat Feb 15 00:56:38 2014 -0800

    [STYLE] Copied style .xml from BigR/eclipse

[33mcommit dda0ea1ead1c34586d6bef1306fa272104a28e3b[m
Author: huandao0812 <daoduchuan@gmail.com>
Date:   Sat Feb 15 15:06:51 2014 +0700

    fix formatting

[33mcommit f2101660f1ab9c6377ce2cefe8961e16badb8a16[m
Author: huandao0812 <daoduchuan@gmail.com>
Date:   Sat Feb 15 15:00:48 2014 +0700

    fix formatting

[33mcommit 1792e3f80c56400cdf19994b326f7452ab1dda5b[m
Author: huandao0812 <daoduchuan@gmail.com>
Date:   Sat Feb 15 14:26:22 2014 +0700

    add LabeledPoint support to SparkRepresentationHandler

[33mcommit be98420fb8cd894b2374955c3a1e6461464e85c5[m
Author: huandao0812 <daoduchuan@gmail.com>
Date:   Sat Feb 15 12:35:20 2014 +0700

    change return type getNumColumns() to int
    
    add default constructor for DDF

[33mcommit 4dd2a6bf390df820009978d08aaf8b727bcf95e9[m
Author: huandao0812 <daoduchuan@gmail.com>
Date:   Sat Feb 15 10:47:58 2014 +0700

    add Kmeans.scala, SparkMetaData.scala, SparkViewHandler.scala,
    
    SparkPersistenceHandler.scala, SparkRepresentationHandler.scala

[33mcommit 6bdba8c83cfd6f6006c765383d54616809dcd9e4[m
Merge: 912b3c0 08641ce
Author: huandao0812 <daoduchuan@gmail.com>
Date:   Sat Feb 15 10:44:09 2014 +0700

    Merge branch 'ddf-load' into ddh-fix
    
    Conflicts:
    	core/src/main/java/com/adatao/ddf/DDF.java
    	core/src/main/java/com/adatao/ddf/content/Schema.java
    	spark/src/main/java/com/adatao/ddf/spark/SparkDDF.java
    	spark/src/main/java/com/adatao/spark/ddf/SparkDDFManager.java

[33mcommit d0885125c24f1a5c022d960946031a24793c5cdd[m
Merge: d1e71da 08641ce
Author: Christopher Nguyen <ctn@users.noreply.github.com>
Date:   Fri Feb 14 18:02:43 2014 -0800

    Merge pull request #27 from adatao/ddf-load
    
    Ddf load

[33mcommit 9d1371ea02307de1217716e24e2904e2c82caad7[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Fri Feb 14 09:44:27 2014 -0600

    Rename DDFContext to DDFManager.

[33mcommit 08641cebbffd7e9c762f1f8022da2ff4b0c77743[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Fri Feb 14 03:22:37 2014 -0800

    [REFACTOR] Move com.adatao.ddf.spark to com.adatao.spark.ddf
    
    Also delegate methods to handlers as much as appropriate.

[33mcommit 912b3c09c4c13cf013167ae4844cf0c05931220b[m
Author: huandao0812 <daoduchuan@gmail.com>
Date:   Fri Feb 14 17:24:10 2014 +0700

    rename RepresentationHandler -> SparkRepresentationHandler
    
    and PersistenceHandler -> SparkPersistenceHandler
    add work for IRunAlgorithms and Kmeans

[33mcommit 285e774fb982d2703dbe0563920dfbd08f2faac2[m
Merge: 4bb1154 6b37814
Author: huandao0812 <daoduchuan@gmail.com>
Date:   Fri Feb 14 11:50:05 2014 +0700

    Merge branch 'ddf-load' into ddh-fix

[33mcommit 4bb11544127da75686e08129b18148ba78e49e66[m
Author: huandao0812 <daoduchuan@gmail.com>
Date:   Fri Feb 14 11:48:39 2014 +0700

    add back ViewHandler.scala, PersistenceHandler.scala and RepresentationHandler.scala

[33mcommit 6b37814de565d3aa7f73823fe4e8ad111fd942a8[m
Author: bhan <bhan@adatao.com>
Date:   Thu Feb 13 20:35:40 2014 -0800

    load hive table to ddf

[33mcommit ba40b36e402bb4fe2d54249d5bf9efda7ca73469[m
Author: bhan <bhan@adatao.com>
Date:   Thu Feb 13 20:32:07 2014 -0800

    load hive table to ddf

[33mcommit d1e71da29bea667d7bc1f9c0fb8276448a84a2bc[m
Merge: f56c3b6 5eaf891
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Feb 13 18:45:04 2014 -0800

    Merge pull request #26 from adatao/ctn-changes
    
    [REFACTOR] Move Spark environment/system-property into SparkDDFManager

[33mcommit 5eaf891cf24581e5f6dec43e3b80b6165eae3702[m
Merge: 4027208 f56c3b6
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Thu Feb 13 17:58:54 2014 -0800

    Merge branch 'master' into ctn-changes

[33mcommit 4027208284628ab78f41ca438d14dda8b168e6b4[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Thu Feb 13 17:58:14 2014 -0800

    [REFACTOR] Move Spark environment/system-property
    
    parsing into SparkDDFManager.

[33mcommit f56c3b62a5f2dba8a847b02cf2b23ac66812bccd[m
Merge: 2fcf31f d419a49
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Feb 13 17:14:34 2014 -0800

    Merge pull request #24 from adatao/ctn-changes
    
    [REFACTOR] Rename ADDFHelper to ADDFManager,

[33mcommit d419a4925e3d0f7383b76bc713bffbdf7e7f3e0d[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Thu Feb 13 16:51:43 2014 -0800

    [REFACTOR] Use strongly typed Schema in load()
    
    signatures, and simultaneously provide an easy way for users
    to start with a String and have it parsed into a Schema object.

[33mcommit 07d4f750fa7c0539722fccef013e2c53310946e0[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Thu Feb 13 14:14:09 2014 -0600

    Fixed the import statements.

[33mcommit eb915f8ceb494ceb1d1a6c091112ad4eb96e1469[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Thu Feb 13 13:57:33 2014 -0600

    Got the DDF Python Client working.

[33mcommit 235b6a8cffe7857a22b9a2f0a8a387d3c01e237a[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Thu Feb 13 11:14:05 2014 -0600

    Almost done with DDF PyClient setup.

[33mcommit c5dddc57173f7d603fe7f962330e45b3a8fc3029[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Thu Feb 13 08:48:12 2014 -0800

    [REFACTOR] Rename ADDFHelper to ADDFManager,
    
    and have ADDFManager take on the role of providing
    initialization and simple "factory" methods like
    loading from disk to generate a new DDF.
    
    KISS.

[33mcommit 419c0e4bc815cf896001f8298b6061d7d2176ae0[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Thu Feb 13 09:11:44 2014 -0600

    Restructure the pyclient package.

[33mcommit 002128643f7d1b0fc192b2698f3e138d304d3d4b[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Thu Feb 13 07:28:05 2014 -0600

    Start working on the Python client.

[33mcommit 2fcf31fbea8a5af166de448aa7137cd448045f1d[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Wed Feb 12 19:19:02 2014 -0600

    Removed duplicated tests.

[33mcommit 41f6ec5c06258af15bbd4f0a6aefe5e47718b95e[m
Merge: 55a505a b016bf7
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Wed Feb 12 18:54:44 2014 -0600

    Merge pull request #23 into master.

[33mcommit b016bf788fe2a60ea77ddc50e4e2363b0010e191[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Wed Feb 12 16:05:49 2014 -0600

    Minor bug fix.

[33mcommit a86fb607638fea942d890a1ab1a2cabc11740d3b[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Wed Feb 12 15:56:14 2014 -0600

    Fixed the checkstyle path.

[33mcommit f2fecd9921dec0b4a698ce6026d9ef54044da781[m
Author: Cuong Kien Bui <cuongbui@adatao.com>
Date:   Wed Feb 12 15:15:45 2014 -0600

    Fixed maven pom file in RootBuild.scala

[33mcommit bb8d344dc03eaea03a0a2dbe7b4e59d5be68a1d9[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Wed Feb 12 14:43:20 2014 -0600

    Added shutdown method to SparkDDFContext to support multiple tests.

[33mcommit cabb9f8224e3c4c3ebd5c1e301c4cd7fca4a7cc8[m
Author: Cuong Kien Bui <cuongbui@adatao.com>
Date:   Wed Feb 12 12:31:25 2014 -0600

    Added a test to show short way to get DDFContext.

[33mcommit 533a205d30539be75dbaede70890af379ed5dcdc[m
Author: Cuong Kien Bui <cuongbui@adatao.com>
Date:   Wed Feb 12 12:24:46 2014 -0600

    Makde DDFContext smarter in setting up connection.

[33mcommit 55014275013e7b20ad776013849d2b29f87d1d68[m
Author: Cuong Kien Bui <cuongbui@adatao.com>
Date:   Wed Feb 12 11:39:33 2014 -0600

    Added description for several classes.

[33mcommit 9e443aecfb84302c8c127651b860a8be7ade48a7[m
Author: Cuong Kien Bui <cuongbui@adatao.com>
Date:   Wed Feb 12 11:34:01 2014 -0600

    Added the readCSV method and several constructors for DDFException class.

[33mcommit 55a505a6f85f312593bea5bd0d4687c7570fc3a7[m
Merge: 8857dac a21ac37
Author: bhan <binhhan.gt@gmail.com>
Date:   Tue Feb 11 16:27:29 2014 -0800

    Merge pull request #21 from adatao/bhan-work
    
     Refactor DDF, AMetaDataHandler, remove IHandleFilteringAndProjections, initial IRunAlgorithms design

[33mcommit a21ac37f74c1004653984b8a516aa6eb128b57a6[m
Author: bhan <bhan@adatao.com>
Date:   Tue Feb 11 13:25:09 2014 -0800

    change to ddf schema

[33mcommit 61e394185ac320ee348b3f351dd76d289193c1e0[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Tue Feb 11 14:44:48 2014 -0600

    Fixed some minor errors.

[33mcommit 2ef4149123b7193566dea0d3884cd439ef8997ba[m
Author: Cuong Kien Bui <cuongbui@adatao.com>
Date:   Tue Feb 11 14:04:26 2014 -0600

    Rename again: DDFFactory to DDFContext, DDFDriver to DDFContextFactory

[33mcommit 20bfbc2ffa26ffc0353725cec8a5cdcfe7e79901[m
Author: Cuong Kien Bui <cuongbui@adatao.com>
Date:   Tue Feb 11 13:47:04 2014 -0600

    Rename DDFDriver to DDFContext to avoid confusion with JDBC drivers.

[33mcommit a818f02d89aa68e6cec63d6b2902bb4aa2fa282e[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Tue Feb 11 12:50:52 2014 -0600

    Almost done with Java gateway set up of DDF Python client.

[33mcommit 718d92607a43ae3be388b567a942b37f9904a159[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Tue Feb 11 11:15:34 2014 -0600

    Start working on Python client for DDF.

[33mcommit f220191b9f788d8699230d4ccbd670b85a15f0c2[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Tue Feb 11 11:03:53 2014 -0600

    Move RClient module into clients/R

[33mcommit 888b1d4edac0b5458dc7c0e876000b53f7d2e58f[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Tue Feb 11 06:24:15 2014 -0600

    Fixed the test cases for Spark module.

[33mcommit f4c03ac38c1d5a0d1c74d15d29209ac737c57a95[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Tue Feb 11 06:18:54 2014 -0600

    Added DDFSPARK_JAR to maven pom to facilitate test cases.

[33mcommit 6e9803576c6667a89e695ee4f0aeb3c9bd69c879[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Tue Feb 11 06:17:59 2014 -0600

    Added examples for RClient module.

[33mcommit c69d2020b63eb5fbc867b9a3075dd5bc7dcd304a[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Tue Feb 11 06:17:07 2014 -0600

    Ignore the inst/java folder in RClient project.

[33mcommit dc950a71e9dceb01c13abe3d7d34f4604cbe4d59[m
Author: bhan <bhan@adatao.com>
Date:   Mon Feb 10 20:41:07 2014 -0800

    fix ddf schema

[33mcommit ac615a47923429c227803e6edd6ba5c4eb4b076c[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Mon Feb 10 22:40:29 2014 -0600

    Added dependency plugin to generate third party libraries, in order to copy them into RClient/adatao.ddf/inst/java

[33mcommit a3fdc34658ef70d84c9e79613a3cd7fd80ebb90e[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Mon Feb 10 22:03:48 2014 -0600

    A test command with rJava.

[33mcommit 06914dca52586d428924c98f29e35623cb2c1915[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Mon Feb 10 21:34:55 2014 -0600

    Start working on RClient.

[33mcommit 558659c4cbb258abd79d64a225c114aa7faa7b9e[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Sun Feb 9 21:24:27 2014 -0600

    Move the code Java for better audience coverage.

[33mcommit 4dee190dfcd23aac5061ee9fb8d6b05b50a8f63a[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Sun Feb 9 19:58:02 2014 -0600

    Added an utility method to get DDF Factory directly from Driver Manager.

[33mcommit 7d5ddc7c0598bf172d49ab2515ffad1a4415f6f5[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Sun Feb 9 19:19:46 2014 -0600

    Added example to DDFFactory stub.

[33mcommit 657fb8964052f86a83676062e565f3fc14f403d0[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Sun Feb 9 16:39:14 2014 -0600

    Added unit test for creating a connection to a Spark cluster.

[33mcommit da3f4264576d72d0b2d59ba451555ea9ff268f03[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Sun Feb 9 16:30:11 2014 -0600

    Added DDFFactory class.

[33mcommit 70804b56ffd70859df2a6a834045cd54c4fe1e67[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Sun Feb 9 15:11:33 2014 -0600

    Got the first test suite done for DDF driver manager with Spark DDFDriver.

[33mcommit 89e93e2e4d1d2549bbfbac9beab2daedc3681f1d[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Sun Feb 9 14:48:46 2014 -0600

    Added a test for Spark DDFDriver.

[33mcommit ddbe3819ce5b6812a4fe6762f3dd1de248649dea[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Sun Feb 9 14:04:49 2014 -0600

    Adding a new module: examples to demonstrate how to
    use our API.

[33mcommit 34ee6e56bc3daf73d9907b058e76486cd588a381[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Sun Feb 9 13:30:20 2014 -0600

    Start small with the DDFDriverManager and DDFDriver.

[33mcommit 9dfa734bcf3ea78dd4fdee0a373e58a1fc381ca6[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Sun Feb 9 11:15:29 2014 -0600

    Simplifying the design of DDF by using the Bridge Pattern.

[33mcommit 697566ba66aa9d038a6364cd53bcaebe1684568b[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Fri Feb 7 21:45:43 2014 -0800

    Update DDF.java

[33mcommit 8857dacf9e5b0d3a58a2641c1df7c4723d2f47f1[m
Merge: 6270f3d 55772a0
Author: ckbui <cuongbuikien@gmail.com>
Date:   Fri Feb 7 22:21:59 2014 -0600

    Merge pull request #20 from adatao/ctn-changes
    
    [INTEGRATION] Added com.adatao.ddf.spark.IntegrationTests.java

[33mcommit 2d71dfacd5231d9f01ae65451d5f543c25ddc369[m
Merge: 0e6bf8c 5b268fb
Author: bhan <bhan@adatao.com>
Date:   Fri Feb 7 19:49:16 2014 -0800

    Merge branch 'bhan-work' of https://github.com/adatao/DDF into bhan-work
    
    Conflicts:
    	core/src/main/java/com/adatao/ddf/DDF.java

[33mcommit 0e6bf8c7c39d6ddd2a5fff8f8c5aa53a7899df23[m
Author: bhan <bhan@adatao.com>
Date:   Fri Feb 7 19:26:08 2014 -0800

    fix import

[33mcommit 8dd262c05c7c6ad4ac3513f0ee1d9f2ce44b9a6b[m
Author: bhan <bhan@adatao.com>
Date:   Fri Feb 7 18:57:53 2014 -0800

    Refactor AMetaDataHandler and DDF, remove IHandleProjectionAndFiltering, initial IRunAlgorithms design

[33mcommit 0d6c5e980521116764adf27252a261ea97716e76[m
Author: bhan <bhan@adatao.com>
Date:   Fri Feb 7 16:36:51 2014 -0800

    Handle DDF metadata

[33mcommit 5b268fb0715d0a6009ec4eb11294894f5c34026c[m
Author: bhan <bhan@adatao.com>
Date:   Fri Feb 7 19:26:08 2014 -0800

    fix import

[33mcommit 55772a0c0832c638ab26ec04e8995bc5cabf2efa[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Fri Feb 7 19:16:52 2014 -0800

    [INTEGRATION] Added com.adatao.ddf.spark.IntegrationTests.java
    
    to be owned by @ckb.

[33mcommit 8ee14479e5020094a23fe6f51568c78d1f071d3d[m
Author: bhan <bhan@adatao.com>
Date:   Fri Feb 7 18:57:53 2014 -0800

    Refactor AMetaDataHandler and DDF, remove IHandleProjectionAndFiltering, initial IRunAlgorithms design

[33mcommit 6270f3d30a1af5b6e1b49ec4dad034ee96a1e62b[m
Merge: d26fefe 7478a1b
Author: bhan <binhhan.gt@gmail.com>
Date:   Fri Feb 7 17:50:29 2014 -0800

    Merge pull request #19 from adatao/ctn-changes
    
    [FEATURE] Added direct getters for DDF handlers.

[33mcommit 7478a1b0a287a1407e9306df78f6f30bbde49294[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Fri Feb 7 17:46:24 2014 -0800

    [COMMENT] Fix erroneous comment about SQL data source

[33mcommit ddd5bd0b72ef0e3e6122c728ef44f1791700fe01[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Fri Feb 7 17:41:12 2014 -0800

    [FEATURE] Added direct getters for DDF handlers.
    
    Also redefine IHandleViews to reflect latest design in
    
    https://docs.google.com/a/adatao.com/drawings/d/1COA82W8AIpyDNWAV9iV0fhxAALSLwGj1ThWCpnM07ks/edit

[33mcommit 2b008f56d60eceaed26350babeb1b086a4268835[m
Author: bhan <bhan@adatao.com>
Date:   Fri Feb 7 16:36:51 2014 -0800

    Handle DDF metadata

[33mcommit d26fefeedd1b3547a3451e32798948a537f1d34a[m
Merge: 0802a5e b5d5282
Author: bhan <binhhan.gt@gmail.com>
Date:   Fri Feb 7 15:33:23 2014 -0800

    Merge pull request #17 from adatao/ctn-changes
    
    	[INSTALL] Don't ignore .jars; add unmanaged jars

[33mcommit b5d5282c85f9173936ed96d37699f6f5af1c30df[m
Merge: 767dc15 0802a5e
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Fri Feb 7 15:32:10 2014 -0800

    Merge branch 'master' into ctn-changes

[33mcommit 767dc15998bb2aeb802d2cf022dd9ba7aef7d31f[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Fri Feb 7 15:30:45 2014 -0800

    [INSTALL] Don't ignore .jars; add unmanaged jars

[33mcommit 0802a5e981a6edf177288e4b80658dad7863c4ad[m
Merge: f1987a8 d4fc7a5
Author: bhan <binhhan.gt@gmail.com>
Date:   Fri Feb 7 15:13:55 2014 -0800

    Merge pull request #16 from adatao/ctn-changes
    
    [INSTALL] Add edu.berkeley to the list of .m2 and

[33mcommit d4fc7a5ace1dcd571a3e7f4aeb9b12b13b8b4401[m
Merge: 93141af f1987a8
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Fri Feb 7 15:13:10 2014 -0800

    Merge branch 'master' into ctn-changes

[33mcommit 93141af0db4b78bcbe823191c5a75970a6eae6ec[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Fri Feb 7 15:11:35 2014 -0800

    [INSTALL] Add edu.berkeley to the list of .m2 and
    
    .ivy2 cache cleanups.

[33mcommit f1987a84176d19c55dbd4ca622f9af237d1926e9[m
Merge: 608b1e3 d8a8711
Author: bhan <binhhan.gt@gmail.com>
Date:   Fri Feb 7 14:57:14 2014 -0800

    Merge pull request #15 from adatao/ctn-changes
    
    [INSTALL] Uncomment line that installs unmanaged jars

[33mcommit d8a8711554a88a3078730d147136b1a2cfea2757[m
Merge: 3034cc3 608b1e3
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Fri Feb 7 14:56:06 2014 -0800

    Merge branch 'master' into ctn-changes

[33mcommit 3034cc323c2bdd05efd5ede8276c91bdafc8f5aa[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Fri Feb 7 14:55:26 2014 -0800

    [INSTALL] Uncomment line that installs unmanaged jars

[33mcommit 608b1e3ac92e8582ca15185942a5a548bdef9f7b[m
Merge: d4f8948 3809a4c
Author: Christopher Nguyen <ctn@users.noreply.github.com>
Date:   Thu Feb 6 22:45:51 2014 -0800

    Merge pull request #14 from adatao/ckb-checkstyle
    
    Add checkstyle configuration files.

[33mcommit 3809a4ca8c0c3e0a884fb4f24c725cebd728f64e[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Thu Feb 6 23:16:37 2014 -0600

    Change the output folder to target.

[33mcommit 1b91731267eb9138b1d19eb41072c4d930e66d9d[m
Author: Cuong Kien Bui <ckb@adatao.com>
Date:   Thu Feb 6 23:09:21 2014 -0600

    Add scalastyle plugin for checking scala classes style.

[33mcommit d4f8948bf1417e439aa28dda782cb5540aaa51b7[m
Merge: 5e0a952 6d706d3
Author: bhan <binhhan.gt@gmail.com>
Date:   Thu Feb 6 12:57:41 2014 -0800

    Merge pull request #13 from adatao/ctn-changes
    
    [MISC] Minor compile error/warning fixes

[33mcommit 6d706d3b67b399ee437b10c4a0ce609e2ae78e91[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Wed Feb 5 23:58:14 2014 -0800

    [MISC] Minor compile error/warning fixes

[33mcommit 5e0a952b53b50df14bf518d6f48e07f3fa592a7b[m
Merge: 7e1d7f3 0e61024
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Feb 5 22:43:40 2014 -0800

    Merge pull request #12 from adatao/ctn-changes
    
    [REFACTOR]  Added spark/ and extras/ projects

[33mcommit 0e6102492d1cb66e53d025573b298d31d7f620da[m
Merge: 8700347 7e1d7f3
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Wed Feb 5 19:15:36 2014 -0800

    Merge branch 'master' into ctn-changes

[33mcommit 870034757f1b43ee54f58fa08161f7890498c5c4[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Wed Feb 5 18:55:19 2014 -0800

    [REFACTOR] Added spark/ and extras/ project

[33mcommit 7e1d7f30b223262f0ac862303ee0cfcc199d5ac1[m
Merge: 1663789 07543a9
Author: bhan <binhhan.gt@gmail.com>
Date:   Wed Feb 5 18:15:18 2014 -0800

    Merge pull request #11 from adatao/ctn-changes
    
    Move com.adatao.ddf.spark to its own project, spark/

[33mcommit 07543a94f4b9b0afff2500899196e9c202c3847f[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Wed Feb 5 15:30:13 2014 -0800

    [REFACTOR] Move Spark PersistenceHandler.scala to
    
    where it really belongs, under spark/

[33mcommit 017ef390155544c15be1b8daca162ee7f700df67[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Wed Feb 5 15:24:46 2014 -0800

    [FEATURE] Add spark/lib for custom jars, e.g.,
    
    patched Hive.

[33mcommit bf791ba79785c6684fa2289a944d4cddd17220fc[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Wed Feb 5 15:23:46 2014 -0800

    [REFACTOR] Starting to move com.adatao.ddf.spark
    
    to its own project under spark/. Also fix in RootBuild.scala for
    Shark dependency.

[33mcommit d470513bc32c85ebb7c3507f8737aa33c957e4e4[m
Author: Cuong Kien Bui <cuongbui@adatao.com>
Date:   Wed Feb 5 17:01:14 2014 -0600

    Add checkstyle configuration files.

[33mcommit 1663789786714f352138cbdc0110588bb1c67146[m
Merge: fea596d b9a19c4
Author: Christopher Nguyen <ctn@users.noreply.github.com>
Date:   Sat Jan 25 10:16:12 2014 -0800

    Merge pull request #8 from adatao/ctn-work
    
    [FEATURE] Differentiate between Representations and Views

[33mcommit b9a19c48d4147b59316d40d3539c048af2ae7c51[m
Merge: a482071 cca260b
Author: Christopher Nguyen <ctn@users.noreply.github.com>
Date:   Thu Jan 23 00:57:09 2014 -0800

    Merge pull request #9 from adatao/ddh-work
    
    fix RootBuild.scala to work with Spark-0.8.1 and Spark-0.8.1

[33mcommit a48207153c9434a8c8d59dd9ac6005d90e93982a[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Thu Jan 23 00:55:16 2014 -0800

    [FEATURE] Added support for etl/IHandlePersistence
    
    comprised of things like SQL load/save.

[33mcommit cca260b43eba2c5d51019a82ead0b07a4c3999a7[m
Author: huandao0812 <daoduchuan@gmail.com>
Date:   Thu Jan 23 11:41:41 2014 +0700

    add TablePartitionHelper.scala

[33mcommit 67de4633c777067532ca7c9d527df80d35fffd17[m
Author: huandao0812 <daoduchuan@gmail.com>
Date:   Thu Jan 23 01:57:54 2014 +0700

    add function to get view
    
    fix RootBuild.scala to work with Spark-0.8.1 and Spark-0.8.1

[33mcommit 062ddef1c25cac103e87378c6d98d79b095fb17c[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Wed Jan 22 02:11:37 2014 -0800

    [WIP] More proper support for ViewFormat enums

[33mcommit 0b03b6e60ac2de9731e61a1d1fa822583a821792[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Tue Jan 21 22:01:08 2014 -0800

    [WIP] Incremental design for IHandleMetadata,
    
    and implementation of AViewsHandler.{remove(),reset()}

[33mcommit 419e58d97b4c0795f6327be4a8d66577f8508dfd[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Tue Jan 21 17:39:19 2014 -0800

    [WIP] Implement support for Views persistence

[33mcommit 1a6cdcdd8c476b8177071e5d5dfe5b4143ca3ffd[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Mon Jan 20 23:53:11 2014 -0800

    [FEATURE] Differentiate between Representations and Views
    
    Preliminary design of the IHandleViews interface.

[33mcommit fea596d1b08b86a31fb1bf7c8fefc0dea58fabfe[m
Merge: 5c1482b 228de33
Author: bhan <binhhan.gt@gmail.com>
Date:   Mon Jan 20 22:50:59 2014 -0800

    Merge pull request #7 from adatao/ctn-work
    
    Comments & Support for curl in bin/get-sbt.sh in case wget is not available

[33mcommit 228de3375e72a584bb931e6b5cd084d6e4dd861f[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Mon Jan 20 15:10:16 2014 -0800

    [WIP] Make Helper lazily create Handlers as needed,
    
    instead of upon instantiations. This will help keep DDFs lightweight
    and make it inexpensive to create multiple views that are themselves DDFs.

[33mcommit 4a9e1d200596f79b4b323ac527f747451ecc646d[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Mon Jan 20 14:00:41 2014 -0800

    [BUGFIX] Fix typos introduced by automated refactoring

[33mcommit 43ea30680b11dd2fdc1bec788c261dded2fd1fcb[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Mon Jan 20 13:00:06 2014 -0800

    [FEATURE] Add support for PhantomReference tracking
    
    so we can easily track/debug memory leaks.

[33mcommit 1ef5b0c846cba0116920fb3d88e14fb4b69a0eed[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Mon Jan 20 12:56:39 2014 -0800

    [REFACTOR] Move interfaces/implementations into their
    
    respective groups, e.g, "etl", "content", "analytics",
    for easier mental tracking of the various interfaces.

[33mcommit 78db02ddd531ecc67037fff887db925ba07329c6[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Mon Jan 20 08:20:41 2014 -0800

    [TOOLS] Added git-* command utilities to bin for
    
    developer convenience. For example, use
    
        % git ub # or git update-branches
    
    to get all your local branches updated, without having to switch
    to one branch at a time.

[33mcommit f6009782193e7638d955bf13ea231b2da4027c51[m
Merge: 8ae5c5a 5c1482b
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Mon Jan 20 05:49:39 2014 -0800

    Merge branch 'master' into ctn-work
    
    Conflicts:
    	bin/get-sbt.sh
    
    Resolved.

[33mcommit 8ae5c5a92c2c6906c06190034f85fd0b24c969d7[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Mon Jan 20 05:43:17 2014 -0800

    [BUGFIX] Support curl in case wget is not available

[33mcommit 5c1482b92981ab16d6db7fd2298a9af450c85786[m
Author: Christopher Nguyen <ctn@users.noreply.github.com>
Date:   Mon Jan 20 05:33:52 2014 -0800

    Update README.md

[33mcommit a6a86742869b83761c566e00da232da38b0dee6c[m
Author: Christopher Nguyen <ctn@users.noreply.github.com>
Date:   Mon Jan 20 05:33:30 2014 -0800

    Update README.md

[33mcommit 9678e2e1768c6a98610b2a25df0c2a0b919eb9e6[m
Author: bhan <binhhan.gt@gmail.com>
Date:   Sun Jan 19 21:20:19 2014 -0800

    Fix errors when wget or curl is not installed

[33mcommit 0a5cb9246297841bc2ced985cc297fba2259b6bd[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sun Jan 19 16:37:00 2014 -0800

    [WIP] Clarifying comments

[33mcommit a2cfa10a07210cdc9b206cbbcea7c10b567dae4a[m
Merge: cc57e99 3f4620a
Author: bhan <binhhan.gt@gmail.com>
Date:   Sun Jan 19 16:34:49 2014 -0800

    Merge pull request #5 from adatao/ctn-work
    
    [FEATURE] Refactor to ADataFrameHandler abstract class,

[33mcommit 3f4620a5362f5fe313a053c41a144a542b1c3236[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sun Jan 19 16:28:23 2014 -0800

    [WIP] Added tests for Scala and Java RepresentationHandlers

[33mcommit 02364d42415aebe3363387e716fb86287b4c1b5c[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sun Jan 19 14:35:27 2014 -0800

    [REFACTOR] Use "Helper" instead of "Implementor".
    
    Simpler and easier to discern.

[33mcommit 82baeef9f9b16b95dd177c18df48c74116e88c50[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sun Jan 19 08:58:32 2014 -0800

    [REFACTOR] Ripple through use of "DDF" over "DataFrame"

[33mcommit c6efb29bad9f3bdfbcf1e5ce9bdf77899baeb52c[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sun Jan 19 08:45:38 2014 -0800

    [REFACTOR] Use DDF instead of DataFrame

[33mcommit dfec267f10f4ba9a682b953bab181de42028e4c1[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sun Jan 19 08:01:55 2014 -0800

    [WIP] Add abstract base implementations, tests
    
    and enable junit in project/RootBuild.scala

[33mcommit 1f1c794eef1778ce41a5b5b7dcd3cebb1aa00383[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sat Jan 18 17:34:16 2014 -0800

    [COMMENT] Added explanatory comment on philosophy
    
    of ADataFrameImplementor.

[33mcommit 0429b34f661a6bda3ad322d7939a07f09eafc36f[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sat Jan 18 17:23:56 2014 -0800

    [FEATURE] Refactor to ADataFrameHandler abstract class,
    
    which should be extended by specific implementors.
    Also initial example implementation of a particular interface,
    IHandleRepresentations.

[33mcommit cc57e994e3983958c8ce5ce927384e7840de5c93[m
Merge: eb40d4a 93105f3
Author: bhan <binhhan.gt@gmail.com>
Date:   Sat Jan 18 13:18:12 2014 -0800

    Merge pull request #4 from adatao/ctn-work
    
    [FEATURE] Implement Dependency Injection, Delegation, and Composite patterns

[33mcommit 93105f3c67f38feab9e2aa68921be4ff91c928d0[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Sat Jan 18 10:28:35 2014 -0800

    [FEATURE] Implement Dependency Injection, Delegation, and Composite patterns
    
    by introducing DataFrame handling interfaces

[33mcommit eb40d4a392384e76cb0c804b06801845f8482614[m
Merge: a5f9ba3 8956f8f
Author: Christopher Nguyen <ctn@users.noreply.github.com>
Date:   Fri Jan 17 18:43:06 2014 -0800

    Merge pull request #3 from adatao/ctn-work
    
    [FEATURE] Add Spark Dependency

[33mcommit 8956f8f100f1a24c27a8bc09a748abafb0a38be6[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Fri Jan 17 18:40:30 2014 -0800

    [FEATURE] Add Spark Dependency

[33mcommit a5f9ba3516948eb5a7eb25a5a57ba24f9728dec0[m
Merge: 4fcb8c8 29ed7aa
Author: Christopher Nguyen <ctn@users.noreply.github.com>
Date:   Fri Jan 17 18:31:40 2014 -0800

    Merge pull request #2 from adatao/ctn-work
    
    [REORG] Major reorg to accommodate Scala-Java mixed project

[33mcommit 29ed7aa2017f2e3a975f6d03c7d42bae76fdb1eb[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Fri Jan 17 17:48:09 2014 -0800

    [TWEAK] Use underscore `_` instead of hyphen `-` in project name

[33mcommit fc5900104a72184c87d9e1032cede978ee3044ef[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Fri Jan 17 17:33:24 2014 -0800

    [REORG] Major reorg to accommodate Scala-Java mixed project

[33mcommit 4fcb8c8955d8af47ae4f60deee9f517c587a82b3[m
Merge: 289f5c8 2bd939e
Author: bhan <binhhan.gt@gmail.com>
Date:   Fri Jan 17 15:53:17 2014 -0800

    Merge pull request #1 from adatao/ctn-work
    
    [CREATED] Initial commit for Adatao DDF!

[33mcommit 2bd939ef2fd303fda075235ab4c41d46fe146d5f[m
Author: Christopher Nguyen <ctn@adatao.com>
Date:   Fri Jan 17 15:51:56 2014 -0800

    [CREATED] Initial commit for Adatao DDF!

[33mcommit 289f5c8fb61758919d7e8d36ddc3404754ab4f89[m
Author: Christopher Nguyen <ctn@users.noreply.github.com>
Date:   Fri Jan 17 14:30:40 2014 -0800

    Initial commit
