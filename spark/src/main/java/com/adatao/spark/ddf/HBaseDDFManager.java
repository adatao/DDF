package com.adatao.spark.ddf;


import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import shark.SharkContext;
import shark.SharkEnv;
import shark.api.JavaSharkContext;
import com.adatao.ddf.DDF;
import com.adatao.ddf.content.Schema;
import com.adatao.ddf.exception.DDFException;

/**
 * An Apache-Spark-based implementation of DDFManager
 */
public class HBaseDDFManager extends SparkDDFManager {

  @Override
  public String getEngine() {
    return "hbase";
  }


  private static final String DEFAULT_SPARK_APPNAME = "DDFClient";
  private static final String DEFAULT_SPARK_MASTER = "local[4]";


  public HBaseDDFManager(SparkContext sparkContext) throws DDFException {
    this.initialize(sparkContext, null);
  }

  /**
   * Use system environment variables to configure the SparkContext creation.
   * 
   * @throws DDFException
   */
  public HBaseDDFManager() throws DDFException {
    this.initialize(null, new HashMap<String, String>());
  }

  public HBaseDDFManager(Map<String, String> params) throws DDFException {
    this.initialize(null, params);
  }

  private void initialize(SparkContext sparkContext, Map<String, String> params) throws DDFException {
    this.setSparkContext(sparkContext == null ? this.createSparkContext(params) : sparkContext);

    // it will never go here
    // TODO remove later
    if (sparkContext instanceof SharkContext) {
      this.setSharkContext((SharkContext) sparkContext);
      sparkContext.conf().set("spark.kryo.registrator", "com.adatao.spark.content.KryoRegistrator");
      mSharkContext.conf().set("spark.kryo.registrator", "com.adatao.spark.content.KryoRegistrator");
      mSparkContext.conf().set("spark.kryo.registrator", "com.adatao.spark.content.KryoRegistrator");

      mLog.info(">>>>>>>>>>>>> setting Kryo for mSharkContext: " + mSharkContext.conf().get("spark.kryo.registrator"));
    }
  }

  public DDF loadTable(String tableName, String seperator) {
    //do nothingGetSummaryMapper extends Function<Object[], Summary[]>
    return null;
  }

  public DDF loadTable(String tableName, List<String> columns) throws DDFException {
    // JavaRDD<String> fileRDD = mJavaSharkContext.textFile(fileURL);
    Configuration config = HBaseConfiguration.create();
    // config.set("hbase.zookeeper.znode.parent", "hostname1");
    // config.set("hbase.zookeeper.quorum","hostname1");
    // config.set("hbase.zookeeper.property.clientPort","2181");
    config.set("hbase.master", "localhost:");
    // config.set("fs.defaultFS","hdfs://hostname1/");
    // config.set("dfs.namenode.rpc-address","localhost:8020");

    config.set(TableInputFormat.INPUT_TABLE, tableName);

    
    String columnsNameType = "";
    
    // init scan object
    Scan scan = new Scan();
    Iterator it = columns.iterator();
    String c = "";
    String cf = "";
    String cq = "";
    while (it.hasNext()) {
      c = (String) it.next();
      if (c.contains(":")) {
        cf = c.split(":")[0];
        cq = c.split(":")[1];
        scan.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cq));
        
        //convert to Shark table
        columnsNameType += cf + "_" + cq + " string" + ",";
      } else {
        cf = c;
        scan.addFamily(Bytes.toBytes(cf));
        columnsNameType += " " + cf + " string" + ",";
      }
      
      
    }
    // set SCAN in conf
    try {
      config.set(TableInputFormat.SCAN, convertScanToString(scan));
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    RDD<Tuple2<ImmutableBytesWritable, Result>> hBaseRDD = getSparkContext().newAPIHadoopRDD(config,
        TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
    //convert to RDD[Row] or RDD[Array[Double]]
    RDD<double[]> result = convertRDD(hBaseRDD);
    long a = result.count();
    System.out.println(">>>> count table = " + a);
    
    
    Schema schema = new Schema(tableName, "mpg string, cyl string"); 
    DDF ddf =  new SparkDDF(this, result, double[].class, null, tableName, schema);
//    ddf.getRepresentationHandler().get(RDD.class, Double[].class);
    return ddf;
  }
  
  public RDD<double[]> convertRDD(RDD<Tuple2<ImmutableBytesWritable, Result>> hbaseRDD) {
    
    RDD<double[]> result = hbaseRDD.map(new ConvertMapper(), null);
    
    return result;
  }
  
//  GetSummaryMapper extends Function<Object[], Summary[]>
  
  private static class ConvertMapper extends Function <Tuple2<ImmutableBytesWritable, Result>, double[]> {
    public ConvertMapper() {
      System.out.println(">>>>>>>>>>>>>> ConvertMapper");
    }
    @Override
    public double[] call(Tuple2<ImmutableBytesWritable, Result> input) throws Exception {
      ImmutableBytesWritable key = input._1;
      Result res = input._2;
      List<org.apache.hadoop.hbase.KeyValue> keyValues = res.list();
      int size = keyValues.size();
      double[] arrResult = new double[size];
      int i = 0;
      Iterator<KeyValue> it = keyValues.iterator();
      while(it.hasNext()) {
        KeyValue kv = it.next();
        System.out.println(">>> family=" + Bytes.toString(kv.getFamily()));
        System.out.println(">>> qualifier=" + Bytes.toString(kv.getQualifier()));
        double a = Double.parseDouble(Bytes.toString(kv.getValue()));
        arrResult[i] = a;
        System.out.println(">>>>>>a=" + a);
        
        i++;
      }
      return arrResult;
    }
  }


  static String convertScanToString(Scan scan) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(out);
    scan.write(dos);
    return Base64.encodeBytes(out.toByteArray());
  }


  public String getDDFEngine() {
    return "hbase";
  }


  private SparkContext mSparkContext;


  public SparkContext getSparkContext() {
    return mSparkContext;
  }

  private void setSparkContext(SparkContext sparkContext) {
    this.mSparkContext = sparkContext;
  }


  private SharkContext mSharkContext;


  public SharkContext getSharkContext() {
    return mSharkContext;
  }


  private JavaSharkContext mJavaSharkContext;


  public JavaSharkContext getJavaSharkContext() {
    return mJavaSharkContext;
  }

  public void setJavaSharkContext(JavaSharkContext javaSharkContext) {
    this.mJavaSharkContext = javaSharkContext;
  }

  /**
   * Also calls setSparkContext() to the same sharkContext
   * 
   * @param sharkContext
   */
  private void setSharkContext(SharkContext sharkContext) {
    this.mSharkContext = sharkContext;
    this.setSparkContext(sharkContext);
  }


  private Map<String, String> mSparkContextParams;


  public Map<String, String> getSparkContextParams() {
    return mSparkContextParams;
  }

  private void setSparkContextParams(Map<String, String> mSparkContextParams) {
    this.mSparkContextParams = mSparkContextParams;
  }



  public void shutdown() {
    if (this.getSharkContext() != null) {
      this.getSharkContext().stop();
    } else if (this.getSparkContext() != null) {
      this.getSparkContext().stop();
    }
  }


  private static final String[][] SPARK_ENV_VARS = new String[][] { 
    // @formatter:off
    { "SPARK_APPNAME", "spark.appname" },
    { "SPARK_MASTER", "spark.master" }, 
    { "SPARK_HOME", "spark.home" }, 
    { "SPARK_SERIALIZER", "spark.serializer" },
    { "SPARK_SERIALIZER", "spark.serializer" },
    { "HIVE_HOME", "hive.home" },
    { "HADOOP_HOME", "hadoop.home" },
    { "DDFSPARK_JAR", "ddfspark.jar" } 
    // @formatter:on
  };


  /**
   * Takes an existing params map, and reads both environment as well as system property settings to merge into it. The
   * merge priority is as follows: (1) already set in params, (2) in system properties (e.g., -Dspark.home=xxx), (3) in
   * environment variables (e.g., export SPARK_HOME=xxx)
   * 
   * @param params
   * @return
   */
  private Map<String, String> mergeSparkParamsFromSettings(Map<String, String> params) {
    if (params == null) params = new HashMap<String, String>();

    Map<String, String> env = System.getenv();

    for (String[] varPair : SPARK_ENV_VARS) {
      if (params.containsKey(varPair[0])) continue; // already set in params

      // Read setting either from System Properties, or environment variable.
      // Env variable has lower priority if both are set.
      String value = System.getProperty(varPair[1], env.get(varPair[0]));
      if (value != null && value.length() > 0) params.put(varPair[0], value);
    }

    // Some well-known defaults
    if (!params.containsKey("SPARK_MASTER")) params.put("SPARK_MASTER", DEFAULT_SPARK_MASTER);
    if (!params.containsKey("SPARK_APPNAME")) params.put("SPARK_APPNAME", DEFAULT_SPARK_APPNAME);

    return params;
  }

  /**
   * Side effect: also sets SharkContext and SparkContextParams in case the client wants to examine or use those.
   * 
   * @param params
   * @return
   * @throws DDFException
   */
  private SparkContext createSparkContext(Map<String, String> params) throws DDFException {
    this.setSparkContextParams(this.mergeSparkParamsFromSettings(params));
    String ddfSparkJar = params.get("DDFSPARK_JAR");
    String[] jobJars = ddfSparkJar != null ? ddfSparkJar.split(",") : new String[] {};

    mJavaSharkContext = new JavaSharkContext(params.get("SPARK_MASTER"), params.get("SPARK_APPNAME"),
        params.get("SPARK_HOME"), jobJars, params);
    this.setSharkContext(SharkEnv.initWithJavaSharkContext(mJavaSharkContext).sharkCtx());

    // set up serialization from System property
    mSharkContext.conf().set("spark.kryo.registrator",
        System.getProperty("spark.kryo.registrator", "com.adatao.spark.content.KryoRegistrator"));
    mSparkContext.conf().set("spark.kryo.registrator",
        System.getProperty("spark.kryo.registrator", "com.adatao.spark.content.KryoRegistrator"));

    return this.getSparkContext();
  }


}
