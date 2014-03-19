package com.adatao.spark.ddf;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import com.adatao.ddf.DDF;
import com.adatao.ddf.DDFManager;
import com.adatao.ddf.content.Schema;
import com.adatao.ddf.exception.DDFException;
import scala.reflect.ClassManifest$;


/**
 * An Apache-Spark-based implementation of DDF
 */

public class SparkDDF extends DDF {

  private static final long serialVersionUID = 7466377156065874568L;


  public <T> SparkDDF(DDFManager manager, RDD<T> rdd, Class<T> unitType, String namespace, String name, Schema schema)
      throws DDFException {

    super(manager);
    if (rdd == null) throw new DDFException("Non-null RDD is required to instantiate a new SparkDDF");
    this.initialize(manager, rdd, new Class<?>[] { RDD.class, unitType }, namespace, name, schema);
  }

  /**
   * Signature without RDD, useful for creating a dummy DDF used by DDFManager
   * 
   * @param manager
   */
  public SparkDDF(DDFManager manager) throws DDFException {
    super(manager);
  }

  /**
   * Available for run-time instantiation only.
   * 
   * @throws DDFException
   */
  protected SparkDDF() throws DDFException {
    super();
  }

  @SuppressWarnings("unchecked")
  public <T> RDD<T> getRDD(Class<T> unitType) throws DDFException {
    Object obj = this.getRepresentationHandler().get(RDD.class, unitType);
    if (obj instanceof RDD<?>) return (RDD<T>) obj;
    else throw new DDFException("Unable to get RDD with unit type " + unitType);
  }

  public <T> JavaRDD<T> getJavaRDD(Class<T> unitType) throws DDFException {
    RDD<T> rdd = this.getRDD(unitType);
    return new JavaRDD<T>(rdd, ClassManifest$.MODULE$.fromClass(unitType));
  }
}
