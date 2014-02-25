/**
 * Copyright 2014 Adatao, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *    
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */
package com.adatao.ddf;

import java.lang.reflect.Constructor;
import java.util.List;

import com.adatao.ddf.analytics.IRunAlgorithms;
import com.adatao.ddf.content.IHandleRepresentations;
import com.adatao.ddf.content.Schema;
import com.adatao.ddf.content.Schema.DataFormat;
import com.adatao.ddf.etl.IHandleSql;
import com.adatao.ddf.etl.IHandleSqlLike;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.util.ConfigHandler;
import com.adatao.ddf.util.IHandleConfig;
import com.adatao.ddf.util.ISupportPhantomReference;
import com.adatao.ddf.util.PhantomReference;
import com.adatao.jcoll.ddf.JCollDDFManager;
import com.google.common.base.Strings;

/**
 * <p>
 * Abstract base class for a {@link DDF} implementor, which provides the support methods necessary
 * to implement various DDF interfaces, such as {@link IHandleRepresentations} and
 * {@link IRunAlgorithms}.
 * </p>
 * <p>
 * We use the Dependency Injection, Delegation, and Composite patterns to make it easy for others to
 * provide alternative (even snap-in replacements), support/implementation for DDF. The class
 * diagram is as follows:
 * </p>
 * 
 * <pre>
 * ------------------   -------------------------
 * |     DDFManager |-->|         DDF           |
 * ------------------   -------------------------
 *                         ^          ^
 *                         |   ...    |        -------------------
 *                         |          |------->| IHandleMetadata |
 *                         |                   -------------------
 *                         |
 *                         |        ----------------------------------
 *                         |------->| IHandleRepresentations |
 *                                  ----------------------------------
 * </pre>
 * <p>
 * An implementor need not provide all or even most of these interfaces. Each interface handler can
 * be get/set separately, as long as they cooperate properly on things like the underlying
 * representation. This makes it easy to roll out additional interfaces and their implementations
 * over time.
 * </p>
 * DDFManager implements {@link IHandleSqlLike} because we want to expose those methods as directly
 * to the API user as possible, in an engine-dependent manner.
 * 
 * @author ctn
 * 
 */
public abstract class DDFManager extends ALoggable implements IDDFManager, IHandleSqlLike, ISupportPhantomReference {

  public DDFManager() {
    this.initialize();
  }

  private void initialize() {
    PhantomReference.register(this);
  }


  static DDFManager sDummyDDFManager = new JCollDDFManager();

  /**
   * We need a dummy DDFManager in order to support some engine-independent global functions, like
   * get(engineName). For this purpose, we'll just use the built-in {@link JCollDDFManager}.
   * 
   * @return
   */
  static DDFManager getDummyDDFManager() {
    return sDummyDDFManager;
  }

  /**
   * Returns a new instance of {@link DDFManager} for the given engine name
   * 
   * @param engineName
   * @return
   * @throws Exception
   */
  public static DDFManager get(String engineName) throws DDFException {
    if (Strings.isNullOrEmpty(engineName)) return null;

    String className = getDummyDDFManager().getConfigValue(engineName, "DDFManager");
    if (Strings.isNullOrEmpty(className)) return null;

    DDFManager manager;
    try {
      manager = (DDFManager) Class.forName(className).newInstance();

    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      throw new DDFException("Cannot get DDFManager for engine " + engineName, e);
    }
    return manager;
  }

  // public static final String DEFAULT_DDF_ENGINE = "spark";

  /**
   * Returns the DDF engine name of a particular implementation, e.g., "spark".
   * 
   * @return
   */
  public abstract String getEngine();

  private DDF mDummyDDF;

  /**
   * Each {@link DDFManager} implementation must provide a "dummy" or helper DDF that we will use to
   * access that implementation's handlers, such as {@link IHandleSql} or {@link IHandleConfig}.
   * 
   * @return
   */
  protected abstract DDF createDummyDDF() throws DDFException;

  protected DDF getDummyDDF() throws DDFException {
    if (mDummyDDF == null) mDummyDDF = this.createDummyDDF();
    return mDummyDDF;
  }

  /**
   * Instantiates a new DDF of the type specified in ddf.ini as "DDF".
   * 
   * @return the newly instantiated DDF
   * 
   * @throws DDFException
   */
  // protected DDF newDDF() throws DDFException {
  @SuppressWarnings("unchecked")
  protected DDF newDDF(DDFManager manager, Object data, Class<?> elementType, String namespace, String name,
      Schema schema) throws DDFException {

    String className = this.getConfigValue("DDF");

    try {
      Constructor<DDF> cons = (Constructor<DDF>) Class.forName(className).getConstructor(DDFManager.class,
          Object.class, Class.class, String.class, String.class, String.class);
      if (cons == null) throw new DDFException("Cannot get constructor for " + className);

      DDF ddf = cons.newInstance(manager, data, elementType, namespace, name, schema);
      if (ddf == null) throw new DDFException("Cannot instantiate a new instance of " + className);

      return ddf;

    } catch (Exception e) {
      throw new DDFException("While instantiating a new DDF", e);
    }
  }


  private String mNamespace;

  public String getNamespace() throws DDFException {
    if (Strings.isNullOrEmpty(mNamespace)) mNamespace = this.getConfigValue("namespace");
    if (Strings.isNullOrEmpty(mNamespace)) mNamespace = this.getConfigValue("global", "namespace");
    return mNamespace;
  }

  public void setNameSpace(String namespace) {
    mNamespace = namespace;
  }

  // ////// ISupportPhantomReference ////////

  public void cleanup() {
    // Do nothing in the base
  }

  // ////// IDDFManager ////////
  public void shutdown() {
    // Do nothing in the base
  }



  // ////// IHandleConfiguration support ////////

  protected IHandleConfig mConfigHandler;

  /**
   * The base implementation here will first look at the environment variable DDF_CONFIG_HANDLER to
   * see if a class name is specified. If yes, that class is used. Otherwise it will instantiate the
   * standard {@link ConfigHandler} and save as our ConfigHandler.
   * 
   * @return
   */
  public IHandleConfig getConfigHandler() throws DDFException {
    if (mConfigHandler == null) {

      try {
        String configHandlerClass = System.getenv("DDF_CONFIG_HANDLER");
        if (configHandlerClass != null) {
          mConfigHandler = (IHandleConfig) Class.forName(configHandlerClass).newInstance();
        }

        if (mConfigHandler == null) mConfigHandler = new ConfigHandler();

        if (mConfigHandler != null) mConfigHandler.loadConfig();

      } catch (Exception e) {
        throw new DDFException(e);
      }
    }

    return mConfigHandler;
  }

  /**
   * Convenience method to get a config value from DDF.
   * 
   * @param key
   * @return
   * @throws Exception
   */
  protected String getConfigValue(String key) throws DDFException {
    return this.getConfigValue(this.getEngine(), key);
  }

  protected String getConfigValue(String section, String key) throws DDFException {
    return this.getConfigHandler().getValue(section, key);
  }



  // ////// IHandleSql facade methods ////////

  @Override
  public DDF sql2ddf(String command) throws DDFException {
    return this.sql2ddf(command, null, null, null);
  }

  @Override
  public DDF sql2ddf(String command, Schema schema) throws DDFException {
    return this.sql2ddf(command, schema, null, null);
  }

  @Override
  public DDF sql2ddf(String command, DataFormat dataFormat) throws DDFException {
    return this.sql2ddf(command, null, null, dataFormat);
  }

  @Override
  public DDF sql2ddf(String command, Schema schema, String dataSource) throws DDFException {
    return this.sql2ddf(command, schema, dataSource, null);
  }

  @Override
  public DDF sql2ddf(String command, Schema schema, DataFormat dataFormat) throws DDFException {
    return this.sql2ddf(command, schema, null, dataFormat);
  }

  @Override
  public DDF sql2ddf(String command, Schema schema, String dataSource, DataFormat dataFormat) throws DDFException {
    return this.getDummyDDF().getSqlHandler().sql2ddf(command, schema, dataSource, dataFormat);
  }

  @Override
  public List<String> sql2txt(String command) throws DDFException {
    return this.sql2txt(command, null);
  }

  @Override
  public List<String> sql2txt(String command, String dataSource) throws DDFException {
    return this.getDummyDDF().getSqlHandler().sql2txt(command, dataSource);
  }
}
