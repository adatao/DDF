package com.adatao.ddf.analytics;


import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;
import scala.actors.threadpool.Arrays;
import com.adatao.ddf.DDF;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.misc.ADDFFunctionalGroupHandler;
import com.adatao.ddf.misc.Config;
import com.adatao.ddf.util.Utils.ClassMethod;
import com.adatao.ddf.util.Utils.MethodInfo;
import com.adatao.ddf.util.Utils.MethodInfo.ParamInfo;
import com.adatao.local.ddf.content.PersistenceHandler.LocalPersistible;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.gson.annotations.Expose;

/**
 */
public class MLSupporter extends ADDFFunctionalGroupHandler implements ISupportML {

  private Boolean sIsNonceInitialized = false;


  /**
   * For ML reflection test-case only
   */
  @Deprecated
  public MLSupporter() {
    super(null);
  }

  public MLSupporter(DDF theDDF) {
    super(theDDF);
    this.initialize();
  }

  private void initialize() {
    if (sIsNonceInitialized) return;

    synchronized (sIsNonceInitialized) {
      if (sIsNonceInitialized) return;
      sIsNonceInitialized = true;

      this.initializeConfiguration();
    }
  }

  /**
   * Optional: put in any hard-coded mapping configuration here
   */
  private void initializeConfiguration() {
    // if (Strings.isNullOrEmpty(Config.getValue(ConfigConstant.ENGINE_NAME_LOCAL.toString(), "kmeans"))) {
    // Config.set(ConfigConstant.ENGINE_NAME_LOCAL.toString(), "kmeans",
    // String.format("%s#%s", MLSupporter.class.getName(), "dummyKMeans"));
    // }
  }



  /**
   * 
   */
  public static abstract class AAlgorithm implements IAlgorithm {
    private IHyperParameters mHyperParameters;
    private Class<?> mInputClass;


    public AAlgorithm(Class<?> inputClass, IHyperParameters parameters) {
      this.setInputClass(inputClass);
      this.setHyperParameters(parameters);
    }

    @Override
    public IHyperParameters getHyperParameters() {
      return mHyperParameters;
    }

    @Override
    public void setHyperParameters(IHyperParameters parameters) {
      mHyperParameters = parameters;
    }

    @Override
    public Class<?> getInputClass() {
      return mInputClass;
    }

    @Override
    public void setInputClass(Class<?> inputClass) {
      mInputClass = inputClass;
    }

    @Override
    public Object prepare(Object data) {
      return data;
    }

    @Override
    public IModel run(Object data) {
      return null;
    }
  }



  /**
   * 
   */
  public abstract static class Model extends LocalPersistible implements IModel {

    private static final long serialVersionUID = 824936593281899283L;

    @Expose Object mTrainingResult;


    public Model(Object trainingResult) {
      mTrainingResult = trainingResult;
    }

    /**
     * TODO: We can't serialize this directly because we can't deserialize an IModelParameters later, at least not
     * without some concrete class constructor.
     */

    private Class<?> mpredictionInputClass;

    private List<String> mFeatureColumnNames;

    public List<String> getFeatureColumnNames() {
      return new ArrayList<String>(this.mFeatureColumnNames);
    }

    public void setFeatureColumnNames(List<String> featureColumnNames) {
      this.mFeatureColumnNames = featureColumnNames;
    }

    public Class<?> getPredictionInputClass() {
      return this.mpredictionInputClass;
    }

    public void setPredictionInputClass(Class<?> predictionInputClass) {
      this.mpredictionInputClass = predictionInputClass;
    }

    public Model(Class<?> predictionInputClass) {
      mpredictionInputClass = predictionInputClass;
    }

    public Model(List<String> featureColumnNames, Class<?> predictionInputClass) {
      this.setFeatureColumnNames(featureColumnNames);
      this.setPredictionInputClass(predictionInputClass);
    }

    protected Object prepareData(DDF ddf) throws DDFException{
      return ddf.getRepresentationHandler().get(this.getPredictionInputClass());
    }
    public abstract DDF predict(Object data, DDF ddf);

    @Override
    public DDF predict(DDF ddf) throws DDFException{
      List<String> ddfColumnNames = ddf.getColumnNames();
      Object data;

      //if featureColumnNames is not the same as ddf's columnNames
      //throw Exception
      if((ddfColumnNames.size() == this.getFeatureColumnNames().size()) &&
          (ddfColumnNames.containsAll(this.getFeatureColumnNames()))){
        data = this.prepareData(ddf);
      } else{
        String cols = Joiner.on(", ").join(ddfColumnNames);
        throw new DDFException(String.format("Please project the ddf to the same columns " +
            "as feature columns that used to train this model: %s.", cols));
      }
      return this.predict(data, ddf);
    }

    protected abstract double predictImpl(double[] point);

    @Override
    public double predict(double[] point) throws DDFException {
      if(this.getFeatureColumnNames().size() != point.length){
        throw new DDFException("Point's size must be the same size as number features used to train the model");
      } else{
        return this.predictImpl(point);
      }
    }

    @Override
    public double[] predict(double[][] points) throws DDFException {
      double[] predictions = new double[points.length];
      for(int i = 0; i < points.length; i++) {
        predictions[i] = this.predict(points[i]);
      }
      return predictions;
    }
  }



  // //// ISupportML //////

  public static final String DEFAULT_TRAIN_METHOD_NAME = "train";


  /**
   * Runs a training algorithm on the entire DDF dataset.
   * 
   * @param trainMethodName
   * @param args
   * @return
   * @throws DDFException
   */
  public IModel train(String trainMethodName, Object... paramArgs) throws DDFException {
    /**
     * Example signatures we must support:
     * <p/>
     * Unsupervised Training
     * <p/>
     * <code>
     * Kmeans.train(data: RDD[Array[Double]], k: Int, maxIterations: Int, runs: Int, initializationMode: String)
     * </code>
     * <p/>
     * Supervised Training
     * <p/>
     * <code>
     * LogisticRegressionWithSGD.train(input: RDD[LabeledPoint], numIterations: Int, stepSize: Double, miniBatchFraction:
     * Double, initialWeights: Array[Double])
     * 
     * SVM.train(input: RDD[LabeledPoint], numIterations: Int, stepSize: Double, regParam: Double, miniBatchFraction:
     * Double)
     * </code>
     */
    // Build the argument type array
    if (paramArgs == null) paramArgs = new Object[0];
    // Class<?>[] argTypes = new Class<?>[paramArgs.length];
    // for (int i = 0; i < paramArgs.length; i++) {
    // argTypes[i] = (paramArgs[i] == null ? null : paramArgs[i].getClass());
    // }
    String originalTrainMethodName = trainMethodName;
    // Locate the training method
    String mappedName = Config.getValueWithGlobalDefault(this.getEngine(), trainMethodName);
    if (!Strings.isNullOrEmpty(mappedName)) trainMethodName = mappedName;

    MLClassMethod trainMethod = new MLClassMethod(trainMethodName, DEFAULT_TRAIN_METHOD_NAME, paramArgs);
    if (trainMethod.getMethod() == null) {
      throw new DDFException(String.format("Cannot locate method specified by %s", trainMethodName));
    }
    // Now we need to map the DDF and its column specs to the input format expected by the method we're invoking
    Object[] allArgs = this.buildArgsForMethod(trainMethod.getMethod(), paramArgs);

    // Invoke the training method
    Object result = trainMethod.classInvoke(allArgs);

    // Construct the result model
    try{
      String modelClassName = Config.getValueWithGlobalDefault(this.getEngine(), String.format("%s_model", originalTrainMethodName));

      Class<?> modelClass = Class.forName(modelClassName);
      Constructor<?> cons = modelClass.getConstructor(result.getClass());
      IModel model = (IModel) cons.newInstance(result);

      List<String> featureColumns;
      List<String> ddfColumns = this.getDDF().getColumnNames();

      if(model.isSupervisedAlgorithmModel()){
        featureColumns = ddfColumns.subList(0, ddfColumns.size() - 1);
      } else{
        featureColumns = ddfColumns;
      }

      model.setFeatureColumnNames(featureColumns);
      return model;
    } catch(Exception e){
      throw new DDFException(e);
    }
  }


  @SuppressWarnings("unchecked")
  private Object[] buildArgsForMethod(Method method, Object[] paramArgs) throws DDFException{
    MethodInfo methodInfo = new MethodInfo(method);
    List<ParamInfo> paramInfos = methodInfo.getParamInfos();
    if (paramInfos == null || paramInfos.size() == 0) return new Object[0];

    Object firstParam = this.convertDDF(paramInfos.get(0));

    if (paramArgs == null || paramArgs.length == 0) {
      return new Object[] { firstParam };

    } else {
      List<Object> result = Lists.newArrayList();
      result.add(firstParam);
      result.addAll(Arrays.asList(paramArgs));
      return result.toArray(new Object[0]);
    }
  }

  /**
   * Override this to return the approriate DDF representation matching that specified in {@link ParamInfo}. The base
   * implementation simply returns the DDF.
   * 
   * @param paramInfo
   * @return
   */
  protected Object convertDDF(ParamInfo paramInfo) throws DDFException{
    return this.getDDF();
  }


  /**
   * 
   */
  private class MLClassMethod extends ClassMethod {

    public MLClassMethod(String classHashMethodName, String defaultMethodName, Object[] args) throws DDFException {
      super(classHashMethodName, defaultMethodName, args);
    }

    /**
     * Override to search for methods that may contain initial arguments that precede our argTypes. These initial
     * arguments might be feature/target column specifications in a train() method. Thus we do the argType matching from
     * the end back to the beginning.
     */
    @Override
    protected void findAndSetMethod(Class<?> theClass, String methodName, Class<?>... argTypes)
        throws NoSuchMethodException, SecurityException {

      if (argTypes == null) argTypes = new Class<?>[0];

      Method foundMethod = null;

      // Scan all methods
      for (Method method : theClass.getDeclaredMethods()) {
        if (!methodName.equalsIgnoreCase(method.getName())) continue;


        // Scan all method arg types, starting from the end, and see if they match with our supplied arg types
        Class<?>[] methodArgTypes = method.getParameterTypes();

        // Check that the number of args are correct:
        // the # of args in the method must be = 1 + the # of args supplied
        // ASSUMING that one of the args in the method is for input data

        if((methodArgTypes.length - 1) == argTypes.length){
          foundMethod = method;
          break;
        }
        //foundMethod = method;
        //break;

        // @formatter:off
        // NB: we don't do this for now because the arg types are hard to match properly, e.g., int or null.
        // Now check that the arg types match
//        boolean allMatched = true;
//        for (int a = argTypes.length - 1, m = methodArgTypes.length - 1; a >= 0; a--, m--) {
//
//          if ((argTypes[a] != null && !methodArgTypes[m].isAssignableFrom(argTypes[a])) //
//              || //
//              (argTypes[a] == null && !methodArgTypes[m].isAssignableFrom(Object.class)) //
//          ) {
//            allMatched = false;
//            break;
//          }
//
//        }
//
//        if (allMatched) {
//          foundMethod = method;
//          break;
//        }
        // @formatter:on
      }

      if (foundMethod != null) {
        foundMethod.setAccessible(true);
        this.setMethod(foundMethod);
      }
    }
  }
}
