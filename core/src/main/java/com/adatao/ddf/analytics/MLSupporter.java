package com.adatao.ddf.analytics;


import java.lang.reflect.Method;
import com.adatao.ddf.DDF;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.misc.ADDFFunctionalGroupHandler;
import com.adatao.ddf.misc.Config;
import com.adatao.ddf.misc.Config.ConfigConstant;
import com.adatao.ddf.util.Utils.ClassMethod;
import com.adatao.local.ddf.content.PersistenceHandler.LocalPersistible;
import com.google.common.base.Strings;
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

  private void initializeConfiguration() {
    if (Strings.isNullOrEmpty(Config.getValue(ConfigConstant.ENGINE_NAME_LOCAL.toString(), "kmeans"))) {
      Config.set(ConfigConstant.ENGINE_NAME_LOCAL.toString(), "kmeans",
          String.format("%s#%s", MLSupporter.class.getName(), "dummyKMeans"));
    }
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
  public static class Model extends LocalPersistible implements IModel {

    private static final long serialVersionUID = 824936593281899283L;

    @Expose Object mTrainingResult;


    public Model(Object trainingResult) {
      mTrainingResult = trainingResult;
    }


    /**
     * TODO: We can't serialize this directly because we can't deserialize an IModelParameters later, at least not
     * without some concrete class constructor.
     */
    private IModelParameters mParams;


    @Override
    public IModelParameters getParameters() {
      return mParams;
    }

    @Override
    public void setParameters(IModelParameters parameters) {
      mParams = parameters;
    }


    /**
     * Override to implement additional equality tests
     */
    @Override
    public boolean equals(Object other) {
      if (!(other instanceof Model)) return false;

      if (this.getParameters() != ((Model) other).getParameters()) return false;

      return true;
    }
  }


  public static IModel dummyKMeans() {
    return new Model("Model Paramters");
  }

  // //// ISupportML //////

  /**
   * Runs an unsupervised (unlabled) training algorithm on the entire DDF dataset.
   * 
   * @param trainMethodName
   * @param args
   * @return
   * @throws DDFException
   */
  public IModel train(String trainMethodName, Object... args) throws DDFException {
    int numColumns = (int) this.getDDF().getNumColumns();
    if (numColumns < 0) numColumns = 0;

    int[] featureColumnIndexes = new int[numColumns];
    for (int i = 0; i < numColumns; i++) {
      featureColumnIndexes[i] = i;
    }

    return this.trainImpl(trainMethodName, null, -1, null, featureColumnIndexes, args);
  }

  @Override
  public IModel train(String trainMethodName, int[] featureColumnIndexes, Object... args) throws DDFException {
    return this.trainImpl(trainMethodName, null, -1, null, featureColumnIndexes, args);
  }

  @Override
  public IModel train(String trainMethodName, int targetColumnIndex, int[] featureColumnIndexes, Object... args)
      throws DDFException {

    return this.trainImpl(trainMethodName, null, targetColumnIndex, null, featureColumnIndexes, args);
  }

  @Override
  public IModel train(String trainMethodName, String[] featureColumnNames, Object... args) throws DDFException {
    return this.trainImpl(trainMethodName, null, -1, featureColumnNames, null, args);
  }

  @Override
  public IModel train(String trainMethodName, String targetColumnName, String[] featureColumnNames, Object... args)
      throws DDFException {

    return this.trainImpl(trainMethodName, targetColumnName, -1, featureColumnNames, null, args);
  }


  public static final String DEFAULT_TRAIN_METHOD_NAME = "train";


  private IModel trainImpl(String trainMethodName, String targetColumnName, int targetColumnIndex,
      String[] featureColumnNames, int[] featureColumnIndexes, Object... paramArgs) throws DDFException {

    // Map column names to indices, if necessary
    if (!Strings.isNullOrEmpty(targetColumnName) && targetColumnIndex != -1) {
      targetColumnIndex = this.getColumnIndex(targetColumnName);
    }

    if (featureColumnNames != null && featureColumnIndexes == null) {
      featureColumnIndexes = this.getColumnIndexes(featureColumnNames);
    }

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
    Class<?>[] argTypes = new Class<?>[paramArgs.length];
    for (int i = 0; i < paramArgs.length; i++) {
      argTypes[i] = (paramArgs[i] == null ? null : paramArgs[i].getClass());
    }

    // Locate the training method
    String mappedName = Config.getValueWithGlobalDefault(this.getEngine(), trainMethodName);
    if (!Strings.isNullOrEmpty(mappedName)) trainMethodName = mappedName;

    MLClassMethod trainMethod = new MLClassMethod(trainMethodName, DEFAULT_TRAIN_METHOD_NAME, argTypes);
    if (trainMethod.getMethod() == null) {
      throw new DDFException(String.format("Cannot locate method specified by %s", trainMethodName));
    }

    // Now we need to map the DDF and its column specs to the input format expected by the method we're invoking
    Object[] allArgs = this.buildArgsForMethod(trainMethod.getMethod(), paramArgs);

    // Invoke the training method
    Object result = trainMethod.classInvoke(allArgs);

    // Construct the result model parameters
    IModel model = new Model(result);

    return model;
  }


  private Object[] buildArgsForMethod(Method method, Object[] paramArgs) {
    return null; // TODO
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

      Method foundMethod = null;

      // Scan all methods
      for (Method method : theClass.getDeclaredMethods()) {
        if (!methodName.equalsIgnoreCase(method.getName())) continue;

        // Scan all method arg types, starting from the end, and see if they match with our supplied arg types
        Class<?>[] methodArgTypes = method.getParameterTypes();
        boolean allMatched = true;
        for (int a = argTypes.length - 1, m = methodArgTypes.length - 1; a >= 0; a--, m--) {

          if ((argTypes[a] != null && !methodArgTypes[m].isAssignableFrom(argTypes[a])) //
              || //
              (argTypes[a] == null && !methodArgTypes[m].isAssignableFrom(Object.class)) //
          ) {
            allMatched = false;
            break;
          }

        }

        if (allMatched) {
          foundMethod = method;
          break;
        }
      }

      if (foundMethod != null) {
        foundMethod.setAccessible(true);
        this.setMethod(foundMethod);
      }
    }
  }


  private int getColumnIndex(String columnName) throws DDFException {
    return this.getColumnIndexes(new String[] { columnName })[0];
  }

  private int[] getColumnIndexes(String[] columnNames) throws DDFException {
    if (columnNames == null || columnNames.length == 0) {
      throw new DDFException("List of column names cannot be null or empty");
    }

    int[] columnIndexes = new int[columnNames.length];

    for (int i = 0; i < columnNames.length; i++) {
      columnIndexes[i] = this.getDDF().getColumnIndex(columnNames[i]);
    }

    return columnIndexes;
  }
}
