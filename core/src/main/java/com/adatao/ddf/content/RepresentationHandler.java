/**
 * 
 */
package com.adatao.ddf.content;


import java.util.HashMap;
import java.util.Map;
import com.adatao.ddf.DDF;
import com.adatao.ddf.exception.DDFException;
import com.adatao.ddf.misc.ADDFFunctionalGroupHandler;
import com.adatao.ddf.types.AGloballyAddressable;
import com.adatao.ddf.types.IGloballyAddressable;

/**
 *
 */
public class RepresentationHandler extends ADDFFunctionalGroupHandler implements IHandleRepresentations {

  public RepresentationHandler(DDF theDDF) {
    super(theDDF);
  }


  // The various representations for our DDF
  protected HashMap<String, Object> mReps = new HashMap<String, Object>();


  public static String getKeyFor(Class<?>[] typeSpecs) {
    if (typeSpecs == null || typeSpecs.length == 0) return "null";

    StringBuilder sb = new StringBuilder();
    for (Class<?> c : typeSpecs) {
      sb.append(c == null ? "null" : c.getName());
      sb.append(':');
    }

    if (sb.length() > 0) sb.deleteCharAt(sb.length() - 1); // remove last ':'

    return sb.toString();
  }


  @Override
  public String getSpecsAsString(Class<?>... typeSpecs) {
    return getKeyFor(typeSpecs);
  }

  /**
   * Gets an existing representation for our {@link DDF} matching the given dataType, if any.
   * 
   * @param typeSpecs
   * 
   * @return null if no matching representation available
   */
  @Override
  public Object get(Class<?>... typeSpecs) throws DDFException {
    return this.get(this.getSpecsAsString(typeSpecs), true);
  }


  @Override
  public Object get(String typeSpecs) throws DDFException {
    return this.get(typeSpecs, true);
  }

  @Override
  public IGetResult get(Class<?>[][] acceptableTypeSpecs) throws DDFException {
    if (acceptableTypeSpecs == null || acceptableTypeSpecs.length == 0) return null;

    // First check what formats are *already* there, so we can save the effort of having to create it
    for (Class<?>[] ats : acceptableTypeSpecs) {
      if (this.has(ats)) return new GetResult(this.get(this.getSpecsAsString(ats), false), ats);
    }

    // Now see which formats can be created
    for (Class<?>[] ats : acceptableTypeSpecs) {
      Object result = this.get(this.getSpecsAsString(ats), true);
      if (result != null) return new GetResult(result, ats);
    }

    return null;
  }

  @Override
  public IGetResult get(String[] acceptableTypeSpecs) throws DDFException {
    if (acceptableTypeSpecs == null || acceptableTypeSpecs.length == 0) return null;

    // First check what formats are *already* there, so we can save the effort of having to create it
    for (String ats : acceptableTypeSpecs) {
      if (this.has(ats)) return new GetResult(this.get(ats, false), ats);
    }

    // Now see which formats can be created
    for (String ats : acceptableTypeSpecs) {
      Object result = this.get(ats, true);
      if (result != null) return new GetResult(result, ats);
    }

    return null;
  }

  private Object get(String typeSpecs, boolean doCreate) throws DDFException {
    if(this.mReps.size() == 0) {
      throw new DDFException("DDF contains no representation");
    }
    Object obj = mReps.get(typeSpecs);

    if (obj == null && doCreate) {
      obj = this.createRepresentation(typeSpecs);
      if (obj != null)  mReps.put(typeSpecs, obj);
    }

    return obj;
  }


  @Override
  public boolean has(String typeSpecs) {
    return mReps.containsKey(typeSpecs);
  }

  @Override
  public boolean has(Class<?>... typeSpecs) {
    return this.has(this.getSpecsAsString(typeSpecs));
  }


  private Class<?>[] mDefaultTypeSpecs;


  /**
   * Returns the default dataType for this engine. The base implementation returns Object[][].class.
   * 
   * @return
   */
  @Override
  public Class<?>[] getDefaultDataType() {
    return mDefaultTypeSpecs;
  }

  @Override
  public void setDefaultDataType(Class<?>... typeSpecs) {
    mDefaultTypeSpecs = typeSpecs;
  }

  @Override
  public Object getDefault() throws DDFException {
    return this.get(this.getDefaultDataType());
  }

  /**
   * Resets (or clears) all representations
   */
  @Override
  public void reset() {
    mReps.clear();
    this.setDefaultDataType((Class<?>[]) null);
  }

  private boolean equalsDefaultDataType(Class<?>... typeSpecs) {
    return this.getSpecsAsString(typeSpecs).equals(this.getSpecsAsString(this.getDefaultDataType()));
  }


  /**
   * Converts from existing representation(s) to the desired representation, which has the specified dataType.
   * 
   * The base representation returns only the default representation if the dataType matches the default type. Otherwise
   * it returns null.
   * 
   * @param dataType
   * @return
   */
  public Object createRepresentation(String typeSpecs) throws DDFException {
    if (typeSpecs != null && typeSpecs.equalsIgnoreCase(this.getSpecsAsString(this.getDefaultDataType()))) {
      return this.get(typeSpecs, false);

    } else {
      return null;
    }
  }

  public static Class<?>[] determineTypeSpecs(Object data, Class<?>... typeSpecs) {
    if (typeSpecs != null && typeSpecs.length > 0) return typeSpecs;
    return (data == null ? null : new Class<?>[] { data.getClass() });
  }

  /**
   * Sets a new and unique representation for our {@link DDF}, clearing out any existing ones
   * 
   */
  @Override
  public void set(Object data, Class<?>... typeSpecs) {
    this.reset();
    this.add(data, typeSpecs);
  }

  /**
   * Adds a new and unique representation for our {@link DDF}, keeping any existing ones but replacing the one that
   * matches the given DDFManagerType, dataType tuple.
   */
  @Override
  public void add(Object data, Class<?>... typeSpecs) {
    if (data == null) return;

    typeSpecs = determineTypeSpecs(data, typeSpecs);
    if (this.getDefaultDataType() == null) this.setDefaultDataType(typeSpecs);

    mReps.put(this.getSpecsAsString(typeSpecs), data);
  }

  /**
   * Removes a representation from the set of existing representations.
   * 
   * @param dataType
   */
  @Override
  public void remove(Class<?>... typeSpecs) {
    mReps.remove(this.getSpecsAsString(typeSpecs));
    //if (this.equalsDefaultDataType(typeSpecs)) this.reset();
  }

  /**
   * Returns a String list of current representations, useful for debugging
   */
  public String getList() {
    String result = "";
    int i = 1;

    for (String s : mReps.keySet()) {
      result += (i++) + ". key='" + s + "', value='" + mReps.get(s) + "'\n";
    }

    return result;
  }

  @Override
  public void cleanup() {
    mReps.clear();
    super.cleanup();
    uncacheAll();
  }

  @Override
  public void cacheAll() {
    // TODO Auto-generated method stub

  }

  @Override
  public void uncacheAll() {
    // TODO Auto-generated method stub
  }


  /**
   * A special class representing a Table that's native to the engine, e.g., Shark Table for the Spark engine.
   */
  public static class NativeTable extends AGloballyAddressable implements IGloballyAddressable {
    private String mNamespace;
    private String mName;


    public NativeTable(String namespace, String name) {
      mNamespace = namespace;
      mName = name;
    }

    @Override
    public String getNamespace() {
      return mNamespace;
    }

    @Override
    public void setNamespace(String namespace) {
      mNamespace = namespace;
    }

    @Override
    public String getName() {
      return mName;
    }

    @Override
    public void setName(String name) {
      mName = name;
    }

    @Override
    public String getGlobalObjectType() {
      return "native_table";
    }
  }


  public static final String NATIVE_TABLE = getKeyFor(new Class<?>[] { NativeTable.class });


  public static class GetResult implements IGetResult {

    public GetResult(Object obj, Class<?>... typeSpecs) {
      this(obj, RepresentationHandler.getKeyFor(typeSpecs));
      mTypeSpecs = typeSpecs;
    }


    /**
     * Internal use only. Don't encourage external use, since we want to require the Class<?>... typeSpecs format.
     * 
     * @param obj
     * @param typeSpecsString
     */
    private GetResult(Object obj, String typeSpecsString) {
      mObject = obj;
      mTypeSpecsString = typeSpecsString;
    }


    private Class<?>[] mTypeSpecs;
    private String mTypeSpecsString;
    private Object mObject;


    @Override
    public Class<?>[] getTypeSpecs() {
      return mTypeSpecs;
    }

    @Override
    public String getTypeSpecsString() {
      return mTypeSpecsString;
    }

    @Override
    public Object getObject() {
      return mObject;
    }
  }


  @Override
  public Map<String, Object> getAllRepresentations() {
    return mReps;
  }
  
  @Override
  public void setRepresentations(Map<String, Object> reps) {
    mReps.clear();
    mReps.putAll(reps);
  }

}
