package com.adatao.ddf;

/**
 * <p>
 * Handles the underlying, implementation-specific representation(s) of a DataFrame. Note that a
 * single DataFrame may have simultaneously multiple representations, all of which are expected to
 * be equivalent in terms of relevant content value.
 * </p>
 * <p>
 * For example, a DataFrame may initially be represented as an RDD[TablePartition]. But in order to
 * send it to a machine-learning algorithm, an RDD[LabeledPoint] representation is needed. An
 * {@link IHandleRepresentations} is expected to help perform this transformation, and it may still
 * hold on to both representations, until they are somehow invalidated.
 * </p>
 * <p>
 * In another example, a DataFrame may be mutated in a transformation from one RDD to a new RDD. It
 * should automatically keep track of the reference to the new RDD, so that to the client of
 * DataFrame, it properly appears to have been mutated. The underlying RDDs are of course immutable.
 * </p>
 * 
 * @author ctn
 * 
 */
public interface IHandleRepresentations {
  /**
   * Retrieves a row-based representation as an containerType of elementType. There are no
   * guarantees whether this is the actual
   * 
   * @param containerType
   * @param elementType
   * @return a pointer to the specified
   */
  public Object get(Class<?> containerType, Class<?> elementType);

  /**
   * Clears out all current representations.
   */
  public void reset();

  /**
   * Clears all current representations and set it to the supplied one.
   * 
   * @param rows
   * @param containerType
   * @param elementType
   */
  public void set(Object rows, Class<?> containerType, Class<?> elementType);

  /**
   * Add a representation to the set of existing representations.
   * 
   * @param rows
   * @param containerType
   * @param elementType
   */
  public void add(Object rows, Class<?> containerType, Class<?> elementType);
}
