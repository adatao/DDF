package com.adatao.ddf.content;

import com.adatao.ddf.etl.IHandleFilteringAndProjections;

/**
 * <p>
 * Handles the underlying, implementation-specific representation(s) of a DDF. Note that a single
 * DDF may have simultaneously multiple representations, all of which are expected to be equivalent
 * in terms of relevant content value. Contrast this with, e.g., {@link IHandleFilteringAndProjections},
 * which results in entirely new DDFs with different columns or rows. These new DDFs are logically
 * referred to as new "views".
 * </p>
 * <p>
 * For example, a DDF may initially be represented as an RDD[TablePartition]. But in order to send
 * it to a machine-learning algorithm, an RDD[LabeledPoint] representation is needed. An
 * {@link IHandleRepresentations} is expected to help perform this transformation, and it may still
 * hold on to both representations, until they are somehow invalidated.
 * </p>
 * <p>
 * In another example, a DDF may be mutated in a transformation from one RDD to a new RDD. It should
 * automatically keep track of the reference to the new RDD, so that to the client of DDF, it
 * properly appears to have been mutated. This makes it possible to for DDF to support R-style
 * replacement functions. The underlying RDDs are of course immutable.
 * </p>
 * <p>
 * As a final example, a DDF may be set up to accept incoming streaming data. The client has a constant
 * reference to this DDF, but the underlying data is constantly being updated, such that each query against
 * this DDF would result in different data being returned/aggregated.
 * </p>
 * 
 * @author ctn
 * 
 */
public interface IHandleRepresentations {
  /**
   * Retrieves a representation with elements of elementType.
   * 
   * @param elementType
   * @return a pointer to the specified
   */
  public Object get(Class<?> elementType);

  /**
   * Clears out all current representations.
   */
  public void reset();

  /**
   * Clears all current representations and set it to the supplied one.
   * 
   * @param data
   * @param elementType
   */
  public void set(Object data, Class<?> elementType);

  /**
   * Adds a representation to the set of existing representations.
   * 
   * @param data
   * @param elementType
   */
  public void add(Object data, Class<?> elementType);

  /**
   * Removes a representation from the set of existing representations.
   * 
   * @param elementType
   */
  public void remove(Class<?> elementType);
  
  /**
   * Cache all representations, e.g., in an in-memory context
   */
  public void cacheAll();
  
  /**
   * Uncache all representations, e.g., in an in-memory context
   */
  public void uncacheAll();
}
