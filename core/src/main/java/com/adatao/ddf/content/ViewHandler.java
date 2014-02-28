/**
 * 
 */
package com.adatao.ddf.content;


import java.util.Iterator;
import java.util.List;

import com.adatao.ddf.ADDFFunctionalGroupHandler;
import com.adatao.ddf.DDF;

/**
 * 
 */
public class ViewHandler extends ADDFFunctionalGroupHandler implements IHandleViews {

  public ViewHandler(DDF theDDF) {
    super(theDDF);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> Iterator<T> getRowIterator(Class<T> rowType) {
    if (rowType == null) rowType = (Class<T>) this.getDDF().getRepresentationHandler().getDefaultRowType();

    Object repr = this.getDDF().getRepresentationHandler().get(rowType);
    return (repr instanceof Iterable<?>) ? ((Iterable<T>) repr).iterator() : null;
  }

  @Override
  public DDF getRandomSample(int numSamples) {
    // TODO Auto-generated method stub
    return null;
  }



  public static class ElementIterator<R, C> implements Iterator<C> {

    private Iterator<R> mRowIterator;
    private Class<R> mRowType;
    private int mColumnIndex;

    public ElementIterator(Iterator<R> rowIterator, Class<R> rowType, int columnIndex) {
      mRowIterator = rowIterator;
      mRowType = rowType;
      mColumnIndex = columnIndex;
    }

    @Override
    public boolean hasNext() {
      return mRowIterator.hasNext();
    }

    @SuppressWarnings("unchecked")
    @Override
    public C next() {
      R row = mRowIterator.next();

      if (mRowType.isArray()) {
        C[] x = (C[]) row;
        return x[mColumnIndex];

      } else if (List.class.isAssignableFrom(mRowType)) {
        return ((List<C>) row).get(mColumnIndex);

      } else {
        return null;
      }
    }

    @Override
    public void remove() {
      // Not supported
    }
  }

  /**
   * The base implementation supports the case where the rowType is an Array or List
   */
  @SuppressWarnings("unchecked")
  @Override
  public <R, C> Iterator<C> getElementIterator(Class<R> rowType, Class<C> columnType, int columnIndex) {
    if (rowType == null) rowType = (Class<R>) this.getDDF().getRepresentationHandler().getDefaultRowType();
    if (columnType == null) columnType = (Class<C>) this.getDDF().getRepresentationHandler().getDefaultColumnType();

    if (List.class.isAssignableFrom(rowType) || rowType.isArray()) {
      Iterator<R> rowIterator = this.getRowIterator(rowType);
      return new ElementIterator<R, C>(rowIterator, rowType, columnIndex);

    } else {
      return null;
    }
  }

  @Override
  public <R, C> Iterator<C> getElementIterator(Class<R> rowType, Class<C> columnType, String columnName) {
    return this.getElementIterator(rowType, columnType, this.getDDF().getColumnIndex(columnName));
  }

  @Override
  public Iterator<?> getRowIterator() {
    return this.getRowIterator(null);
  }

  @Override
  public Iterator<?> getElementIterator(int columnIndex) {
    return this.getElementIterator(null, null, columnIndex);
  }

  @Override
  public Iterator<?> getElementIterator(String columnName) {
    return this.getElementIterator(null, null, this.getDDF().getColumnIndex(columnName));
  }

}
