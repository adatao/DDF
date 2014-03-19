/*
 *  Copyright (C) 2013 Adatao, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/**
 * 
 */
package com.adatao.pa.spark.types;


import java.io.Serializable;
import java.util.Iterator;


/**
 * Will skip all null rows
 * 
 * @author ctn
 * 
 */
@SuppressWarnings("serial")
public class FastArrayIterator<T> implements Serializable, Iterator<T> {

	private int mRow = 0;
	private int mMaxRows;
	private T[] mData;

	/**
	 * Needed by some deserializers
	 */
	public FastArrayIterator() {

	}

	public FastArrayIterator(T[] data) {
		this.mData = data;
		this.mMaxRows = (this.mData == null ? 0 : this.mData.length);
		this.advanceToNextNonNullRow();
	}

	private void advanceToNextNonNullRow() {
		while (mRow < mMaxRows && mData[mRow] == null) {
			mRow++;
		}
	}

	@Override
	public boolean hasNext() {
		return mRow < mMaxRows;
	}

	/**
	 * Will skip all null rows
	 */
	@Override
	public T next() {
		T result = mData[mRow++];
		this.advanceToNextNonNullRow();
		return result;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("remove() not supported");
	}
}
