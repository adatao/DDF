/**
 * 
 */
package com.adatao.ddf.content;

import java.io.Serializable;
import java.util.HashMap;
import java.util.UUID;

import com.adatao.ddf.DDF;
import com.adatao.ddf.misc.ADDFFunctionalGroupHandler;

/**
 * @author ctn
 * 
 */
public abstract class AMetaDataHandler extends ADDFFunctionalGroupHandler
		implements IHandleMetaData {

	public AMetaDataHandler(DDF theDDF) {
		super(theDDF);
	}

	private UUID mId = UUID.randomUUID();

	@Override
	public UUID getId() {
		return mId;
	}

	@Override
	public void setId(UUID id) {
		mId = id;
	}

	private long mNumRows = 0L;
	private boolean bNumRowsIsValid = false;

	/**
	 * Each implementation needs to come up with its own way to compute the row
	 * count.
	 * 
	 * @return row count of a DDF
	 */
	protected abstract long getNumRowsImpl();

	/**
	 * Called to assert that the row count needs to be recomputed at next access
	 */
	protected void invalidateNumRows() {
		bNumRowsIsValid = false;
	}

	@Override
	public long getNumRows() {
		if (!bNumRowsIsValid) {
			mNumRows = this.getNumRowsImpl();
			bNumRowsIsValid = true;
		}
		return mNumRows;
	}

	private HashMap<Integer, ICustomMetaData> mCustomMetaDatas;

	public HashMap<Integer, ICustomMetaData> getmCustomMetaDatas() {
		return mCustomMetaDatas;
	}

	public void setmCustomMetaDatas(
			HashMap<Integer, ICustomMetaData> mCustomMetaDatas) {
		this.mCustomMetaDatas = mCustomMetaDatas;
	}

	public ICustomMetaData getCustomMetaData(int idx) {
		return mCustomMetaDatas.get(idx);
	}

	public void setCustomMetaData(ICustomMetaData customMetaData) {
		mCustomMetaDatas.put(customMetaData.getColumnIndex(), customMetaData);
	}

	public HashMap<Integer, ICustomMetaData> getListCustomMetaData() {
		return mCustomMetaDatas;
	}

	public static interface ICustomMetaData extends Serializable  {
		

		public double[] buildCoding(String value);

		public double get(String value, int idx);

		public int getColumnIndex();

		public int getColumnIndex(String value);
	}

}
