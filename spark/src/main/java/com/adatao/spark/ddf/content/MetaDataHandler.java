package com.adatao.spark.ddf.content;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.math.util.OpenIntToDoubleHashMap.Iterator;
import org.apache.log4j.Logger;

import com.adatao.ddf.DDF;
import com.adatao.ddf.content.AMetaDataHandler;
import com.adatao.ddf.content.AMetaDataHandler.ICustomMetaData;

/**
 *
 */
public class MetaDataHandler extends AMetaDataHandler implements Serializable {
	private static Logger logger = Logger.getLogger(MetaDataHandler.class);
	

	public HashMap<Integer, DummyCustomMetaData> dataMapping = new HashMap<Integer, DummyCustomMetaData>();
	
	public MetaDataHandler(DDF theDDF) {
		super(theDDF);
	}

	@Override
	protected long getNumRowsImpl() {
		String tableName = this.getDDF().getSchemaHandler().getTableName();
		logger.debug("get NumRows Impl called");
		try {
			List<String> rs = this.getManager().sql2txt(
					"SELECT COUNT(*) FROM " + tableName);
			return Long.parseLong(rs.get(0));
		} catch (Exception e) {
			logger.error("Unable to query from " + tableName, e);
		}
		return 0;
	}

	public void buildListCustomMetaData() {
		// TODO get multi factors
		// then build ListCustomMetaData
		
		DummyCustomMetaData dcmd = new DummyCustomMetaData();

		dcmd.columnIndex = 1;
		dcmd.mapping = new HashMap<String, Integer>();
		dcmd.mapping.put("IND", 1);
		dcmd.mapping.put("ISP", 2);
		dcmd.mapping.put("IAD", 3);
		dataMapping.put(1, (DummyCustomMetaData) dcmd);

	}

	public class DummyCustomMetaData implements Serializable {
		/**
		 * 
		 */
		public Integer columnIndex;
		public HashMap<String, Integer> mapping;

		public DummyCustomMetaData() {
			mapping = new HashMap<String, Integer>();
		}

		private static final long serialVersionUID = 1L;

		public double[] buildCoding(String value) {
			return null;
		}

		public double get(String value, int idx) {
			return 0;
		}

		public int getColumnIndex() {
			System.out.println(">>>>>>>getColumnIndex");
			return (columnIndex);
		}

		public int getColumnIndex(String value) {
			return 0;
		}
	}
}
