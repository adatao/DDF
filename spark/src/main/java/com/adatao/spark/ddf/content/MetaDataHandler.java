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
public class MetaDataHandler extends AMetaDataHandler {
	private static Logger logger = Logger.getLogger(MetaDataHandler.class);

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
		//TODO get multi factors
		//then build ListCustomMetaData
		HashMap<Integer, ICustomMetaData>  a = new HashMap<Integer, ICustomMetaData> ();
		DummyCustomMetaData dcmd = new DummyCustomMetaData();
		dcmd.mapping =  new HashMap<String, Integer> ();
		dcmd.mapping.put("IND", 1);
		dcmd.mapping.put("ISP", 2);
		dcmd.mapping.put("IAD", 3);
		a.put(1, (DummyCustomMetaData) dcmd);
		
		System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> buildListCustomMetaData: ");
		
		
		java.util.Iterator<Integer> it = a.keySet().iterator();
		while(it.hasNext()) {
			DummyCustomMetaData customMetaData = (DummyCustomMetaData) a.get(it.next());
			if(customMetaData != null) System.out.println(">>>>customMetaData: " + customMetaData);
			this.setCustomMetaData(customMetaData);
		}
		
	}
	
	
	@Override
	public HashMap<Integer, ICustomMetaData> getListCustomMetaData() {
		HashMap<Integer, ICustomMetaData> a = super.getListCustomMetaData();
		return (a);
	}

	public class DummyCustomMetaData implements ICustomMetaData {
		/**
		 * 
		 */
		public HashMap<String, Integer> mapping = new HashMap<String, Integer> ();

		public DummyCustomMetaData() {
			 mapping = new HashMap<String, Integer> ();
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
			return 0;
		}
		
		public int getColumnIndex(String value) {
			return 0;
		}
	}
}
