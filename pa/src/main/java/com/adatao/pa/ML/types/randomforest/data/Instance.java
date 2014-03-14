package com.adatao.pa.ML.types.randomforest.data;

import java.io.Serializable;

@SuppressWarnings("serial")
/**
 * Store the information of one instance: which is a double array.
 * @author phamnamlong
 *
 */
public class Instance implements Serializable {
	private double[] attrs;

	public Instance(double[] v) {
		this.attrs = v.clone();
	}

	public double get(int i) {
		return this.attrs[i];
	}

	@Override
	public String toString() {
		String res = "";
		for (int i = 0; i < attrs.length; i++) {
			res += get(i);
			if (i < attrs.length - 1)
				res += ",";
		}
		return res;
	}
	
	public int size() {
		return attrs.length;
	}
}
