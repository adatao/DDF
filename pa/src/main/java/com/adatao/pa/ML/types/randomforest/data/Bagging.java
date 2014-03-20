package com.adatao.pa.ML.types.randomforest.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Bagging {
	Data originalData;
	Random random;

	public Bagging(Data data, long seed) {
		this.originalData = data;
		this.random = new Random(seed);
	}

	public Data[] sampleWithPlacement() {
		int N = originalData.size();
		
		Data[] subSets = new Data[2];

		// sample with replacement
		List<Instance> instances = new ArrayList<Instance>();
		boolean seen[] = new boolean[N];
		for (int i = 0; i < N; i++) {
			int rIndex = random.nextInt(N);
			seen[rIndex] = true;
			instances.add(originalData.get(rIndex));
		}

		// then find the out-of-bagg (OOB) data
		List<Instance> oob = new ArrayList<Instance>();
		for (int i = 0; i < N; i++)
			if (!seen[i]) {
				oob.add(originalData.get(i));
			}
		
		// reuse the DataFormat of originalData
		subSets[0] = new Data(originalData.getDataFormat(), instances);
		subSets[1] = new Data(originalData.getDataFormat(), oob);
		return subSets;
	}
}
