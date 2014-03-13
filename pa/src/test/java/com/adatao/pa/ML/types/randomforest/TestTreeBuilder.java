package com.adatao.pa.ML.types.randomforest;

import static org.junit.Assert.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.Test;
import com.adatao.pa.ML.types.randomforest.data.Data;
import com.adatao.pa.ML.types.randomforest.data.DataFormat;
import com.adatao.pa.ML.types.randomforest.data.Instance;
import com.adatao.pa.ML.types.randomforest.tree.TreeBuilder;
import com.adatao.pa.spark.DataManager.MetaInfo;
import adatao.ML.types.randomforest.node.Node;

public class TestTreeBuilder {

	private long seed = 0;
	
	@Test
	public final void testTreeBuilderNumReg() {
		MetaInfo[] metaInfo = new MetaInfo[2];

		metaInfo[0] = new MetaInfo("Score", "double", 1);
		metaInfo[0].setFactor(null);
		metaInfo[1] = new MetaInfo("Grade", "double", 2);
		metaInfo[1].setFactor(null);
		int[] xCols = { 0 };
		int yCol = 1;

		DataFormat dataFormat = new DataFormat(metaInfo, xCols, yCol);

		List<Instance> inst = new ArrayList<Instance>();
		inst.add(new Instance(new double[] { 81, 0 }));
		inst.add(new Instance(new double[] { 80, 0 }));
		inst.add(new Instance(new double[] { 79, 1 }));
		inst.add(new Instance(new double[] { 70, 1 }));
		inst.add(new Instance(new double[] { 69, 2 }));
		inst.add(new Instance(new double[] { 60, 2 }));

		Data data = new Data(dataFormat, inst);
		TreeBuilder treeBuilder = new TreeBuilder(data, seed);
		Node tree = treeBuilder.build();
		System.out.println(tree.toString());
		assertEquals(0, (int) tree.classify(new Instance(new double[] { 84, -1 })));
		assertEquals(0, (int) tree.classify(new Instance(new double[] { 80, -1 })));
		assertEquals(1, (int) tree.classify(new Instance(new double[] { 79, -1 })));
		assertEquals(2, (int) tree.classify(new Instance(new double[] { 69, -1 })));
		assertEquals(2, (int) tree.classify(new Instance(new double[] { 59, -1 })));
	}

	@Test
	public final void testTreeBuilderNumCla() {
		MetaInfo[] metaInfo = new MetaInfo[2];

		metaInfo[0] = new MetaInfo("Score", "double", 1);
		metaInfo[0].setFactor(null);
		metaInfo[1] = new MetaInfo("Grade", "string", 2);
		Map<String, Integer> factor = new HashMap<String, Integer>();
		factor.put("A", 0);
		factor.put("B", 1);
		factor.put("C", 2);
		metaInfo[1].setFactor(factor);
		int[] xCols = { 0 };
		int yCol = 1;

		DataFormat dataFormat = new DataFormat(metaInfo, xCols, yCol);

		List<Instance> inst = new ArrayList<Instance>();
		inst.add(new Instance(new double[] { 81, 0 }));
		inst.add(new Instance(new double[] { 80, 0 }));
		inst.add(new Instance(new double[] { 79, 1 }));
		inst.add(new Instance(new double[] { 70, 1 }));
		inst.add(new Instance(new double[] { 69, 2 }));
		inst.add(new Instance(new double[] { 60, 2 }));

		Data data = new Data(dataFormat, inst);
		TreeBuilder treeBuilder = new TreeBuilder(data, seed);
		Node tree = treeBuilder.build();
		System.out.println(tree.toString());
		assertEquals(0, (int) tree.classify(new Instance(new double[] { 84, -1 })));
		assertEquals(0, (int) tree.classify(new Instance(new double[] { 80, -1 })));
		assertEquals(1, (int) tree.classify(new Instance(new double[] { 79, -1 })));
		assertEquals(2, (int) tree.classify(new Instance(new double[] { 69, -1 })));
		assertEquals(2, (int) tree.classify(new Instance(new double[] { 59, -1 })));
	}

	@Test
	public final void testTreeBuilderCatReg() {
		MetaInfo[] metaInfo = new MetaInfo[2];

		metaInfo[0] = new MetaInfo("Grade", "string", 1);
		Map<String, Integer> factor = new HashMap<String, Integer>();
		factor.put("A", 0);
		factor.put("B", 1);
		factor.put("C", 2);
		metaInfo[0].setFactor(factor);
		metaInfo[1] = new MetaInfo("Score", "double", 2);
		metaInfo[1].setFactor(null);
		int[] xCols = { 0 };
		int yCol = 1;

		DataFormat dataFormat = new DataFormat(metaInfo, xCols, yCol);

		List<Instance> inst = new ArrayList<Instance>();
		inst.add(new Instance(new double[] { 0, 89 }));
		inst.add(new Instance(new double[] { 0, 86 }));
		inst.add(new Instance(new double[] { 0, 80 }));
		inst.add(new Instance(new double[] { 0, 80 }));
		inst.add(new Instance(new double[] { 1, 79 }));
		inst.add(new Instance(new double[] { 1, 75 }));
		inst.add(new Instance(new double[] { 1, 71 }));
		inst.add(new Instance(new double[] { 1, 70 }));
		inst.add(new Instance(new double[] { 2, 69 }));
		inst.add(new Instance(new double[] { 2, 67 }));
		inst.add(new Instance(new double[] { 2, 66 }));
		inst.add(new Instance(new double[] { 2, 61 }));

		Data data = new Data(dataFormat, inst);
		TreeBuilder treeBuilder = new TreeBuilder(data, seed);
		Node tree = treeBuilder.build();
		System.out.println(tree.toString());
		assertTrue(80 <= tree.classify(new Instance(new double[] { 0, -1 })));
		assertTrue(80 > tree.classify(new Instance(new double[] { 1, -1 })));
		assertTrue(70 <= tree.classify(new Instance(new double[] { 1, -1 })));
		assertTrue(70 > tree.classify(new Instance(new double[] { 2, -1 })));
		assertTrue(60 <= tree.classify(new Instance(new double[] { 2, -1 })));
	}

	@Test
	public final void testTreeBuilderCatCla() {
		MetaInfo[] metaInfo = new MetaInfo[2];

		metaInfo[0] = new MetaInfo("Grade", "string", 1);
		Map<String, Integer> factor = new HashMap<String, Integer>();
		factor.put("A", 0);
		factor.put("B", 1);
		factor.put("C", 2);
		factor.put("D", 3);
		metaInfo[0].setFactor(factor);
		metaInfo[1] = new MetaInfo("Score", "double", 2);
		Map<String, Integer> factor1 = new HashMap<String, Integer>();
		factor1.put("Good", 0);
		factor1.put("Average", 1);
		factor1.put("Bad", 2);
		metaInfo[1].setFactor(factor1);
		int[] xCols = { 0 };
		int yCol = 1;

		DataFormat dataFormat = new DataFormat(metaInfo, xCols, yCol);

		List<Instance> inst = new ArrayList<Instance>();
		inst.add(new Instance(new double[] { 0, 0 }));
		inst.add(new Instance(new double[] { 1, 1 }));
		inst.add(new Instance(new double[] { 2, 2 }));
		inst.add(new Instance(new double[] { 3, 2 }));

		Data data = new Data(dataFormat, inst);
		TreeBuilder treeBuilder = new TreeBuilder(data, seed);
		treeBuilder.setMinSplitNum(1);
		Node tree = treeBuilder.build();
		System.out.println(tree.toString());
		assertEquals(0, (int) tree.classify(new Instance(new double[] { 0, -1 })));
		assertEquals(1, (int) tree.classify(new Instance(new double[] { 1, -1 })));
		assertEquals(2, (int) tree.classify(new Instance(new double[] { 2, -1 })));
		assertEquals(2, (int) tree.classify(new Instance(new double[] { 3, -1 })));
	}

}
