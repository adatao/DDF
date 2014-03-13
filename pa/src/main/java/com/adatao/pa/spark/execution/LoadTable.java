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

package com.adatao.pa.spark.execution;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.actors.threadpool.Arrays;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import com.adatao.pa.spark.SparkThread;
import com.adatao.pa.spark.DataManager.DataFrame;
import com.adatao.pa.spark.DataManager.MetaInfo;
import com.adatao.pa.spark.types.ExecutorResult;
import com.adatao.pa.spark.types.FastArrayIterator;
import com.adatao.pa.spark.types.IExecutor;
import com.adatao.pa.spark.types.SuccessResult;


@SuppressWarnings("serial")
public class LoadTable implements IExecutor {

	// { API variables

	private String fileURL;

	private boolean hasHeader = false;

	private String separator = " ";

	private String fileType;

	/**
	 * If true, always use double in preference to integer. This is useful to avoid repeated casting of ints into
	 * doubles for, e.g., linear regression
	 */
	private boolean doPreferDouble = true;

	/**
	 * If not null, then parse only these columns. Default = all columns.
	 */
	private int[] columns;

	/**
	 * If one of the columns parses to null or NaN, drop the whole line. Default = don't drop. The way we support this
	 * is as follows: first, if we detect a null or NaN in a row, we set the entire row value to null. Then in the
	 * partition iterator returned, by using SerializableObjectArrayIterator, it will skip all null row values
	 * automatically.
	 */
	private boolean doDropNaNs;

	private int sampleSize = 2;



	public String getFileURL() {
		return fileURL;
	}


	public LoadTable setFileURL(String fileURL) {
		this.fileURL = fileURL;
		return this;
	}


	public boolean isHasHeader() {
		return hasHeader;
	}


	public LoadTable setHasHeader(boolean hasHeader) {
		this.hasHeader = hasHeader;
		return this;
	}


	public String getSeparator() {
		return separator;
	}


	public LoadTable setSeparator(String separator) {
		this.separator = separator;
		return this;

	}


	public String getFileType() {
		return fileType;
	}


	public LoadTable setFileType(String fileType) {
		this.fileType = fileType;
		return this;

	}


	public boolean isDoPreferDouble() {
		return doPreferDouble;
	}


	public LoadTable setDoPreferDouble(boolean doPreferDouble) {
		this.doPreferDouble = doPreferDouble;
		return this;
	}


	public int[] getColumns() {
		return columns;
	}


	public LoadTable setColumns(int[] columns) {
		this.columns = columns.clone();
		return this;

	}


	public boolean isDoDropNaNs() {
		return doDropNaNs;
	}


	public LoadTable setDoDropNaNs(boolean doDropNaNs) {
		this.doDropNaNs = doDropNaNs;
		return this;

	}


	public int getSampleSize() {
		return sampleSize;
	}


	public LoadTable setSampleSize(int sampleSize) {
		this.sampleSize = sampleSize;
		return this;

	}

	// } API variables



	// private DataManager dataManager;

	/**
	 * Used to share the parsed metadata with all partitions
	 */
	private Broadcast<MetaInfo[]> broadcastMetaInfo;


	public static Logger LOG = LoggerFactory.getLogger(LoadTable.class);


	/**
	 * Parse all lines in one partition from the file and return a JavaRDD<Object[]>.
	 */
	static class ParsePartition extends FlatMapFunction<Iterator<String>, Object[]> {

		private String separator;
		private Broadcast<MetaInfo[]> broadcastMetaInfo;
		private int[] columnIndices; // columns to parse. Default: all columns. Base 1.
		private boolean doDropNaNs; // if true, set the whole line to null if one of the columns parses to NaN or null.

		/**
		 * 
		 * @param separator
		 *            column separator
		 * @param broadcastMetaInfo
		 * @param columnIndices
		 *            the array of column indices to parse. Base 1.
		 * @param doDropNaNs
		 */
		public ParsePartition(String separator, Broadcast<MetaInfo[]> broadcastMetaInfo, int[] columnIndices,
				boolean doDropNaNs) {
			this.separator = separator;
			this.broadcastMetaInfo = broadcastMetaInfo;
			this.columnIndices = columnIndices.clone();
			this.doDropNaNs = doDropNaNs;

			this.initializeColumnList();
		}

		// Initialize column list, if not defined by client
		private void initializeColumnList() {
			if (this.columnIndices != null) {
				// Client passes it in as base-1, we adjust it here to base-0 so we can use it later
				for (int i = 0; i < this.columnIndices.length; i++) {
					this.columnIndices[i]--;
				}
			}
			else {
				MetaInfo[] metaInfo = broadcastMetaInfo.value();
				this.columnIndices = new int[metaInfo.length];
				for (int i = 0; i < metaInfo.length; i++) {
					this.columnIndices[i] = i;
				}
			}
		}

		/**
		 * new method to test performance 
		 * assumption: input is double
		 * if performance is significant improves we can extend this for other data type
		 */
		@Override
		public Iterable<Object[]> call(Iterator<String> lines) throws Exception {

			Iterable<Object[]> result = null;

			//copy of lines
			int numLines = 0;
			//have to do this loop and copy because of needed numLines 
			//will change this later
			ArrayList<String> lines0 = new ArrayList<String> ();
			// First convert the list of lines into an array of lines
			while (lines.hasNext()) {
				//				String[] tmp = lines.next().split(separator);
				lines0.add(lines.next());
				numLines++;
			}

			final Object[][] parsedRows = new Object[numLines][this.columnIndices.length];

			// Now for a given data type, walk through the data, one row at a time, and parse it

			String item; // a single [row,column] cell value
			String[] items = null;
			MetaInfo[] metaInfoArray = broadcastMetaInfo.value();
			String type = "";
			
			for (int row = 0; row < numLines; row++) {
				//this might look expensive but overall it would be minor
				items = lines0.get(row).split(separator);
				
				for (int outputColumn = 0; outputColumn < this.columnIndices.length; outputColumn++) {
					int inputColumn = this.columnIndices[outputColumn];
					item = items[inputColumn];
					
					type = metaInfoArray[inputColumn].getType();
					if ("Unknown".equals(type)) {
						// Try to figure out the column type is, by trial-and-error
						if (numLines > 1) { // avoid trying to do this on a header line
							String testString = items[inputColumn];//lines3[1][inputColumn];
							try {
								Integer.parseInt(testString);
								type = "java.lang.Integer";
							}
							catch (NumberFormatException e) {
								try {
									Double.parseDouble(testString);
									type = "java.lang.Double";
								}
								catch (NumberFormatException e1) {
									type = "java.lang.String";
								}
							}
						}
					}
					
//					System.out.println(">>>> type=" + type);
				
					//double
					if ("java.lang.Double".equals(type)) {
						try {
							parsedRows[row][outputColumn] = Double.parseDouble(item);
						}
						catch (NumberFormatException e) {
							parsedRows[row][outputColumn] = null; // TODO: drop the line if necessary
						}
					}
					else if ("java.lang.String".equals(type)) {
						try {
							parsedRows[row][outputColumn] = item;
						}
						catch (NumberFormatException e) {
							parsedRows[row][outputColumn] = null; // TODO: drop the line if necessary
						}
					}
					else if ("java.lang.Integer".equals(type)) {
						try {
							parsedRows[row][outputColumn] = Integer.parseInt(item);
						}
						catch (NumberFormatException e) {
							parsedRows[row][outputColumn] = null; // TODO: drop the line if necessary
						}
					}
					else if ("java.lang.Boolean".equals(type)) {
						Pattern pTrue = Pattern.compile("t|true|y|yes|1", Pattern.CASE_INSENSITIVE);
						// Pattern pFalse = Pattern.compile("f|false|n|no|0", Pattern.CASE_INSENSITIVE);
							Matcher mTrue = pTrue.matcher(item);
							// Matcher mFalse = pFalse.matcher(item);
							parsedRows[row][outputColumn] = mTrue.matches();
					}
					
				}
			}

			if (this.doDropNaNs) {
				// Drop entire row if any column is NaN, by setting the row to null
				for (int row = 0; row < numLines; row++) {
					for (int col = 0; col < this.columnIndices.length; col++) {
						if (parsedRows[row][col] == null) {
							parsedRows[row] = null; // Drop entire row
							break;
						}
						// System.out.println(">>> CTN: " + Arrays.toString(parsedRows[row]));
					}
				}
			}

			result = new Iterable<Object[]>() {

				@Override
				public Iterator<Object[]> iterator() {
					return new FastArrayIterator<Object[]>(parsedRows);
				}
			};

			return result;
		}
		
//		@Override
		public Iterable<Object[]> call2(Iterator<String> lines) throws Exception {

			Iterable<Object[]> result = null;

			// First convert the list of lines into an array of lines
			ArrayList<String[]> lines2 = new ArrayList<String[]>();
			String line;
			while (lines.hasNext()) {
				line = lines.next();
				lines2.add(line.split(separator));
			}

			int numLines = lines2.size();
			String[][] lines3 = lines2.toArray(new String[0][0]);

			// Now scan the partition, one column at a time
			MetaInfo[] metaInfoArray = broadcastMetaInfo.value();

			final Object[][] parsedRows = new Object[numLines][this.columnIndices.length];

			for (int outputColumn = 0; outputColumn < this.columnIndices.length; outputColumn++) {

				int inputColumn = this.columnIndices[outputColumn];

				// TODO: check bounds
				System.out.println(">>> CTN: inputColumn = " + inputColumn);

				String type = metaInfoArray[inputColumn].getType();

				if ("Unknown".equals(type)) {
					// Try to figure out the column type is, by trial-and-error
					if (numLines > 1) { // avoid trying to do this on a header line
						String testString = lines3[1][inputColumn];
						try {
							Integer.parseInt(testString);
							type = "java.lang.Integer";
						}
						catch (NumberFormatException e) {
							try {
								Double.parseDouble(testString);
								type = "java.lang.Double";
							}
							catch (NumberFormatException e1) {
								type = "java.lang.String";
							}
						}
					}
				}

				// Now for a given data type, walk through the data, one row at a time, and parse it

				String item; // a single [row,column] cell value

				if ("java.lang.String".equals(type)) {
					// Just copy the entire column as Strings
					for (int row = 0; row < numLines; row++) {
						parsedRows[row][outputColumn] = lines3[row][inputColumn];
					}
				}
				else if ("java.lang.Boolean".equals(type)) {
					Pattern pTrue = Pattern.compile("t|true|y|yes|1", Pattern.CASE_INSENSITIVE);
					// Pattern pFalse = Pattern.compile("f|false|n|no|0", Pattern.CASE_INSENSITIVE);

					for (int row = 0; row < numLines; row++) {
						item = lines3[row][inputColumn];
						Matcher mTrue = pTrue.matcher(item);
						// Matcher mFalse = pFalse.matcher(item);
						parsedRows[row][outputColumn] = mTrue.matches();
					}
				}
				else if ("java.lang.Integer".equals(type)) {
					for (int row = 0; row < numLines; row++) {
						item = lines3[row][inputColumn];

						try {
							parsedRows[row][outputColumn] = Integer.parseInt(item);
						}
						catch (NumberFormatException e) {
							try {
								parsedRows[row][outputColumn] = (int) Math.round(Double.parseDouble(item));
							}
							catch (NumberFormatException e1) {
								parsedRows[row][outputColumn] = null; // TODO: drop the line if necessary
							}
						}
					}
				}
				else if ("java.lang.Double".equals(type)) {
					for (int row = 0; row < numLines; row++) {
						item = lines3[row][inputColumn];

						try {
							parsedRows[row][outputColumn] = Double.parseDouble(item);
						}
						catch (NumberFormatException e) {
							parsedRows[row][outputColumn] = null; // TODO: drop the line if necessary
						}
					}
				}

			}

			if (this.doDropNaNs) {
				// Drop entire row if any column is NaN, by setting the row to null
				for (int row = 0; row < numLines; row++) {
					for (int col = 0; col < this.columnIndices.length; col++) {
						if (parsedRows[row][col] == null) {
							parsedRows[row] = null; // Drop entire row
							break;
						}
						// System.out.println(">>> CTN: " + Arrays.toString(parsedRows[row]));
					}
				}
			}

			result = new Iterable<Object[]>() {

				@Override
				public Iterator<Object[]> iterator() {
					return new FastArrayIterator<Object[]>(parsedRows);
				}
			};

			return result;
		}
	}

	static public class LoadTableResult extends SuccessResult {

		String dataContainerID;
		MetaInfo[] metaInfo;

		public String getDataContainerID() {
			return dataContainerID;
		}

		public LoadTableResult setDataContainerID(String dataContainerID) {
			this.dataContainerID = dataContainerID;
			return this;
		}

		public LoadTableResult setMetaInfo(MetaInfo[] metaInfo) {
			this.metaInfo = metaInfo.clone();
			return this;
		}

		public MetaInfo[] getMetaInfo() {
			return metaInfo;
		}
	}

	/**
	 * Given a String[] vector of data values along one column, try to infer what the data type should be.
	 * 
	 * TODO: precompile regex
	 * 
	 * @param vector
	 * @return string representing name of the type "integer", "double", "character", or "logical" The algorithm will
	 *         first scan the vector to detect whether the vector contains only digits, ',' and '.', <br>
	 *         if true, then it will detect whether the vector contains '.', <br>
	 *         &nbsp; &nbsp; if true then the vector is double else it is integer <br>
	 *         if false, then it will detect whether the vector contains only 'T' and 'F' <br>
	 *         &nbsp; &nbsp; if true then the vector is logical, otherwise it is characters
	 */
	public static String determineType(String[] vector, Boolean doPreferDouble) {
		boolean isNumber = true;
		boolean isInteger = true;
		boolean isLogical = true;
		boolean allNA = true;

		for (String s : vector) {
			if (s == null || s.startsWith("NA") || s.startsWith("Na") || s.matches("^\\s*$")) {
				// Ignore, don't set the type based on this
				continue;
			}

			allNA = false;

			if (isNumber) {
				// match numbers: 123,456.123 123 123,456 456.123 .123
				if (!s.matches("(^|^-)((\\d+(,\\d+)*)|(\\d*))\\.?\\d+$")) {
					isNumber = false;
				}
				// match double
				else if (isInteger && s.matches("(^|^-)\\d*\\.{1}\\d+$")) {
					isInteger = false;
				}
			}

			// NOTE: cannot use "else" because isNumber changed in the previous
			// if block
			if (isLogical) {
				if (!s.toLowerCase().matches("^t|f|true|false$")) {
					isLogical = false;
				}
			}
		}

		String result = "Unknown";

		if (!allNA) {
			if (isNumber) {
				if (!isInteger || doPreferDouble) {
					result = Double.class.getName();
				}
				else {
					result = Integer.class.getName();
				}
			}
			else {
				if (isLogical) {
					result = Boolean.class.getName();
				}
				else {
					result = String.class.getName();
				}
			}
		}

		// System.out.println(">>> CTN: got type " + result);

		return result;
	}

	/**
	 * TODO: check more than a few lines in case some lines have NA
	 * 
	 * @param fileRDD
	 * @return
	 */
	public MetaInfo[] getMetaInfo(JavaRDD<String> fileRDD) {
		String[] headers = null;

		// sanity check
		if (sampleSize < 1) {
			LOG.info("DATATYPE_SAMPLE_SIZE must be bigger than 1");
			return null;
		}

		List<String> sampleStr = fileRDD.take(sampleSize);
		sampleSize = sampleStr.size(); // actual sample size
		LOG.info("Sample size: " + sampleSize);

		// create sample list for getting data type
		String[] firstSplit = sampleStr.get(0).split(separator);

		// get header
		if (hasHeader) {
			headers = firstSplit;
		}
		else {
			headers = new String[firstSplit.length];
			for (int i = 0; i < headers.length;) {
				headers[i] = "V" + (++i);
			}
		}

		String[][] samples = hasHeader ? (new String[firstSplit.length][sampleSize - 1])
				: (new String[firstSplit.length][sampleSize]);

		MetaInfo[] metaInfoArray = new MetaInfo[firstSplit.length];
		int start = hasHeader ? 1 : 0;
		for (int j = start; j < sampleSize; j++) {
			firstSplit = sampleStr.get(j).split(separator);
			for (int i = 0; i < firstSplit.length; i++) {
				samples[i][j - start] = firstSplit[i];
			}
		}

		for (int i = 0; i < samples.length; i++) {
			String[] vector = samples[i];
			metaInfoArray[i] = new MetaInfo(headers[i], determineType(vector, doPreferDouble), i + 1 /* R column index is base-1 */);
		}

		return metaInfoArray;
	}

	public JavaRDD<Object[]> getDataFrame(JavaRDD<String> fileRDD, Broadcast<MetaInfo[]> broadcastMetaInfo) {

		// Update column list, if not defined by client
		if (this.columns == null) {
			MetaInfo[] metaInfo = broadcastMetaInfo.value();
			this.columns = new int[metaInfo.length];
			for (int i = 0; i < metaInfo.length; i++) {
				this.columns[i] = i + 1;
			}
		}

		JavaRDD<Object[]> res;

		if (hasHeader) {
			// res = fileRDD.map(new ParseLine(separator, broadcastMetaInfo, this.columns, this.doDropNaNs));
			res = fileRDD
					.mapPartitions(new ParsePartition(separator, broadcastMetaInfo, this.columns, this.doDropNaNs));
		}
		else {
			// res = fileRDD.map(new ParseLine(separator, broadcastMetaInfo, this.columns, this.doDropNaNs));
			res = fileRDD
					.mapPartitions(new ParsePartition(separator, broadcastMetaInfo, this.columns, this.doDropNaNs));
		}

		// long count = res.count();
		// System.out.println(">>> CTN: got this many rows: " + count);

		// This causes NotSerializableException for some reason. TODO: deal with this later
		// if (this.doDropNaNs) {
		// // Drop lines which have been nullified due to null/NaN column values
		// res = res.filter(new Function<Object[], Boolean>() {
		//
		// @Override
		// public Boolean call(Object[] columnData) {
		// return true;
		// }
		// });
		// }

		return res;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see adatao.bigr.spark.execution.Executor#run()
	 */
	@Override
	public ExecutorResult run(SparkThread sparkThread) {
		JavaSparkContext sc = sparkThread.getSparkContext();
		JavaRDD<String> fileRDD = sc.textFile(fileURL);

		// fileRDD.cache(); // to avoid reading from disk/page cache twice (once for metadata, once for dataframe)

		MetaInfo[] metaInfo = getMetaInfo(fileRDD);
		broadcastMetaInfo = sc.broadcast(metaInfo);

		JavaRDD<Object[]> dataFrameTable = getDataFrame(fileRDD, broadcastMetaInfo);

		String dataContainerID = sparkThread.getDataManager().add(new DataFrame(metaInfo, dataFrameTable));
		LOG.info("Meta info: " + Arrays.toString(metaInfo));

		return new LoadTableResult().setDataContainerID(dataContainerID).setMetaInfo(metaInfo);
	}

	@Override
	public String toString() {
		return "LoadTable [fileURL=" + fileURL + ", hasHeader=" + hasHeader + ", separator=" + separator + ", type="
				+ fileType + "]";
	}
}
