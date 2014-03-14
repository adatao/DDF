package adatao.bigr.spark.execution;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.Gson;

import adatao.bigr.spark.DataManager;
import adatao.bigr.spark.SparkThread;
import adatao.bigr.spark.DataManager.*;
import adatao.bigr.thrift.generated.JsonResult;

import scala.actors.threadpool.Arrays;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

public class LoadTable implements Executor {
	private String fileURL;
	private boolean hasHeader = false;
	private String separator = " ";
	private String fileType;
	private DataManager dataManager;
	int sampleSize = 2;
	Broadcast<MetaInfo[]> broadcastMetaInfo;

	public static Logger LOG = LoggerFactory.getLogger(LoadTable.class);

	static class ParseLine extends Function<String, Object[]> {
		private static final long serialVersionUID = 8381483852970973442L;
		String separator;
		Broadcast<MetaInfo[]> broadcastMetaInfo;

		public ParseLine(String separator,
				Broadcast<MetaInfo[]> broadcastMetaInfo) {
			this.separator = separator;
			this.broadcastMetaInfo = broadcastMetaInfo;
		}

		@SuppressWarnings("unchecked")
		public Object[] call(String line) throws SecurityException,
				NoSuchMethodException, IllegalArgumentException,
				IllegalAccessException, InvocationTargetException,
				InstantiationException, ClassNotFoundException {
			MetaInfo[] metaInfoArray = broadcastMetaInfo.value();
			String[] splits = line.split(separator);
			Object[] newSplits = new Object[splits.length];

			for (int i = 0; i < splits.length; i++) {
				String type = metaInfoArray[i].getType();
				if (splits[i].equals("NA")) {
					newSplits[i] = null;
				} else if (type.equals("Unknown")) {
					newSplits[i] = null;

				} else if (type.equals("java.lang.String")) {
					newSplits[i] = splits[i];
				} else if (type.equals("java.lang.Boolean")) {
					if (splits[i].toLowerCase().matches("^t|true$")) {
						newSplits[i] = true;
					} else if (splits[i].toLowerCase().matches("^f|false$")) {
						newSplits[i] = false;
					} else {
						newSplits[i] = null;
					}
				} else if (type.equals("java.lang.Integer")) {
					try {
						newSplits[i] = java.lang.Integer.parseInt(splits[i]);
					} catch (NumberFormatException e) {
						newSplits[i] = null;
					}
				} else if (type.equals("java.lang.Double")) {
					try {
						newSplits[i] = java.lang.Double.parseDouble(splits[i]);
					} catch (NumberFormatException e) {
						newSplits[i] = null;
					}
				}
			}
			return newSplits;
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
			this.metaInfo = metaInfo;
			return this;
		}

		public MetaInfo[] getMetaInfo() {
			return metaInfo;
		}
	}

	/**
	 * @param vector
	 * @return string representing name of the type "interger", "double",
	 *         "character", or "logical" The algorithm will first scan the
	 *         vector to detect whether the vector contains only digits, ',' and
	 *         '.', <br>
	 *         if true, then it will detect whether the vector contains '.', <br>
	 *         &nbsp; &nbsp; if true then the vector is double else it is
	 *         integer <br>
	 *         if false, then it will detect whether the vector contains only
	 *         'T' and 'F' <br>
	 *         &nbsp; &nbsp; if true then the vector is logical, otherwise it is
	 *         characters
	 */
	private String getType(String[] vector) {
		boolean isNumber = true;
		boolean isInteger = true;
		boolean isLogical = true;
		boolean allNA = true;

		for (String s : vector) {
			if (s == null || s.equals("NA") || s.matches("^\\s*$")) {
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

		if (allNA) {
			return "Unknown";
		}

		if (isNumber) {
			if (isInteger) {
				return Integer.class.getName();
			} else {
				return Double.class.getName();
			}
		} else {
			if (isLogical) {
				return Boolean.class.getName();
			} else {
				return String.class.getName();
			}
		}
	}

	@SuppressWarnings("unused")
	private MetaInfo[] getMetaInfo(JavaRDD<String> fileRDD) {
		String[] headers = null;

		// get header
		String[] firstSplit = fileRDD.first().split(separator);
		if (hasHeader) {
			headers = firstSplit;
		} else {
			headers = new String[firstSplit.length];
			for (int i = 0; i < headers.length; i++) {
				headers[i] = "V" + (i + 1);
			}
		}

		// get column data type
		// sanity check
		if (sampleSize < 1) {
			LOG.info("DATATYPE_SAMPLE_SIZE must be bigger than 1");
			return null;
		}

		List<String> sampleStr;
		sampleStr = fileRDD.take(sampleSize);
		// reassign sampleSize when sampleStr is smaller
		sampleSize = sampleStr.size() < sampleSize ? sampleStr.size()
				: sampleSize;

		LOG.info("Sample size: " + sampleSize);
		// create sample list for getting data type
		String[] splits = sampleStr.get(0).split(separator);
		String[][] samples = hasHeader ? (new String[splits.length][sampleSize - 1])
				: (new String[splits.length][sampleSize]);

		MetaInfo[] metaInfoArray = new MetaInfo[splits.length];
		int start = hasHeader ? 1 : 0;
		for (int j = start; j < sampleSize; j++) {
			splits = sampleStr.get(j).split(separator);
			for (int i = 0; i < splits.length; i++) {
				samples[i][j - start] = splits[i];
			}
		}

		for (int i = 0; i < samples.length; i++) {
			String[] vector = samples[i];
			metaInfoArray[i] = new MetaInfo().setHeader(headers[i]);
			metaInfoArray[i].setType(getType(vector));
		}

		return metaInfoArray;
	}

	private JavaRDD<Object[]> getDataFrame(JavaRDD<String> fileRDD) {
		if (hasHeader) {
			return fileRDD.map(new ParseLine(separator, broadcastMetaInfo))
					.cache();
		} else {
			return fileRDD.map(new ParseLine(separator, broadcastMetaInfo))
					.cache();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see adatao.bigr.rclient.execution.Executor#run()
	 */
	@Override
	public ExecutorResult run(SparkThread sparkThread) {
		JavaSparkContext sc = sparkThread.getSparkContext();
		JavaRDD<String> fileRDD = sc.textFile(fileURL);

		MetaInfo[] metaInfo = getMetaInfo(fileRDD);
		broadcastMetaInfo = sc.broadcast(metaInfo);

		JavaRDD<Object[]> dataFrame = getDataFrame(fileRDD);

		String dataContainerID = sparkThread.getDataManager().add(
				new DataFrame(metaInfo, dataFrame));
		LOG.info("Meta info: " + Arrays.toString(metaInfo));

		return new LoadTableResult().setDataContainerID(dataContainerID)
				.setMetaInfo(metaInfo);
	}

	public LoadTable setFileURL(String fileURL) {
		this.fileURL = fileURL;
		return this;
	}

	public LoadTable setHasHeader(boolean hasHeader) {
		this.hasHeader = hasHeader;
		return this;
	}

	public Executor setSeparator(String separator) {
		this.separator = separator;
		return this;
	}

	public void setSparkThread(SparkThread sparkThread) {
		// this.sparkThread = sparkThread;
	}

	@Override
	public String toString() {
		return "LoadTable [fileURL=" + fileURL + ", hasHeader=" + hasHeader
				+ ", separator=" + separator + ", type=" + fileType + "]";
	}

	public int getSampleSize() {
		return sampleSize;
	}

	public void setSampleSize(int sampleSize) {
		this.sampleSize = sampleSize;
	}
}
