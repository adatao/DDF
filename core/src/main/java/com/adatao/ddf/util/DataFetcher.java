package com.adatao.ddf.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

enum FetchCounter {
  ERROR_FETCHING_FILE, FETCHED_FILE
}

/**
 * To fetch data from S3 to hdfs
 * 
 * @author bhan
 */

public class DataFetcher {
  public static final Logger LOG = LoggerFactory.getLogger(DataFetcher.class);

  public class DataFetchMapper implements Mapper<Text, Text, Text, Text> {

    private Configuration conf;
    private int bufferSize;
    private Path outputDir;

    private long copyStream(InputStream inputStream, OutputStream outputStream,
        Reporter reporter) throws IOException {
      long bytesCopied = 0;
      try {
        int len;
        byte[] buffer = new byte[bufferSize];
        while ((len = inputStream.read(buffer)) > 0) {
          outputStream.write(buffer, 0, len);
          reporter.progress();
          bytesCopied += len;
        }
      } catch (Exception e) {
        throw new IOException("Exception raised while copying data file", e);
      }
      return bytesCopied;
    }

    @Override
    public void map(Text key, Text bucket,
        OutputCollector<Text, Text> collector, Reporter reporter)
        throws IOException {
      Path inputPath = new Path("s3://" + bucket + "/" + key);
      Path outputPath = new Path(outputDir, key.toString().replace("/", "_")
          .replace(":", "_").replace("%", "_"));
      FileSystem inFs = FileSystem.get(inputPath.toUri(), conf);
      FileSystem outFs = FileSystem.get(outputPath.toUri(), conf);
      InputStream inStream = inFs.open(inputPath);
      OutputStream outStream = outFs.create(outputPath);
      for (int i = 0; i < 10; i++) {
        try {
          copyStream(inStream, outStream, reporter);
          inStream.close();
          outStream.close();
          reporter.getCounter(FetchCounter.FETCHED_FILE).increment(1);
          return;
        } catch (Exception e) {
          LOG.error("Failed to download file '" + inputPath.toString() + "'"
              + " to '" + outputPath.toString() + "'", e);
        }
      }
      LOG.error("Failed to download file '" + inputPath.toString() + "'"
          + " to '" + outputPath.toString() + "'");
      reporter.getCounter(FetchCounter.ERROR_FETCHING_FILE).increment(1);
      collector.collect(key, bucket);
    }

    @Override
    public void configure(JobConf jobConf) {
      conf = jobConf;
      bufferSize = 4 * 1024 * 1024;
      outputDir = new Path(jobConf.get("datafetcher.outputpath"));
    }

    @Override
    public void close() throws IOException {
    }
  }

  /**
   * hadoop jar DataFetcher.jar
   * s3://https://s3.amazonaws.com/adatao-sample-data/ hdfs:///temp
   * hdfs:///test-data accessId secretKey
   */

  public static void main(String[] args) throws IOException {
    JobConf conf = new JobConf();
    conf.setJobName("Downloading from " + args[0] + " to " + args[2]);
    conf.set("fs.s3n.awsAccessKeyId", args[3]);
    conf.set("fs.s3n.awsSecretAccessKey", args[4]);
    conf.set("fs.s3.awsAccessKeyId", args[3]);
    conf.set("fs.s3.awsSecretAccessKey", args[4]);
    conf.set("fs.s3bfs.awsAccessKeyId", args[3]);
    conf.set("fs.s3bfs.awsSecretAccessKey", args[4]);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    conf.setMapperClass(DataFetchMapper.class);
    conf.setInputFormat(KeyValueTextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
    conf.set("key.value.separator.in.input.line", "\t");
    conf.setJarByClass(DataFetchMapper.class);
    conf.setNumReduceTasks(0);
    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    conf.set("datafetcher.outputpath", args[2]);
    JobClient.runJob(conf);
  }
}
