package patent_citations;

import java.io.IOException;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
public class InverterTool extends Configured implements Tool {

  public static class InvertMapper extends
      Mapper<Object, Text, LongWritable, LongWritable> {
    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      String workingOn  = value.toString();
      if(!workingOn.contains("CITING")) {
        String[] words    = workingOn.split(",");
        long firstPatent  = Long.parseLong(words[0]);
        long secondPatent = Long.parseLong(words[1]);
        context.write(new LongWritable(secondPatent), new LongWritable(firstPatent));     
      }
    }
  }
  
  public static class InvertReducer extends Reducer<LongWritable,LongWritable, LongWritable, LongWritable> {
    public void reduce(LongWritable key, Iterable<LongWritable> values,
        Context context
        ) throws IOException, InterruptedException {
    }
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new InverterTool(), args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {
    // When implementing tool
    Configuration conf = this.getConf();

    // Create job
    Job job = new Job(conf, "Tool Job");
    job.setJarByClass(InverterTool.class);

    // Setup MapReduce job
    // Do not specify the number of Reducer
    job.setMapperClass(InvertMapper.class);
    job.setReducerClass(InvertReducer.class);

    // Specify key / value
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);

    // Input
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    job.setInputFormatClass(TextInputFormat.class);

    // Output
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setOutputFormatClass(TextOutputFormat.class);

    // Execute job and return status
    return job.waitForCompletion(true) ? 0 : 1;
  }

}
