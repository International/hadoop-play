import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class LetterCounter {
	public static class LetterMapper extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter arg3)
				throws IOException {
			String word = value.toString();
			System.out.println("map called with:" + word);
			for(char c : word.toCharArray()) {
				System.out.println("output collecting:" + c + " with 1");
				output.collect(new Text(String.valueOf(c)), new IntWritable(1));
			}
		}
		
	}
	
	public static class LetterReducer extends MapReduceBase implements 
	   Reducer< Text, IntWritable, Text, IntWritable > {

		@Override
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter arg3)
				throws IOException {
			// TODO Auto-generated method stub
			int counter = 0;
			while(values.hasNext()) {
				IntWritable value = values.next();
				System.out.println("value is:"+ value.get());
				counter += value.get();
			}
			System.out.println("reducing:" + key.toString() + " with counter:" + counter);
			output.collect(key, new IntWritable(counter));
		}  
		
	}
	
	public static void main(String args[])throws Exception 
	   { 
	      JobConf conf = new JobConf(LetterCounter.class); 
	      
	      conf.setJobName("letter_counter"); 
	      conf.setOutputKeyClass(Text.class);
	      conf.setOutputValueClass(IntWritable.class); 
	      conf.setMapperClass(LetterMapper.class); 
	      conf.setCombinerClass(LetterReducer.class); 
	      conf.setReducerClass(LetterReducer.class); 
	      conf.setInputFormat(TextInputFormat.class); 
	      conf.setOutputFormat(TextOutputFormat.class); 
	      
	      FileInputFormat.setInputPaths(conf, new Path(args[0])); 
	      FileOutputFormat.setOutputPath(conf, new Path(args[1])); 
	      
	      JobClient.runJob(conf); 
	   }
}
