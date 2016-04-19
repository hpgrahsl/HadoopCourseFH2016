package edu.campus02.iwi.hadoop.warmup;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class BigramDriver extends Configured implements Tool {

	public static class BGMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		private Text outKey = new Text();
		private IntWritable outValue = new IntWritable(1);
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] tokens = value.toString().trim().split("\\s+");
			if(tokens.length >= 2) {
				for(int s=0; s < tokens.length-1; s++) {
					outKey.set(tokens[s].concat(" ").concat(tokens[s+1]));
					context.write(outKey, outValue);
				}
			}
			
		}
	}

	public static class BGReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		private Text bigram = new Text();
		private IntWritable max = new IntWritable(0);
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			//summing up bigram counts
			int sum = 0;
			for(IntWritable t:values) {
				sum+=t.get();
			}
			//keep reference to most common bigram seen so far
			if(sum > max.get()) {
				max.set(sum);
				bigram.set(key.toString());
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
	         context.write(bigram, max);
	    }
		
	}

	public int run(String[] args) throws Exception {
		
		Configuration  conf = new Configuration();
		Job job = Job.getInstance(conf,"bigram job");
		job.setJarByClass(BigramDriver.class);
		
		job.setMapperClass(BGMapper.class);
		job.setReducerClass(BGReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setNumReduceTasks(1);
		
		FileInputFormat.addInputPath(job,new Path(args[0]));
		Path outputPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		return job.waitForCompletion(true) ? 0 : -1;
		
	}

	public static void main(String[] args) throws Exception {
		
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <inputDir> <outputDir>\n",
					BigramDriver.class.getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			System.exit(-1);
		}
		
		int exitCode = ToolRunner.run(new BigramDriver(), args);
		System.exit(exitCode);
	}

}
