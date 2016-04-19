package edu.campus02.iwi.hadoop.warmup;

import java.io.IOException;
import java.util.StringTokenizer;

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

import edu.campus02.iwi.hadoop.env.WinConfig;

public class WordCount extends Configured implements Tool {

	public static class MapperWC extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final IntWritable ONE = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, ONE);
			}
		}
	}

	public static class ReducerWC extends Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable outValue = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable value:values) {
				sum += value.get();
			}
			outValue.set(sum);
			context.write(key, outValue);
		}
	}

	public int run(String[] args) throws Exception {
		
		Configuration  conf = new Configuration();
		Job job = Job.getInstance(conf,"word count example");
		job.setJarByClass(WordCount.class);
		
		job.setMapperClass(MapperWC.class);
		
		if(Boolean.valueOf(args[2])) {
			job.setCombinerClass(ReducerWC.class);
		}
		
		job.setReducerClass(ReducerWC.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job,new Path(args[0]));
		Path outputPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		return job.waitForCompletion(true) ? 0 : -1;
	}

	public static void main(String[] args) throws Exception {
		
		WinConfig.setupEnv();
		
		if (args.length != 3) {
			System.err.printf("Usage: %s [generic options] <inputDir> <outputDir> <withCombiner>\n",
					WordCount.class.getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			System.exit(-1);
		}
		
		int exitCode = ToolRunner.run(new WordCount(), args);
		System.exit(exitCode);
	}

}