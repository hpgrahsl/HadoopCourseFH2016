package edu.campus02.iwi.hadoop.warmup;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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


public class AllCapsDriver extends Configured implements Tool {

	public static class CapsMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

		private Text outValue = new Text();
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			outValue.set(value.toString().toUpperCase());
			context.write(key, outValue);
		}
	}

	public static class CapsReducer extends Reducer<LongWritable, Text, Text, NullWritable> {

		public void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			for(Text t:values) {
				context.write(t, NullWritable.get());
			}
			
		}
	}

	public int run(String[] args) throws Exception {
		
		Configuration  conf = new Configuration();
		Job job = Job.getInstance(conf,"capitalizer job");
		job.setJarByClass(AllCapsDriver.class);
		
		job.setMapperClass(CapsMapper.class);
		//job.setCombinerClass(CapsReducer.class);
		job.setReducerClass(CapsReducer.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

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
					AllCapsDriver.class.getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			System.exit(-1);
		}
		
		int exitCode = ToolRunner.run(new AllCapsDriver(), args);
		System.exit(exitCode);
	}

}
