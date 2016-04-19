package edu.campus02.iwi.hadoop.warmup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.campus02.iwi.hadoop.env.WinConfig;


public class BigramDriver extends Configured implements Tool {

	//TODO: Implement your Mapper and Reducer classes here...

	public int run(String[] args) throws Exception {
		
		Configuration  conf = new Configuration();
		Job job = Job.getInstance(conf,"bigram job");
		job.setJarByClass(BigramDriver.class);
		
		//TODO: Set your implemented Mapper and Reducer classes here...
		//job.setMapperClass(XXX.class);
		//job.setReducerClass(YYY.class);
		
		//TODO: configure Map Output Key/Value classes
		//job.setMapOutputKeyClass(KEY.class);
		//job.setMapOutputValueClass(VALUE.class);
				
		//TODO: configure Reduce Output Key/Value classes
		//job.setOutputKeyClass(KEY.class);
		//job.setOutputValueClass(VALUE.class);

		job.setNumReduceTasks(1);
		
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
