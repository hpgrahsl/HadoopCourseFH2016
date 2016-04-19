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


public class FilterDriver extends Configured implements Tool {

	//TODO: Implement your Mapper and/or Reducer classes here...
	
	public int run(String[] args) throws Exception {
		
		Configuration  conf = new Configuration();
		
		Job job = Job.getInstance(conf,"filter job");
		job.setJarByClass(FilterDriver.class);
		
		//TODO: Set your implemented Mapper and/or Reducer classes here...
		//job.setMapperClass(XXX.class);
		//job.setReducerClass(YYY.class);
		
		//TODO: configure Map and/or Reduce Output Key/Value classes
		//job.setMapOutputKeyClass(KEY.class);
		//job.setMapOutputValueClass(VALUE.class);
		//job.setOutputKeyClass(KEY.class);
		//job.setOutputValueClass(VALUE.class);
		
		//TODO: what number to set numReduceTasks?
		//job.setNumReduceTasks(?);
		
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
					FilterDriver.class.getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			System.exit(-1);
		}
		
		int exitCode = ToolRunner.run(new FilterDriver(), args);
		System.exit(exitCode);
	}

}
