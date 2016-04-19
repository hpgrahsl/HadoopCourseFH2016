package edu.campus02.iwi.hadoop.twitter;

import java.util.UUID;

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

public class DriverTweetFeed extends Configured implements Tool  {
	
	//TODO: Implement your Mapper and Reducer classes here...
			
	public int run(String[] args) throws Exception {
		
		if (args.length < 2) {
			System.err.printf("Usage: %s [generic options] <inputDir> <outputDir>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
        
		Job job1 = Job.getInstance(getConf());
		job1.setJarByClass(getClass());
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		Path outpath = new Path(args[1]+"/"+UUID.randomUUID().toString());
		FileOutputFormat.setOutputPath(job1, outpath);
		outpath.getFileSystem(getConf()).delete(outpath, true);
		
		//TODO: Set your implemented Mapper and Reducer classes here...
		//job1.setMapperClass(XXX.class);
		//job1.setReducerClass(YYY.class);
		
		//TODO: configure Map Output Key/Value classes
		//job1.setMapOutputKeyClass(KEY.class);
		//job1.setMapOutputValueClass(VALUE.class);
				
		//TODO: configure Reduce Output Key/Value classes
		//job1.setOutputKeyClass(KEY.class);
		//job1.setOutputValueClass(VALUE.class);
		
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		return job1.waitForCompletion(true) ? 0 : -1;
	}
	
	public static void main(String[] args) throws Exception {
		
		WinConfig.setupEnv();
		
		int exitCode = ToolRunner.run(new DriverTweetFeed(), args);
		System.exit(exitCode);
	}

}
