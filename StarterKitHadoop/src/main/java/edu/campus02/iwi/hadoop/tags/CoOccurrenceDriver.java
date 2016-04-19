package edu.campus02.iwi.hadoop.tags;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.campus02.iwi.hadoop.env.WinConfig;

public class CoOccurrenceDriver extends Configured implements Tool {

	//TODO: Implement your Mapper and Reducer classes here...
	//since you are using two MRjobs you need to implement
	//1 mapper & 1 reducer for each of the two MRjobs

	public int run(String[] args) throws Exception {
		
		//NOTE: This problem needs two MapReduce Jobs
		//the output of the 1st MRjob being input to the 2nd MRjob
		
		Job job1 = Job.getInstance(getConf());
		job1.setJarByClass(getClass());
		
		//TODO: Set your implemented Mapper and Reducer classes for 1st job here...
		//your implementation might heavily benefit by setting a combiner class 
		//job1.setMapperClass(XXX.class);
		//job1.setReducerClass(YYY.class);
		//job1.setCombinerClass(YYY.class);
		
		job1.setNumReduceTasks(1);
		
		//TODO: configure Map Output Key/Value classes
		//job1.setMapOutputKeyClass(KEY.class);
		//job1.setMapOutputValueClass(VALUE.class);
						
		//TODO: configure Reduce Output Key/Value classes
		//job1.setOutputKeyClass(KEY.class);
		//job1.setOutputValueClass(VALUE.class);
		
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		Path outP1 = new Path(args[1]+"/job1");
		outP1.getFileSystem(getConf()).delete(outP1,true);
		FileOutputFormat.setOutputPath(job1, outP1);
		
		if(job1.waitForCompletion(true)) {
			Job job2 = Job.getInstance(getConf());;
			job2.setJarByClass(getClass());
			
			//TODO: Set your implemented Mapper and Reducer classes for 2nd job here...
			//job2.setMapperClass(XXX.class);
			//job2.setReducerClass(YYY.class);
			
			job2.setNumReduceTasks(1);
			
			//TODO: configure Map Output Key/Value classes
			//job2.setMapOutputKeyClass(KEY.class);
			//job2.setMapOutputValueClass(VALUE.class);
			
			job2.setInputFormatClass(KeyValueTextInputFormat.class);
			job2.setOutputFormatClass(TextOutputFormat.class);
			
			FileInputFormat.addInputPath(job2, outP1);
			Path outP2 = new Path(args[1]+"/job2");
			outP2.getFileSystem(getConf()).delete(outP2,true);
			FileOutputFormat.setOutputPath(job2, outP2);
			return job2.waitForCompletion(true) ? 0 : -1;
		}
		return -1;
	}

	public static void main(String[] args) throws Exception {
		
		WinConfig.setupEnv();
		
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <inputFile> <outputDir>\n",
					CoOccurrenceDriver.class.getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			System.exit(-1);
		}
		
		int exitCode = ToolRunner.run(new CoOccurrenceDriver(), args);
		System.exit(exitCode);
	}
}