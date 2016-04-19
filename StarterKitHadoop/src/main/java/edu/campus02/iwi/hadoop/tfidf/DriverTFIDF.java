package edu.campus02.iwi.hadoop.tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.campus02.iwi.hadoop.env.WinConfig;

public class DriverTFIDF extends Configured implements Tool  {
	
	
	//TODO: Implement your Mapper and Reducer classes here...
	//since you are using three MRjobs you need to implement
	//1 mapper & 1 reducer for each of the three MRjobs
	
	public int run(String[] args) throws Exception {
		
		//TODO: configure & setup all 3 jobs needed 
        //in order to calculate the TFIDF values for all the
        //inputs documents => see commented lines below
		
		//Getting the number of documents for the whole corpus
		//(i.e. all files from the input path)
		FileSystem fs = FileSystem.get(getConf());
		FileStatus[] userFilesStatusList = fs.listStatus(new Path(args[0]));
        final int numCorpusFiles = userFilesStatusList.length;
        String[] fileNames = new String[numCorpusFiles];
        for (int i = 0; i < numCorpusFiles; i++) {
            fileNames[i] = userFilesStatusList[i].getPath().getName();
            System.out.println((i+1)+ ". document: "+fileNames[i]);
        }

		Job job1 = Job.getInstance(getConf(), "Word Frequencies Per Doc");
		job1.setJarByClass(getClass());
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		Path outpath = new Path(args[1]+"/p1");
		FileOutputFormat.setOutputPath(job1, outpath);
		outpath.getFileSystem(getConf()).delete(outpath, true);
		
		//TODO: Set your implemented Mapper and Reducer classes for 1st job here...
		//job1.setMapperClass(XXX.class);
		//job1.setReducerClass(YYY.class);

		//TODO: configure Map Output Key/Value classes
		//job1.setMapOutputKeyClass(KEY.class);
		//job1.setMapOutputValueClass(VALUE.class);
		
		job1.setNumReduceTasks(Integer.parseInt(args[2]));
		
		//TODO: configure Reduce Output Key/Value classes
		//job1.setOutputKeyClass(KEY.class);
		//job1.setOutputValueClass(VALUE.class);
		
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		//NOTE: for testing uncomment this line to stop after the 1st MRjob
		//return job1.waitForCompletion(true) ? 0 : -1;
		
		if(job1.waitForCompletion(true)) {
			Job job2 = Job.getInstance(getConf(), "Total Words Per Doc");
			job2.setJarByClass(getClass());
			FileInputFormat.addInputPath(job2, new Path(args[1]+"/p1"));
			outpath = new Path(args[1]+"/p2");
			FileOutputFormat.setOutputPath(job2, outpath);
			outpath.getFileSystem(getConf()).delete(outpath, true);
			
			//TODO: Set your implemented Mapper and Reducer classes for 2nd job here...
			//job2.setMapperClass(XXX.class);
			//job2.setReducerClass(YYY.class);
			
			//TODO: configure Map Output Key/Value classes
			//job2.setMapOutputKeyClass(KEY.class);
			//job2.setMapOutputValueClass(VALUE.class);
			
			job2.setNumReduceTasks(Integer.parseInt(args[2]));
			
			//TODO: configure Reduce Output Key/Value classes
			//job2.setOutputKeyClass(KEY.class);
			//job2.setOutputValueClass(VALUE.class);
			
			job2.setInputFormatClass(TextInputFormat.class);
			job2.setOutputFormatClass(TextOutputFormat.class);
			
			//NOTE: for testing uncomment this line to stop after the 2nd MRjob
			//return job2.waitForCompletion(true) ? 0 : -1;
			
			if(job2.waitForCompletion(true)) {
				Configuration conf = getConf();
				//NOTE: this stores the number of corpus files within job conf
				//so that it is accessible within a job for Mapper or Reducer instances 
				conf.setInt("numCorpusFiles", numCorpusFiles);
				Job job3 = Job.getInstance(conf, "final TFIDF calculation");
				job3.setJarByClass(getClass());
				FileInputFormat.addInputPath(job3, new Path(args[1]+"/p2"));
				outpath = new Path(args[1]+"/p3");
				FileOutputFormat.setOutputPath(job3, outpath);
				outpath.getFileSystem(getConf()).delete(outpath, true);
				
				//TODO: Set your implemented Mapper and Reducer classes for 3rd job here...
				//job3.setMapperClass(XXX.class);
				//job3.setReducerClass(YYY.class);
				
				//TODO: configure Map Output Key/Value classes
				//job3.setMapOutputKeyClass(KEY.class);
				//job3.setMapOutputValueClass(VALUE.class);
				
				job3.setNumReduceTasks(Integer.parseInt(args[2]));
				
				//TODO: configure Reduce Output Key/Value classes
				//job3.setOutputKeyClass(KEY.class);
				//job3.setOutputValueClass(VALUE.class);
				
				job3.setInputFormatClass(TextInputFormat.class);
				job3.setOutputFormatClass(TextOutputFormat.class);
					
				return job3.waitForCompletion(true) ? 0 : -1;
			}
		}
		return -1;
	}
	
	public static void main(String[] args) throws Exception {
		
		WinConfig.setupEnv();
		
		if (args.length != 3) {
			System.err.printf("Usage: %s [generic options] <inputDir> <outputDir> <numReducers>\n",
					DriverTFIDF.class.getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			System.exit(-1);
		}
		
		int exitCode = ToolRunner.run(new DriverTFIDF(), args);
		System.exit(exitCode);
	}

}
