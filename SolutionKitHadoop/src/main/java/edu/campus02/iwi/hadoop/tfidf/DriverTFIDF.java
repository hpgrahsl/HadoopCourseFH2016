package edu.campus02.iwi.hadoop.tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DriverTFIDF extends Configured implements Tool  {
	
	public int run(String[] args) throws Exception {
		
		if (args.length != 3) {
			System.err.printf("Usage: %s [generic options] <inputDir> <outputDir> <numReducers>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		
		//Getting the number of documents for the whole corpus
		//(i.e. all files from the input path)
		FileSystem fs = FileSystem.get(getConf());
		FileStatus[] userFilesStatusList = fs.listStatus(new Path(args[0]));
        final int numCorpusFiles = userFilesStatusList.length;
        String[] fileNames = new String[numCorpusFiles];
        for (int i = 0; i < numCorpusFiles; i++) {
            fileNames[i] = userFilesStatusList[i].getPath().getName();
            System.out.println((i+1)+ ". Dokument: "+fileNames[i]);
        }

        //TODO: implement the Mapper & Reducer classes for all
        //3 jobs needed to calculate the TFIDF values within the
        //inputs documents => see commented lines below
        
		Job job1 = Job.getInstance(getConf(), "Word Frequencies Per Doc");
		job1.setJarByClass(getClass());
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		Path outpath = new Path(args[1]+"/p1");
		FileOutputFormat.setOutputPath(job1, outpath);
		outpath.getFileSystem(getConf()).delete(outpath, true);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		job1.setMapperClass(WordDocFreqs.Map.class);
		//job1.setCombinerClass(XXX.class);
		job1.setReducerClass(WordDocFreqs.Reduce.class);
		
		job1.setNumReduceTasks(Integer.parseInt(args[2]));
		job1.setOutputKeyClass(TextTextTuple.class);
		job1.setOutputValueClass(LongWritable.class);

		//return job1.waitForCompletion(true) ? 0 : -1;
		
		if(job1.waitForCompletion(true)) {
			Job job2 = Job.getInstance(getConf(), "Total Words Per Doc");
			job2.setJarByClass(getClass());
			FileInputFormat.addInputPath(job2, new Path(args[1]+"/p1"));
			outpath = new Path(args[1]+"/p2");
			FileOutputFormat.setOutputPath(job2, outpath);
			outpath.getFileSystem(getConf()).delete(outpath, true);
			job2.setInputFormatClass(TextInputFormat.class);
			job2.setOutputFormatClass(TextOutputFormat.class);
			
			job2.setMapperClass(WordCountPerDoc.Map.class);
			job2.setReducerClass(WordCountPerDoc.Reduce.class);
			
			job2.setNumReduceTasks(Integer.parseInt(args[2]));
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(TextLongTuple.class);
			
			if(job2.waitForCompletion(true)) {
				Configuration conf = getConf();
				//NOTE: this stores the number of corpus files within job conf
				//so that it is accessible within map reduce job
				conf.setInt("numCorpusFiles", numCorpusFiles);
				Job job3 = Job.getInstance(conf, "final TFIDF calculation");
				job3.setJarByClass(getClass());
				FileInputFormat.addInputPath(job3, new Path(args[1]+"/p2"));
				outpath = new Path(args[1]+"/p3");
				FileOutputFormat.setOutputPath(job3, outpath);
				outpath.getFileSystem(getConf()).delete(outpath, true);
				job3.setInputFormatClass(TextInputFormat.class);
				job3.setOutputFormatClass(TextOutputFormat.class);
				
				job3.setMapperClass(CalcTFIDF.Map.class);
				job3.setReducerClass(CalcTFIDF.Reduce.class);
				
				job3.setNumReduceTasks(Integer.parseInt(args[2]));
				job3.setOutputKeyClass(Text.class);
				job3.setOutputValueClass(Text.class);
				return job3.waitForCompletion(true) ? 0 : -1;
			}
		}
		return -1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new DriverTFIDF(), args);
		System.exit(exitCode);
	}

}
