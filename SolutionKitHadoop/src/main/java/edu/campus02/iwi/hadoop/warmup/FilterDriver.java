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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class FilterDriver extends Configured implements Tool {

	public final static String CONFIG_PARAM_FILTER = "WORD_TO_FILTER";
	
	public static class FilterMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

		private String wordToFilter;
		private Text outKey = new Text();
		
		@Override
		public void setup(Context context) {
			wordToFilter = context.getConfiguration().get(CONFIG_PARAM_FILTER);
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			if(value.toString().contains(wordToFilter)) {
				outKey.set(value.toString().trim());
				context.write(outKey, NullWritable.get());
			}
			
		}
	}

	public int run(String[] args) throws Exception {
		
		Configuration  conf = new Configuration();
		
		//set word_to_filter for as config parameter
		conf.set(CONFIG_PARAM_FILTER, args[2]);
		
		Job job = Job.getInstance(conf,"filter job");
		job.setJarByClass(FilterDriver.class);
		
		job.setMapperClass(FilterMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//OUR FILTER JOB DOES NOT NEED ANY REDUCER AT ALL
		job.setNumReduceTasks(0);
		
		FileInputFormat.addInputPath(job,new Path(args[0]));
		Path outputPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		return job.waitForCompletion(true) ? 0 : -1;
		
	}

	public static void main(String[] args) throws Exception {
		
		if (args.length != 3) {
			System.err.printf("Usage: %s [generic options] <inputDir> <outputDir> <word_to_filter>\n",
					FilterDriver.class.getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			System.exit(-1);
		}
		
		int exitCode = ToolRunner.run(new FilterDriver(), args);
		System.exit(exitCode);
	}

}
