package edu.campus02.iwi.hadoop.tags;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CoOccurrenceDriver extends Configured implements Tool {

	public static class TagPairMapper extends Mapper<LongWritable, Text, TextTextTuple, IntWritable> {

		private TextTextTuple outTagPair = new TextTextTuple();
		private Text tag1 = new Text();
		private Text tag2 = new Text();
		private IntWritable outOne = new IntWritable(1);

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] urlTags = value.toString().split("\t"); 
			//NOTE: skipping the first entry (index 0) which is the url itself
			for(int t1=1;t1<urlTags.length-1;t1++) {
				for(int t2=t1+1;t2<urlTags.length;t2++) {
					tag1.set(urlTags[t1]);
					tag2.set(urlTags[t2]);
					//emit both tag pairs (tagA-tagB,tagB-tagA) because order is irrelevant 
					outTagPair.set(tag1, tag2);
					context.write(outTagPair, outOne);
					outTagPair.set(tag2, tag1);
					context.write(outTagPair, outOne);
				}
			}
		}
	}

	public static class TagPairReducer extends Reducer<TextTextTuple, IntWritable, TextTextTuple, IntWritable> {

		private IntWritable outSum = new IntWritable(0);

		public void reduce(TextTextTuple key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int counter = 0;
			for(IntWritable i : values) {
				counter += i.get();
			}
			outSum.set(counter);
			context.write(key, outSum);
		}
	}

	public static class CoOccurenceMapper extends Mapper<Text, Text, Text, TextLongTuple> {

		private TextLongTuple outCoOccurence = new TextLongTuple(); 
		private Text coTag = new Text();
		private LongWritable coCount   = new LongWritable(0);

		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] parts = value.toString().split("\t");
			if(parts.length != 2) {
				throw new IOException("malformed input format for given value: " +value.toString());
			}
			coTag.set(parts[0]);
			coCount.set(Integer.parseInt(parts[1]));
			outCoOccurence.set(coTag, coCount);
			context.write(key, outCoOccurence);
		}
	}

	public static class CoOccurenceReducer extends Reducer<Text, TextLongTuple, Text, Text> {

		private Text outAllCoOccurences = new Text();

		public void reduce(Text key, Iterable<TextLongTuple> values, Context context)
				throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();
			for(TextLongTuple co : values) {
				sb.append(co).append('\t');
			}
			outAllCoOccurences.set(sb.toString());
			context.write(key, outAllCoOccurences);
		}
	}

	public int run(String[] args) throws Exception {
		
		Job job1 = Job.getInstance(getConf());
		job1.setJarByClass(getClass());
		job1.setMapperClass(TagPairMapper.class);
		job1.setReducerClass(TagPairReducer.class);
		job1.setCombinerClass(TagPairReducer.class);
		job1.setNumReduceTasks(1);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setMapOutputKeyClass(TextTextTuple.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		Path outP1 = new Path(args[1]+"/job1");
		outP1.getFileSystem(getConf()).delete(outP1,true);
		FileOutputFormat.setOutputPath(job1, outP1);
		
		if(job1.waitForCompletion(true)) {
			Job job2 = Job.getInstance(getConf());;
			job2.setJarByClass(getClass());
			job2.setMapperClass(CoOccurenceMapper.class);
			job2.setReducerClass(CoOccurenceReducer.class);
			job2.setNumReduceTasks(1);
			job2.setInputFormatClass(KeyValueTextInputFormat.class);
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(TextLongTuple.class);
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