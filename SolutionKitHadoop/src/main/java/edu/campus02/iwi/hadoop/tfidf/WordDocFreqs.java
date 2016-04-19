package edu.campus02.iwi.hadoop.tfidf;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WordDocFreqs {
	
	public static class Map extends Mapper<LongWritable, Text, TextTextTuple, LongWritable> {

		private LongWritable one = new LongWritable(1);

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			
			String filename = ((FileSplit)context.getInputSplit()).getPath().getName();
			
			while (tokenizer.hasMoreTokens()) {
				TextTextTuple wordDoc = new TextTextTuple(tokenizer.nextToken(),filename);
				context.write(wordDoc, one);
			}
		}
	}
	
	public static class Reduce extends Reducer<TextTextTuple, LongWritable, TextTextTuple, LongWritable> {

		private LongWritable outValue = new LongWritable();

		public void reduce(TextTextTuple key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for(LongWritable value:values) {
				sum += value.get();
			}
			outValue.set(sum);
			context.write(key, outValue);
		}
	}

}
