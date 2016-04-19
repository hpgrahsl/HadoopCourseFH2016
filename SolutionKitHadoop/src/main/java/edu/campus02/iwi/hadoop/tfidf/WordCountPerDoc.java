/**
 * 
 */
package edu.campus02.iwi.hadoop.tfidf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author hanspeter
 *
 */
public class WordCountPerDoc {

	public static class Map extends Mapper<LongWritable, Text, Text, TextLongTuple> {

		private Text keyDocId = new Text();
		private Text word = new Text();
		private LongWritable freq = new LongWritable();
		private TextLongTuple valueWordFreq = new TextLongTuple();

		@Override
		protected void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			String[] parts = StringUtils.split(value.toString());
			if(parts.length == 3) {
				keyDocId.set(parts[1]);
				word.set(parts[0]);
				freq.set(Long.parseLong(parts[2]));
				valueWordFreq.set(word, freq);
				context.write(keyDocId, valueWordFreq);
			}
		}
	}

	public static class Reduce extends Reducer<Text, TextLongTuple, TextTextTuple, LongLongTuple> {

		private TextTextTuple keyWordDocId = new TextTextTuple();
		private LongLongTuple valueFreqCount = new LongLongTuple();
		private LongWritable total = new LongWritable();

		@Override
		protected void reduce(Text key, Iterable<TextLongTuple> value, Context context)
				throws IOException, InterruptedException {
			List<TextLongTuple> tempCache = new ArrayList<TextLongTuple>(); 
			long sumFreq = 0;
			for(TextLongTuple pair:value) {
				TextLongTuple copy = new TextLongTuple(pair.first.toString(),pair.second.get());
				tempCache.add(copy);
				sumFreq += pair.second.get();
			}
			for(TextLongTuple pair:tempCache) {
				keyWordDocId.set(pair.first, key);
				total.set(sumFreq);
				valueFreqCount.set(pair.second, total);
				context.write(keyWordDocId,valueFreqCount);
			}
		}

	}

}

