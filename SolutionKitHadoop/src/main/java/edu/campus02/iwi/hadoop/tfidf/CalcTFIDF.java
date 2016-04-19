/**
 * 
 */
package edu.campus02.iwi.hadoop.tfidf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class CalcTFIDF {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		private Text keyWord = new Text();
		private Text values = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			String[] parts = StringUtils.split(value.toString());
			StringBuilder sb = new StringBuilder();
			if(parts.length == 4) {
				keyWord.set(parts[0]);
				sb.append(parts[1])
					.append('\t')
					.append(parts[2])
					.append('\t')
					.append(parts[3])
					.append('\t');
				values.set(sb.toString());
				context.write(keyWord, values);
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, TextTextTuple, DoubleWritable> {

		private long numCorpusFiles = 0;
		
		private TextTextTuple keyWordDocId = new TextTextTuple();
		private DoubleWritable outTFIDF = new DoubleWritable(); 

		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			// get the number of documents indirectly from the file-system
			// using the configuration entry that was set in the driver code for
			// job 3
			numCorpusFiles = context.getConfiguration().getInt("numCorpusFiles", 0);
		}
		
		@Override
		protected void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			List<Text> tempCache = new ArrayList<Text>(); 
			long freqInCorpus = 0;
			for(Text data:value) {
				Text copy = new Text(data.toString());
				tempCache.add(copy);
				freqInCorpus++;
			}
			for(Text data:tempCache) {
				String[] parts = StringUtils.split(data.toString());
				if(parts.length==3) {
					keyWordDocId.set(key, new Text(parts[0]));
					double tf = Double.parseDouble(parts[1]) / Double.parseDouble(parts[2]);
					double idf = Math.log10(numCorpusFiles / (double)freqInCorpus);
					outTFIDF.set(tf*idf);
					context.write(keyWordDocId, outTFIDF);
				}
			}
		}

	}

}

