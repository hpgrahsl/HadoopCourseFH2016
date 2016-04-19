package edu.campus02.iwi.hadoop.twitter;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

public class DriverTweetFeed extends Configured implements Tool  {
	
	public static class TwitterStreamMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		private Text outKey = new Text();
		private Text outValue = new Text();
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			try {
				Tweet tweet = new Gson().fromJson(value.toString(), Tweet.class);
				if(tweet.text!= null) {
					TweetParts parts = new TweetParts();
					parts.num_chars = tweet.text.length();
					parts.num_hashtags = tweet.entities.hashtags.size();
					parts.num_urls = tweet.entities.urls.size();
					
					//stats based on tweet's language
					outKey.set(tweet.lang);
					
					outValue.set(new Gson().toJson(parts, TweetParts.class));
					context.write(outKey, outValue);
				}
			} catch (JsonSyntaxException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	public static class TwitterStreamReducer extends Reducer<Text, Text, Text, Text> {
		
		private Text outValue = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			try {
				TweetAvgResult result = new TweetAvgResult();
				int counter = 0;
				for(Text v:values) {
					TweetParts tp = new Gson().fromJson(v.toString(), TweetParts.class);
					result.avg_num_chars += tp.num_chars;
					result.perc_containing_urls += (tp.num_urls >= 1 ? 1 : 0);
					result.at_least_three_hashtags += (tp.num_hashtags >= 3 ? 1 : 0);
					counter++;
				}
				result.num_tweets = counter;
				result.avg_num_chars /= counter;
				result.perc_containing_urls = result.perc_containing_urls / counter * 100;
				outValue.set(new Gson().toJson(result,TweetAvgResult.class));
				context.write(key,outValue);
			} catch (JsonSyntaxException e) {
				e.printStackTrace();
			}
		}

	}
			
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
		
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		job1.setMapperClass(TwitterStreamMapper.class);
		job1.setReducerClass(TwitterStreamReducer.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		return job1.waitForCompletion(true) ? 0 : -1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new DriverTweetFeed(), args);
		System.exit(exitCode);
	}

}
