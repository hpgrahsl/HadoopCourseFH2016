package edu.campus02.iwi.hadoop.sort;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.campus02.iwi.hadoop.env.WinConfig;

public class SecondarySortSample extends Configured implements Tool {

	public static final String NAMES_FILE = "names.txt";
	public static String[] LAST_NAMES = {"Gruenwald","Hollosi","Dobler","Schweighofer","Steyer","Loidolt","Liu","Scherr","Poschauko","Weitlaner","Pergler","Hoeber","Softic","Tretter"};
	public static String[] FIRST_NAMES = {"Stephan","Arno","Lisa","Patrick","Manfred","Thomas","Xiaoshan","Birgit","Silvia","Doris","Elisabeth","Angelika","Selver","Verena"};

	public static class Map extends Mapper<Text, Text, Person, Text> {

		private Person outputKey = new Person();

		protected void map(Text lastName, Text firstName, Context context)
				throws IOException, InterruptedException {
			outputKey.set(lastName.toString(), firstName.toString());
			context.write(outputKey, firstName);
		}
	}

	public static class Reduce extends Reducer<Person, Text, Text, Text> {

		Text lastName = new Text();
		Text firstNames = new Text();

		public void reduce(Person key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			lastName.set(key.getLastName());
			StringBuilder sb = new StringBuilder();
			for (Text firstName : values) {
				sb.append(firstName).append("\t");
			}
			firstNames.set(sb.toString());
			context.write(lastName, firstNames);
		}
	}
	
	public static void createInputFile(Path file) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = file.getFileSystem(conf);
		OutputStream os = fs.create(file);
		List<String> fullNames = new ArrayList<String>();
		for(String l : LAST_NAMES) {
			for(String f : FIRST_NAMES) {
				fullNames.add(l+"\t"+f);
			}
		}
		//shuffle the names to get a pseudo-random names file output
		Collections.shuffle(fullNames);
		for(String s:fullNames) {
			IOUtils.write(s+"\n", os);
		}
		os.close();
	}

	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"secondary sort sample");
		job.setJarByClass(SecondarySortSample.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(Integer.parseInt(args[1]));

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		job.setMapOutputKeyClass(Person.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		//NOTE: custom control of the framework internal sorting
		job.setPartitionerClass(LastNamePartitioner.class);
		job.setSortComparatorClass(PersonComparator.class);
		job.setGroupingComparatorClass(LastNameComparator.class);

		String output = args[0];
		FileInputFormat.setInputPaths(job, output+"/data/");
		Path inputFile = new Path(output+"/data/"+NAMES_FILE);

		createInputFile(inputFile);

		Path outputPath = new Path(output+"/result");
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath, true);

		return job.waitForCompletion(true) ? 0 : -1;

	}

	public static void main(String[] args) throws Exception {
		
		WinConfig.setupEnv();
		
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <outputDir> <numReducers>\n",
					SecondarySortSample.class.getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			System.exit(-1);
		}
		
		int exitCode = ToolRunner.run(new SecondarySortSample(), args);
		System.exit(exitCode);
	}

}
