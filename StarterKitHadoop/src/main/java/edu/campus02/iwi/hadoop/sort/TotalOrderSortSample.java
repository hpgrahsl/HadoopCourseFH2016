package edu.campus02.iwi.hadoop.sort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.campus02.iwi.hadoop.env.WinConfig;

public class TotalOrderSortSample extends Configured implements Tool {

	public static final String NAMES_FILE = "names.seq";
	public static String[] LAST_NAMES = {"Gruenwald","Hollosi","Dobler","Schweighofer","Steyer","Loidolt","Liu","Scherr","Poschauko","Weitlaner","Pergler","Hoeber","Softic","Tretter"};
	public static String[] FIRST_NAMES = {"Stephan","Arno","Lisa","Patrick","Manfred","Thomas","Xiaoshan","Birgit","Silvia","Doris","Elisabeth","Angelika","Selver","Verena"};

	public static class Map extends Mapper<Text, Text, Person, NullWritable> {

		private Person outputKey = new Person();

		protected void map(Text lastName, Text firstName, Context context)
				throws IOException, InterruptedException {
			outputKey.set(lastName.toString(), firstName.toString());
			context.write(outputKey, NullWritable.get());
		}
	}

	public static class Reduce extends Reducer<Person, NullWritable, Person, NullWritable> {

		public void reduce(Person key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
		
	}

	public static void createInputFile(Path file) throws IOException {	
		List<Person> people = new ArrayList<Person>();
		for(String l : LAST_NAMES) {
			for(String f : FIRST_NAMES) {
				people.add(new Person(f,l));
			}
		}
		//shuffle the names to get a pseudo-random names file output
		Collections.shuffle(people);
		Configuration conf = new Configuration();
		
		SequenceFile.Writer writer = 
				SequenceFile.createWriter(conf, Writer.file(file), Writer.keyClass(Person.class),
			            Writer.valueClass(NullWritable.class));
		
		for(Person p:people) {
			writer.append(p, NullWritable.get());
		}
		
		writer.close();
	}

	public int run(String[] args) throws Exception {
		
		String output = args[0];
		
		Path inPath = new Path(output+"/data");
		Path outPath = new Path(output+"/result");

		InputSampler.Sampler<Person,NullWritable> sampler =
				new InputSampler.RandomSampler<Person,NullWritable>(0.1,32,1);

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(TotalOrderSortSample.class);
	
		job.setNumReduceTasks(Integer.parseInt(args[1]));

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Person.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setSortComparatorClass(PersonComparator.class);
		
		Path inputFile = new Path(output+"/data/"+NAMES_FILE);
		createInputFile(inputFile);
		
		FileInputFormat.addInputPath(job, inPath);
		FileOutputFormat.setOutputPath(job, outPath);
		
		outPath.getFileSystem(conf).delete(outPath, true);
		
		Path partitionFile = new Path(output, "_partitions.seq");
		//NOTE: the following 2 lines are needed for using hadoop's distributed cache 
		//so that the partition file is accessible from all participating nodes
		//BUT we don't need this since everything is locally executed in this case...
		//URI partitionUri = new URI(partitionFile.toString() + "#_partitions.seq");
		//job.addCacheFile(partitionUri);
		
		System.out.println("DEBUG: Partition file path: " + partitionFile);
		
		job.setPartitionerClass(TotalOrderPartitioner.class);
		
		TotalOrderPartitioner.setPartitionFile(job.getConfiguration(),partitionFile);
		InputSampler.writePartitionFile(job, sampler);

		return job.waitForCompletion(true) ? 0 : -1;
	}
	
	public static void main(String[] args) throws Exception {
		
		WinConfig.setupEnv();
		
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <outputDir> <numReducers>\n",
					TotalOrderSortSample.class.getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			System.exit(-1);
		}
		
		int exitCode = ToolRunner.run(new TotalOrderSortSample(), args);
		System.exit(exitCode);
	}


	
}
