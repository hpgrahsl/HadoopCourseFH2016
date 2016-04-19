package edu.campus02.iwi.hadoop.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class LastNamePartitioner extends Partitioner<Person, Text> {
	@Override
	public int getPartition(Person key, Text value, int numPartitions) {
		//NOTE: this is to ensure that a reducer operates on all available
		//values (= first names) for a specific last name
		//when only using 1 reducer this would not matter
		int partNum = Math.abs(key.getLastName().hashCode() * 127) % numPartitions;
		return partNum;
	}
}
