package edu.campus02.iwi.hadoop.tfidf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;

public class LongLongTuple extends Tuple<LongWritable, LongWritable> {

	public LongLongTuple() {
		set(new LongWritable(), new LongWritable());
	}

	public LongLongTuple(Long first, Long second) {
		set(new LongWritable(first), new LongWritable(second));
	}

	public LongLongTuple(LongWritable first, LongWritable second) {
		set(first, second);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compareTo(Tuple tuple) {
		int cmp = first.compareTo((LongWritable)tuple.first);
		if (cmp != 0) {
			return cmp;
		}
		return second.compareTo((LongWritable)tuple.second);
	}

}
