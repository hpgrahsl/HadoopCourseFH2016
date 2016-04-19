package edu.campus02.iwi.hadoop.tfidf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class TextLongTuple extends Tuple<Text, LongWritable> {

	public TextLongTuple() {
		set(new Text(), new LongWritable());
	}

	public TextLongTuple(String first, Long second) {
		set(new Text(first), new LongWritable(second));
	}

	public TextLongTuple(Text first, LongWritable second) {
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
		int cmp = first.compareTo((Text)tuple.first);
		if (cmp != 0) {
			return cmp;
		}
		return second.compareTo((LongWritable)tuple.second);
	}

}
