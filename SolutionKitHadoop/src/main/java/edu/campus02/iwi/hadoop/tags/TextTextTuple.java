package edu.campus02.iwi.hadoop.tags;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

public class TextTextTuple extends Tuple<Text, Text> {

	public TextTextTuple() {
		set(new Text(), new Text());
	}

	public TextTextTuple(String first, String second) {
		set(new Text(first), new Text(second));
	}

	public TextTextTuple(Text first, Text second) {
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
		return second.compareTo((Text)tuple.second);
	}

}
