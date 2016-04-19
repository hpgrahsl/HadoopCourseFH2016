package edu.campus02.iwi.hadoop.sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class LastNameComparator extends WritableComparator {

	public LastNameComparator() {
		super(Person.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		Person p1 = (Person) w1;
		Person p2 = (Person) w2;
		//NOTE: grouping of the sorted records for a single reduce function call
		//according to last name comparison in lexicographic order
		int cmp = p1.getLastName().compareTo(p2.getLastName());
		return cmp;
	}

}
