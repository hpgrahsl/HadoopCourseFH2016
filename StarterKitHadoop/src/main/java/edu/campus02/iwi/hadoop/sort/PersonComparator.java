package edu.campus02.iwi.hadoop.sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class PersonComparator extends WritableComparator {
	
	protected PersonComparator() {
		super(Person.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		Person p1 = (Person) w1;
		Person p2 = (Person) w2;
		//NOTE: determines the sort order for keys of type Person
		//according to the entire key (last+first name)
		//1) according to last name		
		int cmpLN = p1.getLastName().compareTo(p2.getLastName());
		if (cmpLN != 0) {
			return cmpLN;
		}
		//2) if equal last names according to first name
		int cmpFN =  p1.getFirstName().compareTo(p2.getFirstName());
		return cmpFN;
	}
}
