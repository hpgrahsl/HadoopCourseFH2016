package edu.campus02.iwi.hadoop.tags;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public abstract class Tuple<F,S> implements WritableComparable<Tuple<F,S>> {
	
	public static final String PAIR_SEPARATOR = "\t";
	
	protected F first;
	protected S second;
	
	public void set(F first, S second) {
		this.first = first;
		this.second = second;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return first + PAIR_SEPARATOR + second;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((first == null) ? 0 : first.hashCode());
		result = prime * result + ((second == null) ? 0 : second.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof Tuple))
			return false;
		Tuple other = (Tuple) obj;
		if (first == null) {
			if (other.first != null)
				return false;
		} else if (!first.equals(other.first))
			return false;
		if (second == null) {
			if (other.second != null)
				return false;
		} else if (!second.equals(other.second))
			return false;
		return true;
	}
	
	@Override
	public abstract void write(DataOutput out) throws IOException;

	@Override
	public abstract void readFields(DataInput in) throws IOException;

	@SuppressWarnings("rawtypes")
	@Override
	public abstract int compareTo(Tuple tuple);

}
