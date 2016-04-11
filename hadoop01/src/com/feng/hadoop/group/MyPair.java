package com.feng.hadoop.group;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.Collator;
import java.util.Comparator;

import org.apache.hadoop.io.WritableComparable;

public class MyPair implements WritableComparable<MyPair> {

	private String first;
	
	private String second;
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(first);
		out.writeUTF(second);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first = in.readUTF();
		second = in.readUTF();
	}

	@Override
	public int compareTo(MyPair o) {
		if(!first.equals(o.getFirst())) {
//			return first.compareTo(o.getFirst());
//			return o.getFirst().compareTo(first);
			Comparator comparator = Collator.getInstance(java.util.Locale.CHINA);
			return comparator.compare(first, o.getFirst());
		}else if(!second.equals(o.getSecond())) {
			return second.compareTo(o.getSecond());
		}
		return 0;
	}

	public String getFirst() {
		return first;
	}

	public void setFirst(String first) {
		this.first = first;
	}

	public String getSecond() {
		return second;
	}

	public void setSecond(String second) {
		this.second = second;
	}

	@Override
	public int hashCode() {
		return first.hashCode() *157 + second.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MyPair other = (MyPair) obj;
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

}
