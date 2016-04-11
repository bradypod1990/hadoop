package com.feng.hadoop.group;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyComparator extends WritableComparator {

	public MyComparator() {
		super(MyPair.class, true);
	}
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		MyPair p1 = (MyPair)a;
		MyPair p2 = (MyPair)b;
		String l = p1.getFirst();
		String l2 = p2.getFirst();
		return l.compareTo(l2);
	}


}
