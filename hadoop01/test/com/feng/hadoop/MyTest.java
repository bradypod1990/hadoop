package com.feng.hadoop;

import java.text.Collator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.mapreduce.lib.partition.InputSampler.SplitSampler;
import org.junit.Test;

public class MyTest {

	@Test
	public void test() {
		
		
		String a = "丁道杰";
		String b = "仇雷";//丁道杰  万尧生  万平平
		System.out.println(a.compareTo(b));
		
		List<String> list = new ArrayList<String>();
		list.add("丁养超");
		list.add("丁振聪");
		list.add("丁道杰");
		list.add("万尧生");
		list.add("万平平");
		list.add("万汝洲");
		list.add("严光忠");
		list.add("严家顺");
		list.add("乔长波");
		list.add("仇雷");
		list.add("付福斌");
		list.add("付超龙");
		list.add("何少文");
		list.add("余存英");
		Comparator comparator = Collator.getInstance(java.util.Locale.CHINA);
		Collections.sort(list,comparator);
		for(String s : list) {
			System.out.println(s);
		}
	}
	
	@Test
	public void test2() {
		String name = "陈";
		char c = name.charAt(0);
		System.out.println(Character.getNumericValue(c));
		
		
	}
}
