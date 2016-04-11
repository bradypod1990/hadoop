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
		
		
		String a = "������";
		String b = "����";//������  ��Ң��  ��ƽƽ
		System.out.println(a.compareTo(b));
		
		List<String> list = new ArrayList<String>();
		list.add("������");
		list.add("�����");
		list.add("������");
		list.add("��Ң��");
		list.add("��ƽƽ");
		list.add("������");
		list.add("�Ϲ���");
		list.add("�ϼ�˳");
		list.add("�ǳ���");
		list.add("����");
		list.add("������");
		list.add("������");
		list.add("������");
		list.add("���Ӣ");
		Comparator comparator = Collator.getInstance(java.util.Locale.CHINA);
		Collections.sort(list,comparator);
		for(String s : list) {
			System.out.println(s);
		}
	}
	
	@Test
	public void test2() {
		String name = "��";
		char c = name.charAt(0);
		System.out.println(Character.getNumericValue(c));
		
		
	}
}
