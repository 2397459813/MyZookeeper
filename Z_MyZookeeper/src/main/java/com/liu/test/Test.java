package com.liu.test;

import java.util.ArrayList;
import java.util.List;

public class Test {
	public static void main(String[] args) throws InterruptedException {
		List<String> list = new ArrayList<String>();
		for(int i=0;i<10000;i++) {
			if(i%3 == 1) {
				list.add("***");
			}else if(i%3 == 2) {
				list.add("&&&");
			}else {
				list.add("%%%");
			}
			if(list.size()>10) {
				list.clear();
				System.out.println("*******init******");
			}
			System.out.println(list.toString());
			Thread.sleep(1000);
		}
		
	}
}
