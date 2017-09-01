package com.lm.tools;

import java.util.regex.Pattern;

public class StrIntercept {
	public static String getPrefix(String ngram) {
		int lastIndex = ngram.lastIndexOf(" ");
		if(-1==lastIndex){
			return ngram;
		}
		String suffix = ngram.substring(0, lastIndex);
		return suffix;
	}
	public static String getSuffix(String ngram){
		int firstIndex= ngram.indexOf(" ");
		if(-1==firstIndex){
			
			return ngram;
		}
		String suffix=ngram.substring(firstIndex+1);
		return suffix;
	}
	
	public static String getMiddle(String ngram){
		int firstIndex=ngram.indexOf(" ");
		
		int lastIndex=ngram.lastIndexOf(" ");
		if(firstIndex==lastIndex)
			return "";
		String mid=ngram.substring(firstIndex+1, lastIndex);
		return mid;
	}
	
	public static void main(String[] args) {
		String str="hello";
		System.out.println(getPrefix(str));
		System.out.println(getSuffix(str));
		//System.out.println(getMiddle(str));
	
	}
	
}
