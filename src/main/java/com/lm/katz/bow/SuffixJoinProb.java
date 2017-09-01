package com.lm.katz.bow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by root on 2017/9/1.
 */
public class SuffixJoinProb extends Reducer<Text,Text,Text,Text>{
	private Text resKey = new Text();
	private Text resValue = new Text();
	String ngram="";
	String suffixProb="";
	String prob="";
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for(Text value:values){
			String temp[]=value.toString().split(" ");
			if(temp[0].equals("suffix")){
				ngram=temp[1];
				prob=temp[2];
			}else if(temp[0].equals("prob")){
				suffixProb=temp[1];
			}
		}
		if (ngram.length()!=0){
			resKey.set(ngram);
			resValue.set((prob+"\t"+suffixProb).trim());
		}
	}
}
