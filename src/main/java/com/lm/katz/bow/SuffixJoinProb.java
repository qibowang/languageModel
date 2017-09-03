package com.lm.katz.bow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
		Configuration conf = context.getConfiguration();
		List<Text> joinValues = new ArrayList<Text>();
		for(Text value:values){
			String temp[]=value.toString().split(" ");
			if(temp[0].equals("suffix")){
				joinValues.add(WritableUtils.clone(value,conf));
			}else if(temp[0].equals("prob")){
				suffixProb=temp[1];
			}
		}
		for(Text value:joinValues){
			String temp[]=value.toString().split(" ");
			ngram=temp[0];
			prob=temp[1];
			resKey.set(ngram);
			resValue.set((prob+"\t"+suffixProb).trim());
			context.write(resKey,resValue);
		}
	}
}
