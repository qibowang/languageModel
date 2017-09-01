package com.lm.katz.bow;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by root on 2017/9/1.
 */
public class Suffix extends Mapper<Text,DoubleWritable,Text,Text>{
	private String fileFlag="suffix";
	private Text resKey = new Text();
	private Text resValue = new Text();

	@Override
	protected void map(Text key, DoubleWritable value, Context context) throws IOException, InterruptedException {
		String ngram=key.toString();
		int len=ngram.split(" ").length;
		if(len>=2){
			int index=ngram.indexOf(" ");
			String suffix=ngram.substring(index+1);
			resKey.set(suffix);
			resValue.set(fileFlag+"\t"+ngram+"\t");
			context.write(resKey,resValue);
			
		}
	}
}
