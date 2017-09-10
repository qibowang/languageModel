package com.lm.katz.bow;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by root on 2017/9/1.
 */
public class BowMapper extends Mapper<Text,Text,Text,Text>{
	private Text resKey = new Text();

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

		String ngram=key.toString();
		int len=ngram.split(" ").length;

		if(len>=2){
			int lastIndex=ngram.lastIndexOf(" ");
			String prefix=ngram.substring(0,lastIndex);

			resKey.set(prefix);
			context.write(resKey,value);


		}
	}
}
