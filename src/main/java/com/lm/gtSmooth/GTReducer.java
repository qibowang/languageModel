package com.lm.gtSmooth;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by root on 2017/5/24.
 */
public class GTReducer extends Reducer<Text,Text,Text,Text>{
	private Text resKey = new Text();
	private Text resValue = new Text();
	private Configuration conf =null;
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		long rawCountSum=0l;
		conf=context.getConfiguration();
		List<Text> list = new ArrayList<Text>();

		for(Text joinValue :values){
			String str=joinValue.toString();
			//ngram\tgtcount\trawcount
			String[] items=str.split("\t");
			rawCountSum+=Long.parseLong(items[2]);
			list.add(WritableUtils.clone(joinValue,conf));
		}
		for(Text joinValue:list){
			String str=joinValue.toString();
			//ngram\tgtcount\trawcount
			String[] items=str.split("\t");
			//ngram\tgtcount\trawcount
			double prob=Double.parseDouble(items[1])/rawCountSum;

			if (prob > 1) {
				prob = 1.0;
			}else{
				BigDecimal b = new BigDecimal(prob);
				prob = b.setScale(7, BigDecimal.ROUND_HALF_UP).doubleValue();
			}

			resKey.set(items[0]);
			resValue.set(String.valueOf(prob));
			context.write(resKey,resValue);
		}

	}
}
