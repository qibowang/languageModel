package com.lm.katz.bow;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by root on 2017/9/1.
 */
public class BowReducer extends Reducer<Text,Text,Text,DoubleWritable> {

	private DoubleWritable resValue = new DoubleWritable();
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double prefixProbSum=0.0;
		double suffixProbSum=0.0;
		for(Text value:values){
			String[] temp=value.toString().split("\t");
			if(temp.length==2){
				prefixProbSum+=Double.parseDouble(temp[0]);
				suffixProbSum+=Double.parseDouble(temp[1]);
			}else{
				prefixProbSum+=Double.parseDouble(temp[0]);
			}
		}

		double numerator=0.0;
		double denominator=0.0;
		if(prefixProbSum<1){
			numerator=Math.log1p(1-prefixProbSum);
		}
		if(suffixProbSum<1){
			denominator=Math.log10(1-suffixProbSum);

		}

		double bow=numerator-denominator;
		resValue.set(bow);
		context.write(key,resValue);

	}


}
