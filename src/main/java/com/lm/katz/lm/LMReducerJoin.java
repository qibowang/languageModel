package com.lm.katz.lm;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.regex.Matcher;

/**
 * Created by root on 2017/9/1.
 */
public class LMReducerJoin extends Reducer<Text,Text,Text,Text>{
	private Text resKey = new Text();
	private Text resValue = new Text();
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double prob=0.0;
		String bow="";
		for(Text value:values){

			String[] temp=value.toString().split("\t");
			if(temp[0].equals("prob")){
				prob= Math.log10(Double.parseDouble(temp[1]));
			}else if(temp[0].equals("bow")){
				bow=temp[1];
			}


		}
		int order = key.toString().split(" ").length;

		if (order == 1) {
			context.getCounter(MyCounter.Counter._1).increment(1);
		} else if (order == 2) {
			context.getCounter(MyCounter.Counter._2).increment(1);
		} else if (order == 3) {
			context.getCounter(MyCounter.Counter._3).increment(1);
		} else if (order == 4) {
			context.getCounter(MyCounter.Counter._4).increment(1);
		} else if (order == 5) {
			context.getCounter(MyCounter.Counter._5).increment(1);
		}else if(order==6){
			context.getCounter(MyCounter.Counter._6).increment(1);
		}else if(order==7){
			context.getCounter(MyCounter.Counter._7).increment(1);
		}else if(order==8){
			context.getCounter(MyCounter.Counter._8).increment(1);
		}else if(order==9){
			context.getCounter(MyCounter.Counter._9).increment(1);
		}

		resKey.set(String.valueOf(prob));
		resValue.set((key.toString()+"\t"+bow).trim());



	}
}
