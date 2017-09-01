package com.lm.countOfCounts;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by root on 2017/5/24.
 */
public class CocMapper extends Mapper<Text,LongWritable,Text,VIntWritable>{
	private VIntWritable one = new VIntWritable(1);
	private Text resKey = new Text();
	@Override
	protected void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
		int len=key.toString().split("\\s+").length;
		resKey.set(len+" "+value.get());
		context.write(resKey,one);
	}
}
