package com.lm.katz.lm;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by root on 2017/9/1.
 */
public class LMMapperBow extends Mapper<Text,DoubleWritable,Text,Text>{
	private Text resValue = new Text();
	private String fileFlag="bow";
	@Override
	protected void map(Text key, DoubleWritable value, Context context) throws IOException, InterruptedException {
		resValue.set(fileFlag+"\t"+value.get());
		context.write(key,resValue);
	}
}
