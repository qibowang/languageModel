package com.lm.rawcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by root on 2017/5/24.
 */
public class RawCountCombiner extends Reducer<Text,VIntWritable,Text,VIntWritable>{
	private VIntWritable resValue = new VIntWritable();
	@Override
	protected void reduce(Text key, Iterable<VIntWritable> values, Context context) throws IOException, InterruptedException {
		int sum=0;

		for(VIntWritable i:values){
			sum+=i.get();
		}
		resValue.set(sum);
		context.write(key,resValue);
	}
}
