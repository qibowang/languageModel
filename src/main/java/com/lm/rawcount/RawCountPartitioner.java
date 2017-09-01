package com.lm.rawcount;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * Created by root on 2017/5/24.
 */
public class RawCountPartitioner extends HashPartitioner<Text,VIntWritable>{
	@Override
	public int getPartition(Text key, VIntWritable value, int numReduceTasks) {
		String[] lineArr =key.toString().split(" ");
		String prefix =(lineArr.length>1)?(lineArr[0]+lineArr[1]):lineArr[0];
		return Math.abs(prefix.hashCode())%numReduceTasks;
	}
}
