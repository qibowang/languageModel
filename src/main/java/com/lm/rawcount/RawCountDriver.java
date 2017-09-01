package com.lm.rawcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Created by root on 2017/5/25.
 */
public class RawCountDriver {
	public static void main(String[] args) {
		String input=null;
		String output=null;
		int order=3;
		for(int i=0;i<args.length;i++){
			if("-input".equals(args[i])){
				input=args[++i];
			}else if("-output".equals(args[i])){
				output=args[++i];
			}else if("-order".equals(args[i])){
				order=Integer.parseInt(args[++i]);
			}else{
				System.out.println("there exists invalid parameters");
				break;
			}
		}

		try {
			Configuration conf = new Configuration();
			conf.setInt("order",order);
			//conf.setBoolean("mapreduce.compress.map.output", true);
			//conf.setClass("mapreduce.map.output.compression.codec", LzoCodec.class,CompressionCodec.class);
			Job job = Job.getInstance(conf,"rawCount");
			job.setJarByClass(RawCountDriver.class);
			job.setNumReduceTasks(1);
			job.setMapperClass(RawCountMapper.class);
			job.setPartitionerClass(RawCountPartitioner.class);
			job.setCombinerClass(RawCountCombiner.class);
			job.setReducerClass(RawCountReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(VIntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);

			FileInputFormat.addInputPath(job, new Path(input));
			//FileInputFormat.setInputDirRecursive(job, true);



		} catch (IOException e) {
			e.printStackTrace();
		}


	}
}
