package com.lm.driver;

import java.io.IOException;

import com.lm.katz.bow.*;
import com.lm.katz.lm.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import com.hadoop.compression.lzo.LzoCodec;




public class Step2Driver {
	public static void main(String[] args) {
		
		try {
			
			int order =5;
			int tasks=9;
			Path outputPath=null;
			String gtPath=null;
			String gtPath2=null;
			String prefixJoinsuffix=null;
			String bowPath=null;
			String finalPath=null;
			String countersPath=null;
			int isLzo=0;
			
			for(int i=0;i<args.length;i++){
				if(args[i].equals("-gt")){
					gtPath=args[++i];
					System.out.println("gtSmooth path ----->"+gtPath);
				}else if(args[i].equals("-gt2")){
					gtPath2=args[++i];
					System.out.println("gtSmooth path2----->"+gtPath2);
				}else if(args[i].equals("-prefix")){
					prefixJoinsuffix=args[++i];
					System.out.println("prefixJoinsuffix---->"+prefixJoinsuffix);
				}else if(args[i].equals("-bow")){
					bowPath=args[++i];
					System.out.println("bowPath--->"+bowPath);
				}else if(args[i].equals("-final")){
					finalPath=args[++i];
					System.out.println("finalPath-->"+finalPath);
				}else if(args[i].equals("-counter")){
					countersPath=args[++i];
					System.out.println("counterPath--->"+countersPath);
				}else if(args[i].equals("-order")){
					order=Integer.parseInt(args[++i]);
					System.out.println("order---->"+order);
				}else if(args[i].equals("-tasks")){
					tasks=Integer.parseInt(args[++i]);
					System.out.println("tasks----->"+tasks);
				}else if(args[i].equals("-isLzo")){
					isLzo=Integer.parseInt(args[++i]);
					System.out.println("isLzo---->"+isLzo);
				}else{
					System.out.println("there exists invalid parameters--->"+args[i]);
					break;
				}
			}
			
			Configuration conf = new Configuration();
			conf.setInt("order", order);
			FileSystem fs = FileSystem.get(conf);
			
			Job job = Job.getInstance(conf,"join1");
			System.out.println(job.getJobName()+" is running!!!");
			job.setJarByClass(Step2Driver.class);
			job.setReducerClass(SuffixJoinProb.class);
			job.setNumReduceTasks(tasks);
			
			MultipleInputs.addInputPath(job, new Path(gtPath), SequenceFileInputFormat.class,Suffix.class);
			MultipleInputs.addInputPath(job, new Path(gtPath2), SequenceFileInputFormat.class,Prob.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			if(isLzo==0){
				setLzo(job);
			}
			
			
			outputPath= new Path(prefixJoinsuffix);
			if(fs.exists(outputPath)){
				fs.delete(outputPath,true);
			}
			FileOutputFormat.setOutputPath(job, outputPath);
			
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			
			if(job.waitForCompletion(true)){
				System.out.println("job finished");
			}else{
				System.out.println("job failed");
			}
			
			Job bowJob = Job.getInstance(conf,"bow job");
			System.out.println(bowJob.getJobName()+"is running!!!");
			bowJob.setJarByClass(Step2Driver.class);
			bowJob.setMapperClass(BowMapper.class);
			bowJob.setReducerClass(BowReducer.class);
			bowJob.setNumReduceTasks(tasks);
			
			bowJob.setInputFormatClass(SequenceFileInputFormat.class);
			bowJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			
			bowJob.setMapOutputKeyClass(Text.class);
			bowJob.setMapOutputValueClass(Text.class);
			bowJob.setOutputKeyClass(Text.class);
			bowJob.setOutputValueClass(DoubleWritable.class);
			if(isLzo==0){
				setLzo(bowJob);
			}
			
			FileInputFormat.addInputPath(bowJob, new Path(prefixJoinsuffix));
			
			outputPath= new Path(bowPath);
			if(fs.exists(outputPath)){
				fs.delete(outputPath,true);
			}
			
			FileOutputFormat.setOutputPath(bowJob, outputPath);
			
			if(bowJob.waitForCompletion(true)){
				System.out.println("bow job successed!");
			}else{
				System.out.println("bow job failed!");
			}
			
			
			Job finalJob = Job.getInstance(conf,"final job");
			System.out.println(finalJob.getJobName()+"is running!!!");
			finalJob.setJarByClass(Step2Driver.class);
			finalJob.setReducerClass(LMReducerJoin.class);
			finalJob.setSortComparatorClass(MyComparator.class);
			finalJob.setNumReduceTasks(1);//此处设为1速度会有所降低  但是生成的文件是排序好的 直接可以用来转换成arpa格式
			
			MultipleInputs.addInputPath(finalJob, new Path(bowPath), SequenceFileInputFormat.class, LMMapperBow.class);
			MultipleInputs.addInputPath(finalJob, new Path(gtPath), SequenceFileInputFormat.class, LMMapperProb.class);
			
			outputPath=new Path(finalPath);
			if(fs.exists(outputPath)){
				fs.delete(outputPath,true);
			}
			
			FileOutputFormat.setOutputPath(finalJob, outputPath);
			finalJob.setMapOutputKeyClass(Text.class);
			finalJob.setMapOutputValueClass(Text.class);
			finalJob.setOutputKeyClass(Text.class);
			finalJob.setOutputValueClass(Text.class);
			
			if(finalJob.waitForCompletion(true)){
				System.out.println("finalJob successed");
			}else{
				System.out.println("finalJob failed");
			}
			
			Text key = new Text();
			IntWritable value = new IntWritable();
			Path res = new Path(countersPath);
			
	        Option option1 = Writer.file(res); 
	        Option keyOption = Writer.keyClass(key.getClass());
	        Option valueOption = Writer.valueClass(value.getClass());
	        Writer writer = SequenceFile.createWriter(conf,option1,keyOption,valueOption);
			long count=0;
			Counters counters=finalJob.getCounters();
			for(int i=1;i<=order;i++){
				
				if(i==1){
					count=counters.findCounter(MyCounter.Counter._1).getValue();
					String str="ngram "+i+"="+count;
					key.set(str);
					writer.append(key, value);
					System.out.println(str);
				}else if(i==2){
					count=counters.findCounter(MyCounter.Counter._2).getValue();
					String str="ngram "+i+"="+count;
					key.set(str);
					writer.append(key, value);
					System.out.println(str);
				}else if(i==3){
					count=counters.findCounter(MyCounter.Counter._3).getValue();
					String str="ngram "+i+"="+count;
					key.set(str);
					writer.append(key, value);
					System.out.println(str);
				}else if(i==4){
					count=counters.findCounter(MyCounter.Counter._4).getValue();
					String str="ngram "+i+"="+count;
					key.set(str);
					writer.append(key, value);
					System.out.println(str);
				}else if(i==5){
					count=counters.findCounter(MyCounter.Counter._5).getValue();
					String str="ngram "+i+"="+count;
					key.set(str);
					writer.append(key, value);
					System.out.println(str);
				}else if(i==6){
					count=counters.findCounter(MyCounter.Counter._6).getValue();
					String str="ngram "+i+"="+count;
					key.set(str);
					writer.append(key, value);
					System.out.println(str);
				}else if(i==7){
					count=counters.findCounter(MyCounter.Counter._7).getValue();
					String str="ngram "+i+"="+count;
					key.set(str);
					writer.append(key, value);
					System.out.println(str);
				}else if(i==8){
					count=counters.findCounter(MyCounter.Counter._8).getValue();
					String str="ngram "+i+"="+count;
					key.set(str);
					writer.append(key, value);
					System.out.println(str);
				}else if(i==9){
					count=counters.findCounter(MyCounter.Counter._9).getValue();
					String str="ngram "+i+"="+count;
					key.set(str);
					writer.append(key, value);
					System.out.println(str);
				}
			}
			
			IOUtils.closeStream(writer);
		
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} 
	}
	public static void setLzo(Job job){
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(job, LzoCodec.class);
	}
}
