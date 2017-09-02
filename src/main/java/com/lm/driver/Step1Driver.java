package com.lm.driver;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import com.hadoop.compression.lzo.LzoCodec;


import com.lm.countOfCounts.CocCombiner;
import com.lm.countOfCounts.CocMapper;
import com.lm.countOfCounts.CocReducer;
import com.lm.gtSmooth.GTMapper;
import com.lm.gtSmooth.GTReducer;
import com.lm.rawcount.RawCountCombiner;
import com.lm.rawcount.RawCountMapper;
import com.lm.rawcount.RawCountPartitioner;
import com.lm.rawcount.RawCountReducer;




public class Step1Driver {
	public static void main(String[] args) {
		int order =3;
		int tasks=1;//设置为7
		String input=null;
		String rawCountPath=null;
		String cocPath=null;
		String gtSmoothPath=null;
		int gtmin=2;
		int lrLabel=0;
		int isLzo=0;//等于0表示压缩
		for(int i=0;i<args.length;i++){
			if(args[i].equals("-input")){
				input=args[++i];
				System.out.println("input--->"+input);
			}else if(args[i].equals("-rawcount")){
				rawCountPath=args[++i];
				System.out.println("rawCountPath--->"+rawCountPath);
			}else if(args[i].equals("-coc")){
				cocPath=args[++i];
				System.out.println("cocPath--->"+cocPath);
			}else if(args[i].equals("-gt")){
				gtSmoothPath=args[++i];
				System.out.println("gtSmoothPath--->"+gtSmoothPath);
			}else if(args[i].equals("-order")){
				order=Integer.parseInt(args[++i]);
				System.out.println("order--->"+order);
			}else if(args[i].equals("-tasks")){
				tasks=Integer.parseInt(args[++i]);
				System.out.println("tasks--->"+tasks);
			}else if(args[i].equals("-gtmin")){
				gtmin=Integer.parseInt(args[++i]);
				System.out.println("gtmin---->"+gtmin);
			}else if(args[i].equals("-lrLabel")){
				lrLabel=Integer.parseInt(args[++i]);
				System.out.println("lrLable---->"+lrLabel);
			}else if(args[i].equals("-isLzo")){
				isLzo=Integer.parseInt(args[++i]);
				System.out.println("isLzo---->"+isLzo);
			}else{
				System.out.println("there exists invalid parameters--->"+args[i]);
				break;
			}
		}
	
		
		
		try {
			Path outputPath=null;
			Configuration conf = new Configuration();
			conf.setInt("order", order);
			conf.setInt("gtmin", gtmin);
			conf.setInt("lrLabel",lrLabel);
			conf.setBoolean("mapreduce.compress.map.output", true);
			conf.setClass("mapreduce.map.output.compression.codec", LzoCodec.class,CompressionCodec.class);
			FileSystem fs = FileSystem.get(conf);
			Job rawCountJob = Job.getInstance(conf,"rawCountJob");
			System.out.println(rawCountJob.getJobName()+"is running!!!");
			rawCountJob.setJarByClass(Step1Driver.class);
			
			rawCountJob.setMapperClass(RawCountMapper.class);
			rawCountJob.setReducerClass(RawCountReducer.class);
			rawCountJob.setCombinerClass(RawCountCombiner.class);
			rawCountJob.setPartitionerClass(RawCountPartitioner.class);
			rawCountJob.setNumReduceTasks(tasks);
			
			rawCountJob.setMapOutputKeyClass(Text.class);
			rawCountJob.setMapOutputValueClass(VIntWritable.class);
			rawCountJob.setOutputKeyClass(Text.class);
			rawCountJob.setOutputValueClass(LongWritable.class);
			
			FileInputFormat.addInputPath(rawCountJob, new Path(input));
			FileInputFormat.setInputDirRecursive(rawCountJob, true);
			outputPath=new Path(rawCountPath);
			if(fs.exists(outputPath)){
				fs.delete(outputPath,true);
			}
			FileOutputFormat.setOutputPath(rawCountJob,outputPath);
			rawCountJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			if(isLzo==0){
				setLzo(rawCountJob);
			}
			
			if(rawCountJob.waitForCompletion(true)){
				System.out.println("rawCountJob successed");
			}else{
				System.out.println("rawCountJob failed");
			}
			
			Job cocJob = Job.getInstance(conf,"coc");
			System.out.println(cocJob.getJobName()+"is running!!!");
			cocJob.setNumReduceTasks(1);
			
			cocJob.setJarByClass(Step1Driver.class);
			cocJob.setMapperClass(CocMapper.class);
			cocJob.setCombinerClass(CocCombiner.class);
			cocJob.setReducerClass(CocReducer.class);
			
			cocJob.setMapOutputKeyClass(Text.class);
			cocJob.setMapOutputValueClass(VIntWritable.class);
			cocJob.setOutputKeyClass(Text.class);
			cocJob.setOutputValueClass(LongWritable.class);
			
			cocJob.setInputFormatClass(SequenceFileInputFormat.class);
			cocJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			
			FileInputFormat.addInputPath(cocJob, new Path(rawCountPath));
			FileInputFormat.setInputDirRecursive(cocJob, true);
			outputPath=new Path(cocPath);
			
			if(FileSystem.get(conf).exists(outputPath)){
				FileSystem.get(conf).delete(outputPath,true);
			}
			FileOutputFormat.setOutputPath(cocJob, outputPath);
			if(cocJob.waitForCompletion(true)){
				System.out.println("count of count step successed!");
			}else{
				System.out.println("count of step failed!");
			}
			
			
			
			conf.set("cocPath", cocPath);
			conf.setInt("gtmin", 2);
			fs= FileSystem.get(conf);
			Job gtSmoothJob = Job.getInstance(conf,"gtSmooth");
			System.out.println(gtSmoothJob.getJobName()+"is running!!!");
			gtSmoothJob.setJarByClass(Step1Driver.class);
			gtSmoothJob.setNumReduceTasks(tasks);
			
			gtSmoothJob.setMapperClass(GTMapper.class);
			gtSmoothJob.setReducerClass(GTReducer.class);
			
			gtSmoothJob.setMapOutputKeyClass(Text.class);
			gtSmoothJob.setMapOutputValueClass(Text.class);
			gtSmoothJob.setOutputKeyClass(Text.class);
			gtSmoothJob.setOutputValueClass(DoubleWritable.class);
			
			gtSmoothJob.setInputFormatClass(SequenceFileInputFormat.class);
			gtSmoothJob.setOutputFormatClass(SequenceFileOutputFormat.class);
			if(isLzo==0){
				setLzo(gtSmoothJob);
			}
			
			
			FileInputFormat.addInputPath(gtSmoothJob, new Path(rawCountPath));
			FileInputFormat.setInputDirRecursive(gtSmoothJob, true);
			
			outputPath=new Path(gtSmoothPath);
			if(fs.exists(outputPath)){
				fs.delete(outputPath,true);
			}
			FileOutputFormat.setOutputPath(gtSmoothJob, outputPath);
			if(gtSmoothJob.waitForCompletion(true)){
				System.out.println("good turning smooth step successed!");
			}else{
				System.out.println("good turning smooth step failed!");
			}
				
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
