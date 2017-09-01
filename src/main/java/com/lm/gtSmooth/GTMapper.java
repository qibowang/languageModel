package com.lm.gtSmooth;

import com.lm.tools.StrIntercept;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Reader.Option;

import java.io.IOException;
import java.util.HashMap;

/**
 * Created by root on 2017/5/24.
 */
public class GTMapper extends Mapper<Text,LongWritable,Text,Text>{
	//map 外层key 是ngram中n 1 2 3 4 5 内层是一个hash 其中key是原始ngram频次 value是出现key次的总共有多少个
	private HashMap<String, HashMap<String, Long>> map = new HashMap<String, HashMap<String, Long>>();
	private Text outKey = new Text();
	private Text outValue = new Text();
	private Configuration conf;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		conf = context.getConfiguration();
		String cocPath = conf.get("cocPath");

		Path path = new Path(cocPath);

		Option option = Reader.file(path);
		Reader reader = new Reader(conf, option);
		Text key = new Text();
		LongWritable value = new LongWritable();
		while (reader.next(key, value)) {
			String[] tokens = key.toString().split(" ");
			String order = tokens[0];
			String rawCount = tokens[1];
			Long times = value.get();
			if (map.containsKey(order)) {
				HashMap<String, Long> temp = map.get(order);
				temp.put(rawCount, times);
				map.put(order, temp);
			} else {
				HashMap<String, Long> temp = new HashMap<String, Long>();
				temp.put(rawCount, times);
				map.put(order, temp);
			}
		}
		IOUtils.closeStream(reader);
	}

	@Override
	protected void map(Text key, LongWritable value, Context context) throws IOException, InterruptedException {
		Long temp=value.get();
		double gtCount= temp.doubleValue();
		String ngram=key.toString();
		long rawCount=value.get();
		int order=ngram.split("\\s+").length;
		String orderStr=String.valueOf(order);
		String rawCountStr=String.valueOf(rawCount);
		for (int i = 1; i < 5; i++) {
			if (map.get(orderStr).containsKey(rawCountStr)
					&& map.get(orderStr).containsKey(String.valueOf(rawCount + i))) {
				Long Nr1=map.get(orderStr).get(String.valueOf(rawCount + i));
				Long Nr=map.get(orderStr).get(rawCountStr);
				gtCount=(rawCount+1d)*Nr1.doubleValue()/Nr.doubleValue();
				break;
			}
		}
		outValue.set(ngram+"\t"+gtCount+"\t"+temp);
		if(order==1){
			outKey.set("unigram");

			context.write(outKey,outValue);
		}else{
			String prefix= StrIntercept.getPrefix(ngram);
			outKey.set(prefix);
			context.write(outKey,outValue);
		}
	}
}
