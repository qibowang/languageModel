package com.lm.rawcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.UnsupportedEncodingException;


/**
 * Created by root on 2017/5/24.
 */
public class RawCountMapper extends Mapper<LongWritable,Text,Text,VIntWritable> {
	private int order;
	private Text resKey = new Text();
	private VIntWritable one = new VIntWritable(1);
	private StringBuffer ngramSb;
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		order=conf.getInt("order",3);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		value=transformText2UTF8(value,"gbk");
		String line=value.toString();
		String[] words=line.split("\\s+");
		for(int i=0;i<=order;i++){
			for(int j=0;j<=words.length-i;j++){
				ngramSb= new StringBuffer(words[j]);
				if (words[j].length() != 0) {
					for (int k = j + 1; k < j + i; k++) {
						ngramSb.append(" "+words[k]);
					}
					resKey.set(ngramSb.toString().trim());
					context.write(resKey, one);
				}
			}
		}
	}



	public static Text transformText2UTF8(Text text, String encoding) {
		String value = null;

		try {
			value = new String(text.getBytes(), 0, text.getLength(), encoding);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new Text(value);
	}
}
