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
	private final char SEPARATOR='▲';
	private int order;
	private Text resKey = new Text();
	private VIntWritable one = new VIntWritable(1);
	private StringBuffer ngramSb;
	private int lrLabel=0;//katz 从左边还是右边算起的标志  如果小于等于0则是正规katz  否则是katz反转
	//lrLabel 0  正常
	//lrLabel 1  左边
	//lrLabel 2 右边
	//lrLabel 3 中间


	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		order=conf.getInt("order",3);
		lrLabel=conf.getInt("lrLabel", 0);

	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		value=transformText2UTF8(value,"gbk");
		String line=value.toString();
		String[] words=line.split("\\s+");
		for(int i=1;i<=order;i++){
			for(int j=0;j<=words.length-i;j++){
				ngramSb= new StringBuffer(words[j]);
				if (words[j].length() != 0) {
					for (int k = j + 1; k < j + i; k++) {
						ngramSb.append(" "+words[k]);
					}
					/*resKey.set(ngramSb.toString().trim());
					context.write(resKey, one);*/
					String ngram=ngramSb.toString().trim();

					if(lrLabel==0){//正常
						resKey.set(ngram);
						context.write(resKey, one);
					}else if(lrLabel==1){//左半部分


						int wordNum = ngram.split(" ").length;
						int len=ngram.length();
						if(wordNum==1){
							if(ngram.charAt(0)==SEPARATOR){
								resKey.set(ngram);
								context.write(resKey, one);
							}
						}else{
							int rightIndex=0;
							if(leftJudge(ngram, rightIndex, SEPARATOR,lrLabel)){
								resKey.set(ngram);
								context.write(resKey, one);
							}
						}


					}else if(lrLabel==2){//右半部分

						int wordNum = ngram.split(" ").length;
						int len=ngram.length();
						if(wordNum==1){
							if(ngram.charAt(0)==SEPARATOR){
								resKey.set(ngram);
								context.write(resKey, one);
							}
						}else{
							int rightIndex=len-1;
							if(leftJudge(ngram, rightIndex, SEPARATOR,lrLabel)){
								resKey.set(ngram);
								context.write(resKey, one);
							}
						}

					}else if(lrLabel==3){
						//int len=ngram.split(" ").length;
						int wordNum = ngram.split(" ").length;

						if(wordNum<=2){
							int index=ngram.indexOf(SEPARATOR);
							if(index!=-1){
								resKey.set(ngram);
								context.write(resKey, one);
							}
						}else if(wordNum==4){
							boolean isCount = fourthJudge(ngram, SEPARATOR);
							if(isCount){
								resKey.set(ngram);
								context.write(resKey, one);
							}
						}else if(wordNum==5){
							int rightIndex=2;
							if(leftJudge(ngram, rightIndex, SEPARATOR,lrLabel)){
								resKey.set(ngram);
								context.write(resKey, one);
							}
						}
					}else{
						break;
					}
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

	public static int getCharNum(String str,char ch,int start,int end){
		int count=0;
		for(int i=start;i<end;i++){
			if(str.charAt(i)==ch){
				count++;
			}
		}

		return count;
	}
	public static boolean fourthJudge(String ngram,char separator){
		int length=ngram.length();

		int firstIndex=ngram.indexOf(separator);
		int lastIndex=ngram.lastIndexOf(separator);
		boolean temp=firstIndex==0&&lastIndex==ngram.length()-1?false:true;
		int start=1;
		int end=length-1;
		int count=getCharNum(ngram,separator,start,end);
		if(temp&&count>0)
			return true;
		return false;
	}
	public static boolean fifthJudge(String ngram,char separator,int index){
		int count=getCharNum(ngram, separator, 0, ngram.length());
		if(count==1&&ngram.charAt(index)==separator)
			return true;
		return false;

	}
	public static boolean leftJudge(String ngram,int rightIndex,char separator,int lrLabel){
		int firstIndex=ngram.indexOf(separator);
		int lastIndex=ngram.lastIndexOf(separator);
		boolean condition=false;
		if(lrLabel==1){
			condition=firstIndex==lastIndex&&firstIndex==rightIndex;
		}else if(lrLabel==2){
			condition=firstIndex==lastIndex&&firstIndex==rightIndex;
		}else if(lrLabel==3){
			String temp[]=ngram.split(" ");
			String middleStr=temp[rightIndex];
			condition=firstIndex==lastIndex&&middleStr.charAt(0)==separator;
		}

		if(condition)
			return true;
		return false;
	}
}
