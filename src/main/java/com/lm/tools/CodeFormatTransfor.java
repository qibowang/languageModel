package com.lm.tools;

import org.apache.hadoop.io.Text;

import java.io.UnsupportedEncodingException;

/**
 * Created by root on 2017/5/24.
 */
public class CodeFormatTransfor {
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
