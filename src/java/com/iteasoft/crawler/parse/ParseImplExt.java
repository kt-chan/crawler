package com.iteasoft.crawler.parse;

import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseText;

public class ParseImplExt extends ParseImpl {


	public ParseImplExt(String text, ParseData data) {
		super(new ParseText(text), data, true);
	}

	public ParseImplExt(ParseText text, ParseData data) {
		super(text, data, true);
	}

}
