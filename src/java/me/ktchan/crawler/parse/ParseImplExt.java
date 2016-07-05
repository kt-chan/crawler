package me.ktchan.crawler.parse;

import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseText;

public class ParseImplExt extends ParseImpl implements ParseExt{

	private ParseFields fields;

	public ParseImplExt(String text, ParseFields fields, ParseData data, boolean isCanonical) {
		super(new ParseText(text), data, isCanonical);
		if (fields != null)
			this.fields = fields;
	}

	public ParseImplExt(ParseText text, ParseFields fields, ParseData data, boolean isCanonical) {
		super(text, data, isCanonical);
		if (fields != null)
			this.fields = fields;
	}

	public ParseImplExt(String text, ParseFields fields, ParseData data) {
		this(new ParseText(text), fields, data, true);
	}

	public ParseImplExt(ParseText text, ParseFields fields, ParseData data) {
		this(text, fields, data, true);
	}

	public ParseImplExt(String text, ParseData data) {
		this(text, null, data);
	}

	public ParseImplExt(ParseText text, ParseData data) {
		this(text, null, data);
	}

	public ParseFields getFields() {
		return this.fields;
	}

}
