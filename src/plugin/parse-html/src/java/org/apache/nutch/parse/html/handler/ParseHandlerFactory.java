package org.apache.nutch.parse.html.handler;

import java.net.URL;
import java.util.ArrayList;

import org.apache.nutch.parse.Outlink;

public class ParseHandlerFactory {

	public static void urlTransform(URL url, ArrayList<Outlink> outlinks) {
		if (url.getHost().contains("qunar.com")) {
			ParserQunarHandler handler = new ParserQunarHandler();
			handler.transform(outlinks);
		}
	}
}
