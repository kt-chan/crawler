package org.apache.nutch.parse.html.handler;

import java.util.ArrayList;
import java.util.List;

import org.apache.nutch.parse.Outlink;

public class ParserQunarHandler {

	public void transform(ArrayList<Outlink> outlinks) {
		List<Outlink> newOutlinks = new ArrayList<Outlink>();

		for (int i = 0; i < outlinks.size(); i++) {
			Outlink link = outlinks.get(i);
			if (!link.getToUrl().endsWith("/#")) {
				newOutlinks.add(link);
			}
		}

		outlinks.clear();

		for (int i = 0; i < newOutlinks.size(); i++) {
			outlinks.add(newOutlinks.get(i));
		}

	}
}
