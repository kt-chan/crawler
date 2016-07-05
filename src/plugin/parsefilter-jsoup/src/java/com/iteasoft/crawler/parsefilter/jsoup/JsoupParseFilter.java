/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.iteasoft.crawler.parsefilter.jsoup;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.HtmlParseFilter;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.protocol.Content;
import org.jsoup.Jsoup;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;

import me.ktchan.crawler.util.DebugWriterUtil;

/**
 * RegexParseFilter. If a regular expression matches either HTML or extracted
 * text, a configurable field is set to true.
 */
public class JsoupParseFilter implements HtmlParseFilter {

	private static final Logger LOG = LoggerFactory.getLogger(JsoupParseFilter.class);
	private static String attributeFile = null;
	private String regexFile = null;

	private Configuration conf;

	private static final Map<String, JsoupRule> rules = new HashMap<String, JsoupRule>();

	public JsoupParseFilter() {
	}

	public JsoupParseFilter(String regexFile) {
		this.regexFile = regexFile;
	}

	public ParseResult filter(Content content, ParseResult parseResult, HTMLMetaTags metaTags, DocumentFragment doc) {
		Parse parse = parseResult.get(content.getUrl());
		String html = new String(content.getContent());

		for (Map.Entry<String, JsoupRule> entry : rules.entrySet()) {
			String field = entry.getKey();
			JsoupRule jsoupRule = entry.getValue();

//			// @@Test
//			DebugWriterUtil.delete("./test/debug.txt");
			Map<String, String[]> metadata = new HashMap<String, String[]>();
			if (matches(html, field, jsoupRule, metadata)) {
				parse.getData().getFields().putAll(metadata);
			}

//			// @@Test
//			for (Entry<String, String[]> e : parse.getData().getFields().entrySet()) {
//				DebugWriterUtil.write("\n", "./test/debug.txt");
//				String output = "";
//				for (String s : e.getValue()) {
//					if (output.trim().isEmpty())
//						output = s;
//					else
//						output = output + "," + s;
//				}
//				DebugWriterUtil.write(e.getKey() + "::" + output, "./test/debug.txt");
//			}
		}

		return parseResult;
	}

	public void setConf(Configuration conf) {
		this.conf = conf;

		// get the extensions for domain urlfilter
		String pluginName = "parsefilter-jsoup";
		Extension[] extensions = PluginRepository.get(conf).getExtensionPoint(HtmlParseFilter.class.getName())
				.getExtensions();
		for (int i = 0; i < extensions.length; i++) {
			Extension extension = extensions[i];
			if (extension.getDescriptor().getPluginId().equals(pluginName)) {
				attributeFile = extension.getAttribute("file");
				break;
			}
		}

		// handle blank non empty input
		if (attributeFile != null && attributeFile.trim().equals("")) {
			attributeFile = null;
		}

		if (attributeFile != null) {
			if (LOG.isInfoEnabled()) {
				LOG.info("Attribute \"file\" is defined for plugin " + pluginName + " as " + attributeFile);
			}
		} else {
			if (LOG.isWarnEnabled()) {
				LOG.warn("Attribute \"file\" is not defined in plugin.xml for plugin " + pluginName);
			}
		}

		// domain file and attribute "file" take precedence if defined
		String file = conf.get("parsefilter.jsoup.file");
		String stringRules = conf.get("parsefilter.jsoup.rules");
		if (regexFile != null) {
			file = regexFile;
		} else if (attributeFile != null) {
			file = attributeFile;
		}
		Reader reader = null;
		if (stringRules != null) { // takes precedence over files
			reader = new StringReader(stringRules);
		} else {
			reader = conf.getConfResourceAsReader(file);
		}
		try {
			if (reader == null) {
				reader = new FileReader(file);
			}
			readConfiguration(reader);
		} catch (IOException e) {
			LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
		}
	}

	public Configuration getConf() {
		return this.conf;
	}

	private boolean matches(String html, String field, JsoupRule jsoupRule, Map<String, String[]> metadata) {

		boolean matched = false;
		String attribute = jsoupRule.attribute;
		Pattern pattern = jsoupRule.regex;

		if (html != null && attribute != null) {

			org.jsoup.nodes.Document jDoc = Jsoup.parse(html);
			Elements jElements = jDoc.select(field);

			for (org.jsoup.nodes.Element e1 : jElements) {

				String content = e1.attr(attribute);

				if (content != null && content.length() != 0) {

					Matcher matcher = pattern.matcher(content);

					if (matcher.find()) {

						// @@Test Extract selected metatag
						String[] outputs = new String[jsoupRule.selectorPairs.length];

						int i = 0;
						for (String selectorPair : jsoupRule.selectorPairs) {

							String[] selectorAttr = selectorPair.split(":");
							Elements selected = e1.select(selectorAttr[0]);
							if (selected.size() > 0) {
								String key = selectorAttr[0].replaceAll("\\[", "").replaceAll("\\]", "")
										.replaceAll("\\.", "");

								if (selectorAttr[1].equals("*") && !selected.text().trim().isEmpty()) {
									outputs[i] = key + ":" + selected.text();
								}

								if (!selectorAttr[1].equals("*") && !selected.attr(selectorAttr[1]).trim().isEmpty()) {
									outputs[i] = key + ":" + selected.attr(selectorAttr[1]);
								}

							}

							i++;
						}

						metadata.put(content, outputs);
						matched = true;
					}
				}
			}
		}

		return matched;
	}

	private synchronized void readConfiguration(Reader configReader) throws IOException {
		if (rules.size() > 0) {
			return;
		}

		String line;
		BufferedReader reader = new BufferedReader(configReader);
		while ((line = reader.readLine()) != null) {
			if (StringUtils.isNotBlank(line) && !line.startsWith("#")) {
				line = line.trim();
				String[] parts = line.split("\t");

				String field = parts[0].trim();
				String source = parts[1].trim();
				String regex = parts[2].trim();
				String[] selectors = parts[3].trim().split(",");

				rules.put(field, new JsoupRule(source, regex, selectors));
			}
		}
	}

	private static class JsoupRule {
		public JsoupRule(String attribute, String regex, String[] selectorPairs) {

			if (regex.equals("*"))
				regex = "\\S";
			this.attribute = attribute;
			this.regex = Pattern.compile(regex);
			this.selectorPairs = selectorPairs;
		}

		public String attribute;
		public Pattern regex;
		public String[] selectorPairs;
	}
}