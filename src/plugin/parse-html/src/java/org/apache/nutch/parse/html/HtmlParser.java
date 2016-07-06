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

package org.apache.nutch.parse.html;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.html.dom.HTMLDocumentImpl;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.HtmlParseFilters;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.parse.Parser;
import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.EncodingDetector;
import org.apache.nutch.util.NutchConfiguration;
import org.cyberneko.html.parsers.DOMFragmentParser;
import org.jsoup.Jsoup;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DOMException;
import org.w3c.dom.DocumentFragment;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import me.ktchan.crawler.util.DebugWriterUtil;

public class HtmlParser implements Parser {
	public static final Logger LOG = LoggerFactory.getLogger("org.apache.nutch.parse.html");

	// I used 1000 bytes at first, but found that some documents have
	// meta tag well past the first 1000 bytes.
	// (e.g. http://cn.promo.yahoo.com/customcare/music.html)
	// NUTCH-2042 (cf. TIKA-357): increased to 8 kB
	private static final int CHUNK_SIZE = 8192;

	// NUTCH-1006 Meta equiv with single quotes not accepted
	private static Pattern metaPattern = Pattern.compile("<meta\\s+([^>]*http-equiv=(\"|')?content-type(\"|')?[^>]*)>",
			Pattern.CASE_INSENSITIVE);
	private static Pattern charsetPattern = Pattern.compile("charset=\\s*([a-z][_\\-0-9a-z]*)",
			Pattern.CASE_INSENSITIVE);
	private static Pattern charsetPatternHTML5 = Pattern
			.compile("<meta\\s+charset\\s*=\\s*[\"']?([a-z][_\\-0-9a-z]*)[^>]*>", Pattern.CASE_INSENSITIVE);

	private String parserImpl;
	private URL baseUrl;

	private String defaultCharEncoding;

	private Configuration conf;

	private DOMContentUtils utils;

	private HtmlParseFilters htmlParseFilters;

	private String cachingPolicy;

	private static String attributeFile = null;

	private static final Map<String, List<JsoupRule>> rules = new HashMap<String, List<JsoupRule>>();

	/**
	 * Given a <code>byte[]</code> representing an html file of an
	 * <em>unknown</em> encoding, read out 'charset' parameter in the meta tag
	 * from the first <code>CHUNK_SIZE</code> bytes. If there's no meta tag for
	 * Content-Type or no charset is specified, the content is checked for a
	 * Unicode Byte Order Mark (BOM). This will also cover non-byte oriented
	 * character encodings (UTF-16 only). If no character set can be determined,
	 * <code>null</code> is returned. <br />
	 * See also
	 * http://www.w3.org/International/questions/qa-html-encoding-declarations,
	 * http://www.w3.org/TR/2011/WD-html5-diff-20110405/#character-encoding, and
	 * http://www.w3.org/TR/REC-xml/#sec-guessing
	 * 
	 * @param content
	 *            <code>byte[]</code> representation of an html file
	 */

	private static String sniffCharacterEncoding(byte[] content) {
		int length = content.length < CHUNK_SIZE ? content.length : CHUNK_SIZE;

		// We don't care about non-ASCII parts so that it's sufficient
		// to just inflate each byte to a 16-bit value by padding.
		// For instance, the sequence {0x41, 0x82, 0xb7} will be turned into
		// {U+0041, U+0082, U+00B7}.
		String str = new String(content, 0, length, StandardCharsets.US_ASCII);

		Matcher metaMatcher = metaPattern.matcher(str);
		String encoding = null;
		if (metaMatcher.find()) {
			Matcher charsetMatcher = charsetPattern.matcher(metaMatcher.group(1));
			if (charsetMatcher.find())
				encoding = new String(charsetMatcher.group(1));
		}
		if (encoding == null) {
			// check for HTML5 meta charset
			metaMatcher = charsetPatternHTML5.matcher(str);
			if (metaMatcher.find()) {
				encoding = new String(metaMatcher.group(1));
			}
		}
		if (encoding == null) {
			// check for BOM
			if (content.length >= 3 && content[0] == (byte) 0xEF && content[1] == (byte) 0xBB
					&& content[2] == (byte) 0xBF) {
				encoding = "UTF-8";
			} else if (content.length >= 2) {
				if (content[0] == (byte) 0xFF && content[1] == (byte) 0xFE) {
					encoding = "UTF-16LE";
				} else if (content[0] == (byte) 0xFE && content[1] == (byte) 0xFF) {
					encoding = "UTF-16BE";
				}
			}
		}

		return encoding;
	}

	public ParseResult getParse(Content content) {
		HTMLMetaTags metaTags = new HTMLMetaTags();

		try {
			baseUrl = new URL(content.getBaseUrl());
		} catch (MalformedURLException e) {
			return new ParseStatus(e).getEmptyParseResult(content.getUrl(), getConf());
		}

		String text = "";
		String title = "";
		Outlink[] outlinks = new Outlink[0];
		Metadata metadata = new Metadata();

		// parse the content
		DocumentFragment root;
		try {
			byte[] contentInOctets = content.getContent();
			InputSource input = new InputSource(new ByteArrayInputStream(contentInOctets));

			EncodingDetector detector = new EncodingDetector(conf);
			detector.autoDetectClues(content, true);
			detector.addClue(sniffCharacterEncoding(contentInOctets), "sniffed");
			String encoding = detector.guessEncoding(content, defaultCharEncoding);

			metadata.set(Metadata.ORIGINAL_CHAR_ENCODING, encoding);
			metadata.set(Metadata.CHAR_ENCODING_FOR_CONVERSION, encoding);

			input.setEncoding(encoding);
			if (LOG.isTraceEnabled()) {
				LOG.trace("Parsing...");
			}
			root = parse(input);
		} catch (IOException e) {
			return new ParseStatus(e).getEmptyParseResult(content.getUrl(), getConf());
		} catch (DOMException e) {
			return new ParseStatus(e).getEmptyParseResult(content.getUrl(), getConf());
		} catch (SAXException e) {
			return new ParseStatus(e).getEmptyParseResult(content.getUrl(), getConf());
		} catch (Exception e) {
			LOG.error("Error: ", e);
			return new ParseStatus(e).getEmptyParseResult(content.getUrl(), getConf());
		}

		// get meta directives
		HTMLMetaProcessor.getMetaTags(metaTags, root, baseUrl);
		if (LOG.isTraceEnabled()) {
			LOG.trace("Meta tags for " + baseUrl + ": " + metaTags.toString());
		}
		// check meta directives
		if (!metaTags.getNoIndex()) { // okay to index
			StringBuffer sb = new StringBuffer();
			if (LOG.isTraceEnabled()) {
				LOG.trace("Getting text...");
			}
			utils.getText(sb, root); // extract text
			text = sb.toString();
			sb.setLength(0);
			if (LOG.isTraceEnabled()) {
				LOG.trace("Getting title...");
			}
			utils.getTitle(sb, root); // extract title
			title = sb.toString().trim();
		}

		if (!metaTags.getNoFollow()) { // okay to follow links
			ArrayList<Outlink> l = new ArrayList<Outlink>(); // extract outlinks
			URL baseTag = utils.getBase(root);
			if (LOG.isTraceEnabled()) {
				LOG.trace("Getting links...");
			}
			utils.getOutlinks(baseTag != null ? baseTag : baseUrl, l, root);
			outlinks = l.toArray(new Outlink[l.size()]);
			if (LOG.isTraceEnabled()) {
				LOG.trace("found " + outlinks.length + " outlinks in " + content.getUrl());
			}
		}

		ParseStatus status = new ParseStatus(ParseStatus.SUCCESS);
		if (metaTags.getRefresh()) {
			status.setMinorCode(ParseStatus.SUCCESS_REDIRECT);
			status.setArgs(
					new String[] { metaTags.getRefreshHref().toString(), Integer.toString(metaTags.getRefreshTime()) });
		}
		ParseData parseData = new ParseData(status, title, outlinks, content.getMetadata(), metadata);
		ParseResult parseResult = ParseResult.createParseResult(content.getUrl(), new ParseImpl(text, parseData));

		// Parse tagFields metadata
		Parse parse = parseResult.get(content.getUrl());
		String html = new String(content.getContent());

		for (Map.Entry<String, List<JsoupRule>> entry : rules.entrySet()) {
			String domain = entry.getKey();
			String baseUrl = content.getBaseUrl();
			if (baseUrl.contains(domain)) {
				List<JsoupRule> jsoupRules = entry.getValue();
				for (JsoupRule jsoupRule : jsoupRules) {
					if (matches(html, jsoupRule, parse.getData().getTagFieldMeta())) {
						parse.getData().getParseMeta().add("fieldMeta", "true");
					} else {
						parse.getData().getParseMeta().add("fieldMeta", "false");
					}
				}
			}
		}

		// run filters on parse
		ParseResult filteredParse = this.htmlParseFilters.filter(content, parseResult, metaTags, root);
		if (metaTags.getNoCache()) { // not okay to cache
			for (Map.Entry<org.apache.hadoop.io.Text, Parse> entry : filteredParse)
				entry.getValue().getData().getParseMeta().set(Nutch.CACHING_FORBIDDEN_KEY, cachingPolicy);
		}

		return filteredParse;
	}

	private DocumentFragment parse(InputSource input) throws Exception {

		if (parserImpl.equalsIgnoreCase("jsoup"))
			return parseJSoup(input);
		else if (parserImpl.equalsIgnoreCase("tagsoup"))
			return parseTagSoup(input);
		else
			return parseNeko(input);
	}

	private DocumentFragment parseJSoup(InputSource input) throws Exception {
		DocumentFragment frag = JsoupDOMBuilder
				.jsoup2HTML(Jsoup.parse(input.getByteStream(), defaultCharEncoding, baseUrl.getHost()));
		return frag;
	}

	private DocumentFragment parseTagSoup(InputSource input) throws Exception {
		HTMLDocumentImpl doc = new HTMLDocumentImpl();
		DocumentFragment frag = doc.createDocumentFragment();
		DOMBuilder builder = new DOMBuilder(doc, frag);
		org.ccil.cowan.tagsoup.Parser parser = new org.ccil.cowan.tagsoup.Parser();
		parser.setContentHandler(builder);
		parser.setFeature(org.ccil.cowan.tagsoup.Parser.ignoreBogonsFeature, true);
		parser.setFeature(org.ccil.cowan.tagsoup.Parser.bogonsEmptyFeature, false);
		parser.setProperty("http://xml.org/sax/properties/lexical-handler", builder);
		parser.parse(input);
		return frag;
	}

	private DocumentFragment parseNeko(InputSource input) throws Exception {
		DOMFragmentParser parser = new DOMFragmentParser();
		try {
			parser.setFeature("http://cyberneko.org/html/features/scanner/allow-selfclosing-iframe", true);
			parser.setFeature("http://cyberneko.org/html/features/augmentations", true);
			parser.setProperty("http://cyberneko.org/html/properties/default-encoding", defaultCharEncoding);
			parser.setFeature("http://cyberneko.org/html/features/scanner/ignore-specified-charset", true);
			parser.setFeature("http://cyberneko.org/html/features/balance-tags/ignore-outside-content", false);
			parser.setFeature("http://cyberneko.org/html/features/balance-tags/document-fragment", true);
			// @@Test
			// parser.setFeature("http://cyberneko.org/html/features/report-errors",
			// LOG.isTraceEnabled());
			parser.setFeature("http://cyberneko.org/html/features/report-errors", true);
		} catch (SAXException e) {
			e.printStackTrace();
		}

		// @@Test
		InputStream inputbyes = input.getByteStream();
		inputbyes.reset();
		int n = inputbyes.available();
		byte[] bytes = new byte[n];
		inputbyes.read(bytes, 0, n);
		DebugWriterUtil.write(new String(bytes, StandardCharsets.UTF_8), "./debug.txt");

		// convert Document to DocumentFragment
		HTMLDocumentImpl doc = new HTMLDocumentImpl();
		doc.setErrorChecking(false);
		DocumentFragment res = doc.createDocumentFragment();
		DocumentFragment frag = doc.createDocumentFragment();
		parser.parse(input, frag);
		res.appendChild(frag);

		try {
			while (true) {
				frag = doc.createDocumentFragment();
				parser.parse(input, frag);
				if (!frag.hasChildNodes())
					break;
				if (LOG.isInfoEnabled()) {
					LOG.info(" - new frag, " + frag.getChildNodes().getLength() + " nodes.");
				}
				res.appendChild(frag);
			}
		} catch (Exception e) {
			LOG.error("Error: ", e);
		}

		return res;
	}

	private boolean matches(String html, JsoupRule jsoupRule, Metadata fieldMeta) {

		boolean matched = false;
		String element = jsoupRule.element;
		String attribute = jsoupRule.attribute;
		Pattern pattern = jsoupRule.regex;

		if (html != null && attribute != null) {

			org.jsoup.nodes.Document jDoc = Jsoup.parse(html);
			Elements jElements = jDoc.select(element);

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

						fieldMeta.addAll(content, outputs);
						matched = true;
					}
				}
			}
		}

		return matched;
	}

	public static void main(String[] args) throws Exception {
		// LOG.setLevel(Level.FINE);
		String name = args[0];
		String url = "file:" + name;
		File file = new File(name);
		byte[] bytes = new byte[(int) file.length()];
		DataInputStream in = new DataInputStream(new FileInputStream(file));
		in.readFully(bytes);
		Configuration conf = NutchConfiguration.create();
		HtmlParser parser = new HtmlParser();
		parser.setConf(conf);
		Parse parse = parser.getParse(new Content(url, url, bytes, "text/html", new Metadata(), conf)).get(url);
		System.out.println("data: " + parse.getData());

		System.out.println("text: " + parse.getText());
		in.close();
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
		this.htmlParseFilters = new HtmlParseFilters(getConf());
		this.parserImpl = getConf().get("parser.html.impl", "neko");
		this.defaultCharEncoding = getConf().get("parser.character.encoding.default", "windows-1252");
		this.utils = new DOMContentUtils(conf);
		this.cachingPolicy = getConf().get("parser.caching.forbidden.policy", Nutch.CACHING_FORBIDDEN_CONTENT);

		// get the extensions for domain urlfilter
		String pluginName = "parse-html";
		Extension[] extensions = PluginRepository.get(conf).getExtensionPoint(Parser.class.getName())
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
		if (file == null && attributeFile != null) {
			file = attributeFile;
		}

		Reader reader = conf.getConfResourceAsReader(file);

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

				String domain = parts[0].trim();
				String element = parts[1].trim();
				String source = parts[2].trim();
				String regex = parts[3].trim();
				String[] selectors = parts[4].trim().split(",");

				addRules(domain, new JsoupRule(element, source, regex, selectors));
			}
		}

	}

	private synchronized void addRules(String domain, JsoupRule jsoupRule) {
		if (rules.containsKey(domain)) {
			rules.get(domain).add(jsoupRule);
		} else {
			List<JsoupRule> jsoupRules = new ArrayList<JsoupRule>();
			jsoupRules.add(jsoupRule);
			rules.put(domain, jsoupRules);
		}
	}

	private static class JsoupRule {
		public JsoupRule(String element, String attribute, String regex, String[] selectorPairs) {

			if (regex.equals("*"))
				regex = "\\S";
			this.element = element;
			this.attribute = attribute;
			this.regex = Pattern.compile(regex);
			this.selectorPairs = selectorPairs;
		}

		public String element;
		public String attribute;
		public Pattern regex;
		public String[] selectorPairs;
	}
}