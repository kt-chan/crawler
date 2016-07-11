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
package org.apache.nutch.protocol.interactiveselenium;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

//HTTP Client imports
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpVersion;
import org.apache.commons.httpclient.cookie.CookiePolicy;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.hadoop.conf.Configuration;
//Nutch imports
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.SpellCheckedMetadata;
import org.apache.nutch.net.protocols.HttpDateFormat;
import org.apache.nutch.net.protocols.Response;
import org.apache.nutch.protocol.ProtocolException;
import org.apache.nutch.protocol.http.api.HttpBase;
import org.apache.nutch.protocol.interactiveselenium.handlers.InteractiveHandlerFactory;
import org.apache.nutch.protocol.interactiveselenium.handlers.InteractiveSeleniumHandler;
import org.apache.nutch.protocol.selenium.HttpWebClient;
import org.openqa.selenium.WebDriver;

/* Most of this code was borrowed from protocol-httpclient */

public class HttpResponse implements Response {

	private URL url;
	private byte[] content;
	private int code;
	private Metadata headers = new SpellCheckedMetadata();
	private static InteractiveSeleniumHandler[] handlers;

	/** The nutch configuration */
	private Configuration conf = null;

	public HttpResponse(Http http, URL url, CrawlDatum datum, boolean followRedirects)
			throws ProtocolException, IOException {

		// Prepare GET method for HTTP request
		this.conf = http.getConf();
		this.url = url;

		GetMethod get = new GetMethod(url.toString());
		get.setFollowRedirects(followRedirects);
		get.setDoAuthentication(true);
		if (http.isIfModifiedSinceEnabled() && datum.getModifiedTime() > 0) {
			get.setRequestHeader("If-Modified-Since", HttpDateFormat.toString(datum.getModifiedTime()));
		}

		// Set HTTP parameters
		HttpMethodParams params = get.getParams();
		if (http.getUseHttp11()) {
			params.setVersion(HttpVersion.HTTP_1_1);
		} else {
			params.setVersion(HttpVersion.HTTP_1_0);
		}
		params.makeLenient();
		params.setContentCharset("UTF-8");
		params.setCookiePolicy(CookiePolicy.BROWSER_COMPATIBILITY);
		params.setBooleanParameter(HttpMethodParams.SINGLE_COOKIE_HEADER, true);

		try {
			HttpClient client = Http.getClient();
			client.getParams().setParameter("http.useragent", http.getUserAgent()); // NUTCH-1941
			code = client.executeMethod(get);

			Header[] heads = get.getResponseHeaders();

			for (int i = 0; i < heads.length; i++) {
				headers.set(heads[i].getName(), heads[i].getValue());
			}

			// Limit download size
			int contentLength = Integer.MAX_VALUE;
			String contentLengthString = headers.get(Response.CONTENT_LENGTH);
			if (contentLengthString != null) {
				try {
					contentLength = Integer.parseInt(contentLengthString.trim());
				} catch (NumberFormatException ex) {
					throw new HttpException("bad content length: " + contentLengthString);
				}
			}
			if (http.getMaxContent() >= 0 && contentLength > http.getMaxContent()) {
				contentLength = http.getMaxContent();
			}

			// always read content. Sometimes content is useful to find a cause
			// for error.
			InputStream in = get.getResponseBodyAsStream();
			try {
				byte[] buffer = new byte[HttpBase.BUFFER_SIZE];
				int bufferFilled = 0;
				int totalRead = 0;
				ByteArrayOutputStream out = new ByteArrayOutputStream();

				// Get Content type header
				String contentType = headers.get(Response.CONTENT_TYPE);
				if (contentType != null) {
					if (contentType.contains("text/html") || contentType.contains("application/xhtml")) {
						content = readPlainContent(url);
					} else {
						while ((bufferFilled = in.read(buffer, 0, buffer.length)) != -1
								&& totalRead + bufferFilled <= contentLength) {
							totalRead += bufferFilled;
							out.write(buffer, 0, bufferFilled);
						}

						content = out.toByteArray();

					}
				}

			} catch (Exception e) {
				if (code == 200)
					throw new IOException(e.toString());
				// for codes other than 200 OK, we are fine with empty content
			} finally {
				if (in != null) {
					in.close();
				}
				get.abort();
			}

			StringBuilder fetchTrace = null;
			if (Http.LOG.isTraceEnabled()) {
				// Trace message
				fetchTrace = new StringBuilder(
						"url: " + url + "; status code: " + code + "; bytes received: " + content.length);
				if (getHeader(Response.CONTENT_LENGTH) != null)
					fetchTrace.append("; Content-Length: " + getHeader(Response.CONTENT_LENGTH));
				if (getHeader(Response.LOCATION) != null)
					fetchTrace.append("; Location: " + getHeader(Response.LOCATION));
			}
			// Extract gzip, x-gzip and deflate content
			if (content != null) {
				// check if we have to uncompress it
				String contentEncoding = headers.get(Response.CONTENT_ENCODING);
				if (contentEncoding != null && Http.LOG.isTraceEnabled())
					fetchTrace.append("; Content-Encoding: " + contentEncoding);
				if ("gzip".equals(contentEncoding) || "x-gzip".equals(contentEncoding)) {
					content = http.processGzipEncoded(content, url);
					if (Http.LOG.isTraceEnabled())
						fetchTrace.append("; extracted to " + content.length + " bytes");
				} else if ("deflate".equals(contentEncoding)) {
					content = http.processDeflateEncoded(content, url);
					if (Http.LOG.isTraceEnabled())
						fetchTrace.append("; extracted to " + content.length + " bytes");
				}
			}

			// Logger trace message
			if (Http.LOG.isTraceEnabled()) {
				Http.LOG.trace(fetchTrace.toString());
			}

		} finally {
			get.releaseConnection();
		}
	}

	public URL getUrl() {
		return url;
	}

	public int getCode() {
		return code;
	}

	public String getHeader(String name) {
		return headers.get(name);
	}

	public Metadata getHeaders() {
		return headers;
	}

	public byte[] getContent() {
		return content;
	}

	/*
	 * ------------------------- * <implementation:Response> *
	 * -------------------------
	 */
	private void loadSeleniumHandlers() {
		if (handlers != null)
			return;

		String handlerConfig = this.conf.get("interactiveselenium.handlers", "DefaultHandler");
		String[] handlerNames = handlerConfig.split(",");
		handlers = new InteractiveSeleniumHandler[handlerNames.length];
		for (int i = 0; i < handlerNames.length; i++) {
			handlers[i] = InteractiveHandlerFactory.getHandler(this.conf, handlerNames[i]);
		}
	}

	private byte[] readPlainContent(URL url) throws IOException {
		if (handlers == null)
			loadSeleniumHandlers();

		String processedPage = "";

		for (InteractiveSeleniumHandler handler : handlers) {
			if (!handler.shouldProcessURL(url.toString())) {
				continue;
			}

			WebDriver driver = HttpWebClient.getDriverForPage(url.toString(), conf);
			processedPage += handler.processDriver(driver);

			HttpWebClient.cleanUpDriver(driver);
		}

		return processedPage.getBytes("UTF-8");
	}

}
