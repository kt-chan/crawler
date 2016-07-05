package me.ktchan.crawler.parse;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.parse.ParseText;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParseResultExt implements Iterable<Map.Entry<Text, ParseExt>> {
	  protected Map<Text, ParseExt> parseMap;
	  protected String originalUrl;

	  public static final Logger LOG = LoggerFactory.getLogger(ParseResultExt.class);

	  /**
	   * Create a container for parse results.
	   * 
	   * @param originalUrl
	   *          the original url from which all parse results have been obtained.
	   */
	  public ParseResultExt(String originalUrl) {
	    parseMap = new HashMap<Text, ParseExt>();
	    this.originalUrl = originalUrl;
	  }

	  /**
	   * Convenience method for obtaining {@link ParseResult} from a single
	   * <code>Parse</code> output.
	   * 
	   * @param url
	   *          canonical url.
	   * @param parse
	   *          single parse output.
	   * @return result containing the single parse output.
	   */
	  public static ParseResultExt createParseResult(String url, ParseExt parse) {
	    ParseResultExt parseResult = new ParseResultExt(url);
	    parseResult.put(new Text(url),  new ParseFields(), new ParseText(parse.getText()), parse.getData());
	    return parseResult;
	  }

	  /**
	   * Checks whether the result is empty.
	   * 
	   * @return
	   */
	  public boolean isEmpty() {
	    return parseMap.isEmpty();
	  }

	  /**
	   * Return the number of parse outputs (both successful and failed)
	   */
	  public int size() {
	    return parseMap.size();
	  }

	  /**
	   * Retrieve a single parse output.
	   * 
	   * @param key
	   *          sub-url under which the parse output is stored.
	   * @return parse output corresponding to this sub-url, or null.
	   */
	  public Parse get(String key) {
	    return get(new Text(key));
	  }

	  /**
	   * Retrieve a single parse output.
	   * 
	   * @param key
	   *          sub-url under which the parse output is stored.
	   * @return parse output corresponding to this sub-url, or null.
	   */
	  public Parse get(Text key) {
	    return parseMap.get(key);
	  }

	  /**
	   * Store a result of parsing.
	   * 
	   * @param key
	   *          URL or sub-url of this parse result
	   * @param text
	   *          plain text result
	   * @param data
	   *          corresponding parse metadata of this result
	   */
	  public void put(Text key, ParseFields fields, ParseText text, ParseData data) {
	    put(key.toString(), fields, text, data);
	  }

	  /**
	   * Store a result of parsing.
	   * 
	   * @param key
	   *          URL or sub-url of this parse result
	   * @param text
	   *          plain text result
	   * @param data
	   *          corresponding parse metadata of this result
	   */
	  public void put(String key, ParseFields fields, ParseText text, ParseData data) {
	    parseMap.put(new Text(key), new ParseImplExt(text, fields, data, key.equals(originalUrl)));
	  }

	  /**
	   * Iterate over all entries in the &lt;url, Parse&gt; map.
	   */
	  public Iterator<Entry<Text, ParseExt>> iterator() {
	    return parseMap.entrySet().iterator();
	  }

	  /**
	   * Remove all results where status is not successful (as determined by
	   * </code>ParseStatus#isSuccess()</code>). Note that effects of this operation
	   * cannot be reversed.
	   */
	  public void filter() {
	    for (Iterator<Entry<Text, ParseExt>> i = iterator(); i.hasNext();) {
	      Entry<Text, ParseExt> entry = i.next();
	      if (!entry.getValue().getData().getStatus().isSuccess()) {
	        LOG.warn(entry.getKey() + " is not parsed successfully, filtering");
	        i.remove();
	      }
	    }

	  }

	  /**
	   * A convenience method which returns true only if all parses are successful.
	   * Parse success is determined by <code>ParseStatus#isSuccess()</code>.
	   */
	  public boolean isSuccess() {
	    for (Iterator<Entry<Text, ParseExt>> i = iterator(); i.hasNext();) {
	      Entry<Text, ParseExt> entry = i.next();
	      if (!entry.getValue().getData().getStatus().isSuccess()) {
	        return false;
	      }
	    }
	    return true;
	  }
}
