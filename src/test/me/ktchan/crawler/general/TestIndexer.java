package me.ktchan.crawler.general;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.nutch.crawl.CrawlDBTestUtil;
import org.apache.nutch.indexer.IndexingJob;
import org.apache.nutch.parse.ParseSegment;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import me.ktchan.crawler.general.TestParse.MODES;

public class TestIndexer {

	private Configuration conf;
	private FileSystem fs;
	private final static Path testdir = new Path("./data/test/");
	private final static String solrPath = "http://localhost:8983/solr";
	private final static int rounds = 1;

	public static enum MODES {
		INJECT, GENERATE, FETCH, PARSE, UPDATEDB, INVERTLINKS, LINKDBREADER, SOLRINDEXER
	}

	public final static HashMap<MODES, String[]> NUTCH_ARGS = new HashMap<>();

	Path crawldbPath;
	Path linkdbPath;
	Path dumpPath;
	Path urlPath;
	Path segPath;

	@Before
	public void setUp() throws Exception {
		conf = CrawlDBTestUtil.createConfiguration();
		urlPath = new Path("./seed/lead.txt");

		fs = FileSystem.get(conf);
		
		
		crawldbPath = new Path(testdir, "crawldb");
		linkdbPath = new Path(testdir, "linkdb");
		dumpPath = new Path(testdir, "dump");
		segPath = new Path(testdir, "segments");
		
		// setup arguments for calling nutch apps
		args_setup();

	}

	@After
	public void tearDown() throws IOException {
		// fs.delete(testdir, true);
	}

	@Test
	public void testIndex() throws IOException {

		IndexingJob solrIndexer = new IndexingJob(conf);
		try {
			solrIndexer.run(NUTCH_ARGS.get(MODES.SOLRINDEXER));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void args_setup() {

		String[] args_inject = new String[2];
		args_inject[0] = crawldbPath.toString();
		args_inject[1] = urlPath.toString();

		String[] args_generate = new String[2];
		args_generate[0] = crawldbPath.toString();
		args_generate[1] = segPath.toString();

		String[] args_fitch = new String[2];
		args_fitch[0] = segPath.toString();
		args_fitch[1] = "-noParsing";

		String[] args_parse = new String[1];
		args_parse[0] = segPath.toString();

		String[] args_updatedb = new String[3];
		args_updatedb[0] = crawldbPath.toString();
		args_updatedb[1] = "-dir";
		args_updatedb[2] = segPath.toString();

		String[] args_invertLinks = new String[3];
		args_invertLinks[0] = linkdbPath.toString();
		args_invertLinks[1] = "-dir";
		args_invertLinks[2] = segPath.toString();

		String[] args_linkdbReader = new String[3];
		args_linkdbReader[0] = linkdbPath.toString();
		args_linkdbReader[1] = "-dump";
		args_linkdbReader[2] = dumpPath.toString() + "/linkdbReader";

		String[] args_solrIndexer = new String[7];
		args_solrIndexer[0] = crawldbPath.toString();
		args_solrIndexer[1] = "-linkdb";
		args_solrIndexer[2] = linkdbPath.toString();
		args_solrIndexer[3] = "-params";
		args_solrIndexer[4] = "solr.server.url=" + solrPath;
		args_solrIndexer[5] = "-dir";
		args_solrIndexer[6] = segPath.toString();

		NUTCH_ARGS.put(MODES.INJECT, args_inject);
		NUTCH_ARGS.put(MODES.GENERATE, args_generate);
		NUTCH_ARGS.put(MODES.FETCH, args_fitch);
		NUTCH_ARGS.put(MODES.PARSE, args_parse);
		NUTCH_ARGS.put(MODES.UPDATEDB, args_updatedb);
		NUTCH_ARGS.put(MODES.INVERTLINKS, args_invertLinks);
		NUTCH_ARGS.put(MODES.LINKDBREADER, args_linkdbReader);
		NUTCH_ARGS.put(MODES.SOLRINDEXER, args_solrIndexer);
	}

}
