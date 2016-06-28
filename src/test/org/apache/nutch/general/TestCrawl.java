/*
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
package org.apache.nutch.general;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.nutch.crawl.CrawlDBTestUtil;
import org.apache.nutch.crawl.CrawlDb;
import org.apache.nutch.crawl.Generator;
import org.apache.nutch.crawl.Injector;
import org.apache.nutch.crawl.LinkDb;
import org.apache.nutch.crawl.LinkDbReader;
import org.apache.nutch.fetcher.Fetcher;
import org.apache.nutch.indexer.IndexingJob;
import org.apache.nutch.parse.ParseSegment;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Basic injector test: 1. Creates a text file with urls 2. Injects them into
 * crawldb 3. Reads crawldb entries and verifies contents 4. Injects more urls
 * into webdb 5. Reads crawldb entries and verifies contents
 * 
 */
public class TestCrawl {

	private Configuration conf;
	private FileSystem fs;
	final static Path testdir = new Path("./test/");

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

		crawldbPath = new Path(testdir, "crawldb");
		linkdbPath = new Path(testdir, "linkdb");
		dumpPath = new Path(testdir, "dump");
		segPath = new Path(testdir, "segments");
		;

		fs = FileSystem.get(conf);

		if (fs.exists(crawldbPath)) {
			fs.delete(crawldbPath, true);
		}

		if (fs.exists(segPath)) {
			fs.delete(segPath, true);
		}

		if (fs.exists(dumpPath)) {
			fs.delete(dumpPath, true);
		}

		fs.mkdirs(crawldbPath);
		fs.mkdirs(segPath);

		// setup arguments for calling nutch apps
		args_setup();

	}

	@After
	public void tearDown() throws IOException {
		// fs.delete(testdir, true);
	}

	@Test
	/*
	 * http://yuebingzhang.elastos.org/2013/08/01/nutch%E7%AE%80%E4%BB%8Bnutch%
	 * E5%8E%9F%E7%90%86nutchsolr%E6%8A%93%E5%8F%96%E5%B9%B6%E7%B4%A2%E5%BC%95%
	 * E7%BD%91%E9%A1%B5%E7%9A%84%E9%85%8D%E7%BD%AE/ 我们先来介绍crawler的存储数据的文件结构：
	 * 
	 * crawler数据文件主要包括三类，分别是web
	 * database，一系列的segment加上index，三者的物理文件分别存储在爬行结果目录下的db目录下webdb子文件夹内，
	 * segments文件夹和index文件夹。（对于nutch1.7，文件结构有所不同，每次爬虫后，会在nutch根目录下产生一个名为“crawler
	 * +年+月+日+时+分+秒”的文件夹，里面存储的就是这次爬虫产生的所有数据文件，文件夹内部的不同之处我们在下面的详细介绍中加以说明）
	 * 三者存储的信息内容如下：
	 * 
	 * Web database，也叫WebDB，其中存储的是爬虫所抓取网页之间的链接结构信息，
	 * 它只在爬虫Crawler工作中使用而和Searcher的工作没有任何关系。WebDB内存储了两种实体的信息：page和link。
	 * Page实体通过描述网络上一个网页的特征信息来表征一个实际的网页，因为网页有很多个需要描述，
	 * WebDB中通过网页的URL和网页内容的MD5两种索引方法对这些网页实体进行了索引。Page实体描述的网页特征主要包括网页内的link数目，
	 * 抓取此网页的时间等相关抓取信息，对此网页的重要度评分等。同样的，Link实体描述的是两个page实体之间的链接关系。
	 * WebDB构成了一个所抓取网页的链接结构图，这个图中Page实体是图的结点，而Link实体则代表图的边。（我在实践中使用的是nutch1.7+
	 * solr4.4，其中将WebDB分为crawlerdb和linkdb两个部分）
	 * 
	 * 一次爬行会产生很多个segment，每个segment内存储的是爬虫Crawler在单独一次抓取循环中抓到的网页以及这些网页的索引。
	 * Crawler爬行时会根据WebDB中的link关系按照一定的爬行策略生成每次抓取循环所需的fetchlist，
	 * 然后Fetcher通过fetchlist中的URLs抓取这些网页并索引，然后将其存入segment。Segment是有时限的，
	 * 当这些网页被Crawler重新抓取后，先前抓取产生的segment就作废了。在存储中。Segment文件夹是以产生时间命名的，
	 * 方便我们删除作废的segments以节省存储空间。
	 * 
	 * Index是Crawler抓取的所有网页的索引，它是通过对所有单个segment中的索引进行合并处理所得的。Nutch利用Lucene技术进行索引
	 * ，所以Lucene中对索引进行操作的接口对Nutch中的index同样有效。但是需要注意的是，Lucene中的segment和Nutch中的不同，
	 * Lucene中的segment是索引index的一部分，但是Nutch中的segment只是WebDB中各个部分网页的内容和索引，
	 * 最后通过其生成的index跟这些segment已经毫无关系了。（由于我们使用solr来实现查询，
	 * 所以在nutch的数据文件中找不到index这个文件夹，它被存入到solr的文件目录下）
	 * 
	 * 在清楚了crawler的文件结构后，我们来介绍crawler的工作流程：
	 * 
	 * Crawler的工作原理主要是：首先Crawler根据WebDB生成一个待抓取网页的URL集合叫做Fetchlist，
	 * 接着下载线程Fetcher开始根据Fetchlist将网页抓取回来，如果下载线程有很多个，那么就生成很多个Fetchlist，
	 * 也就是一个Fetcher对应一个Fetchlist。然后Crawler根据抓取回来的网页WebDB进行更新，
	 * 根据更新后的WebDB生成新的Fetchlist，里面是未抓取的或者新发现的URLs，然后下一轮抓取循环重新开始。这个循环过程可以叫做“产生/抓取
	 * /更新”循环。
	 * 
	 * 指向同一个主机上Web资源的URLs通常被分配到同一个Fetchlist中，
	 * 这样的话防止过多的Fetchers对一个主机同时进行抓取造成主机负担过重。另外Nutch遵守Robots Exclusion
	 * Protocol，网站可以通过自定义Robots.txt控制Crawler的抓取。
	 * 
	 * 在Nutch中，Crawler操作的实现是通过一系列子操作的实现来完成的。这些子操作Nutch都提供了子命令行可以单独进行调用。
	 * 下面就是这些子操作的功能描述以及命令行，命令行在括号中。
	 * 
	 * 1. 创建一个新的WebDb (admin db -create).
	 * 
	 * 2. 将抓取起始URLs写入WebDB中 (inject).
	 * 
	 * 3. 根据WebDB生成fetchlist并写入相应的segment(generate).
	 * 
	 * 4. 根据fetchlist中的URL抓取网页 (fetch).
	 * 
	 * 5. 根据抓取网页更新WebDb (updatedb).
	 * 
	 * 6. 循环进行3－5步直至预先设定的抓取深度。
	 * 
	 * 7. 根据WebDB得到的网页评分和links更新segments (updatesegs).
	 * 
	 * 8. 对所抓取的网页进行索引(index).
	 * 
	 * 9. 在索引中丢弃有重复内容的网页和重复的URLs (dedup).
	 * 
	 * 10. 将segments中的索引进行合并生成用于检索的最终index(merge).
	 * 
	 * Crawler详细工作流程是：在创建一个WebDB之后(步骤1),
	 * “产生/抓取/更新”循环(步骤3－6)根据一些种子URLs开始启动。当这个循环彻底结束，Crawler根据抓取中生成的segments创建索引（
	 * 步骤7－10）。在进行重复URLs清除（步骤9）之前，每个segment的索引都是独立的（步骤8）。最终，
	 * 各个独立的segment索引被合并为一个最终的索引index（步骤10）。
	 * 
	 * nutch1.7+solr4.4配置
	 * 
	 * 从Nutch 网站下载apache-nutch-1.7-bin.tar.gz 回来解开(~/apache-nutch-1.7/)
	 * 
	 * 把Solr 预设的core 范例collection1 复制为core-nutch
	 * 
	 * 把Nutch 提供的conf/schema-solr4.xml 覆盖掉Solr core-nutch 的conf/schema.xml
	 * 
	 * 将Solr core-nutch conf/schema.xml补上一行漏掉的栏位设定<field name=”_version_”
	 * type=”long” stored=”true” indexed=”true” multiValued=”false”/>
	 * 
	 * 重开Solr，进Web管理介面的Core Admin新增一个core-nutch core，并且不该有这个新增core的错误讯息。
	 * 
	 * 回来~/apache-nutch-1.7/ 设定Nutch。 conf/ 底下先编辑nutch-site.xml
	 * 补上http.agent.name 的crawler 名称设定。
	 * 
	 * 再编辑regex-urlfilter.txt将最后一行+.注解掉，改为+^http://my.site.domain.name只抓我自己的网站
	 * 
	 * 再回到~/apache-nutch-1.7/，新增一个urls/
	 * 目录，里头放一个seed.txt，内容放自己想要抓的种子网址，这里因为只想要抓自己的网站，所以只要放一行http://my.site.domain
	 * .name/就好
	 * 
	 * 接下来跑./bin/nutch crawl urls/ -solr
	 * http://solr.server.name/solr/core-nutch/ -threads 20 -depth 2 -topN
	 * 3测试能不能抓到网站上层的几个网页，到Solr 管理介面里头用查询功能如果有资料，就是成功了。
	 * 
	 */
	public void testInject() throws IOException {

		int rounds = 1;

		Injector injector = new Injector(conf);
		Generator generator = new Generator(conf);
		Fetcher fetcher = new Fetcher(conf);
		ParseSegment parser = new ParseSegment(conf);
		CrawlDb crawldb = new CrawlDb(conf);
		LinkDb linkdb = new LinkDb(conf);
		LinkDbReader linkdbReader = null;

		try {
			injector.run(NUTCH_ARGS.get(MODES.INJECT));

			for (int i = 0; i < rounds; i++) {
				generator.run(NUTCH_ARGS.get(MODES.GENERATE));
				FileStatus[] statuses = fs.listStatus(segPath);

				for (FileStatus status : statuses) {
					NUTCH_ARGS.get(MODES.FETCH)[0] = new Path(segPath.toString(), status.getPath().getName())
							.toString();
					fetcher.run(NUTCH_ARGS.get(MODES.FETCH));
				}

				for (FileStatus status : statuses) {
					NUTCH_ARGS.get(MODES.PARSE)[0] = new Path(segPath.toString(), status.getPath().getName())
							.toString();
					parser.run(NUTCH_ARGS.get(MODES.PARSE));
				}

				crawldb.run(NUTCH_ARGS.get(MODES.UPDATEDB));
			}
			linkdb.run(NUTCH_ARGS.get(MODES.INVERTLINKS));

			// invertlinks

			// indexing to solr
			IndexingJob solrIndexer = new IndexingJob(conf);
			solrIndexer.run(NUTCH_ARGS.get(MODES.SOLRINDEXER));

			// dump out for testing
			linkdbReader = new LinkDbReader(conf, dumpPath);
			linkdbReader.run(NUTCH_ARGS.get(MODES.LINKDBREADER));

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			linkdbReader.close();
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

		String[] args_updatedb = new String[2];
		args_updatedb[0] = crawldbPath.toString();
		args_updatedb[1] = segPath.toString();

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
		args_solrIndexer[4] = "solr.server.url=http://localhost:8983/solr";
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
