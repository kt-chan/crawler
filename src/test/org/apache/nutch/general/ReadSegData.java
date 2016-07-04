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
import org.apache.nutch.segment.SegmentReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Basic injector test: 1. Creates a text file with urls 2. Injects them into
 * crawldb 3. Reads crawldb entries and verifies contents 4. Injects more urls
 * into webdb 5. Reads crawldb entries and verifies contents
 * 
 */
public class ReadSegData {

	private Configuration conf;
	private FileSystem fs;
	private final static Path testdir = new Path("./test/");
	private final static int rounds = 2;

	public static enum MODES {
		INJECT, GENERATE, FETCH, PARSE, UPDATEDB, INVERTLINKS, LINKDBREADER, SOLRINDEXER
	}

	public final static HashMap<MODES, String[]> NUTCH_ARGS = new HashMap<>();

	public final static Path segPath = new Path(testdir, "segments");
	public final static Path dumpPath = new Path(testdir, "dump/segments");

	@Before
	public void setUp() throws Exception {
		conf = CrawlDBTestUtil.createConfiguration();
		fs = FileSystem.get(conf);
		
		if (fs.exists(dumpPath)) {
			fs.delete(dumpPath, true);
		}
		
	}

	@After
	public void tearDown() throws IOException {

	}

	@Test
	public void readSeg() throws IOException {

		boolean co = true;
		boolean fe = true;
		boolean ge = true;
		boolean pa = true;
		boolean pd = true;
		boolean pt = true;

		int i = 0;
		
		SegmentReader segReader = new SegmentReader(conf, co, fe, ge, pa, pd, pt);
		
		try {
			
			FileStatus[] statuses = fs.listStatus(segPath);
			for (FileStatus status : statuses) {
				i++;
				if(i >= 3 ) break;
				
				Path input = new Path(segPath.toString(), status.getPath().getName());
				Path output = new Path(dumpPath.toString(), status.getPath().getName());
				segReader.dump(input, output);
			}
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			segReader.close();
		}

	}
}
