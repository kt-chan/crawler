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
package me.ktchan.crawler.general;

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
	private final static Path testdir = new Path("./test/");
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
		fs.mkdirs(dumpPath);

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
	 * E7%BD%91%E9%A1%B5%E7%9A%84%E9%85%8D%E7%BD%AE/ 鎴戜滑鍏堟潵浠嬬粛crawler鐨勫瓨鍌ㄦ暟鎹殑鏂囦欢缁撴瀯锛�
	 * 
	 * crawler鏁版嵁鏂囦欢涓昏鍖呮嫭涓夌被锛屽垎鍒槸web
	 * database锛屼竴绯诲垪鐨剆egment鍔犱笂index锛屼笁鑰呯殑鐗╃悊鏂囦欢鍒嗗埆瀛樺偍鍦ㄧ埇琛岀粨鏋滅洰褰曚笅鐨刣b鐩綍涓媤ebdb瀛愭枃浠跺す鍐咃紝
	 * segments鏂囦欢澶瑰拰index鏂囦欢澶广�傦紙瀵逛簬nutch1.7锛屾枃浠剁粨鏋勬湁鎵�涓嶅悓锛屾瘡娆＄埇铏悗锛屼細鍦╪utch鏍圭洰褰曚笅浜х敓涓�涓悕涓衡�渃rawler
	 * +骞�+鏈�+鏃�+鏃�+鍒�+绉掆�濈殑鏂囦欢澶癸紝閲岄潰瀛樺偍鐨勫氨鏄繖娆＄埇铏骇鐢熺殑鎵�鏈夋暟鎹枃浠讹紝鏂囦欢澶瑰唴閮ㄧ殑涓嶅悓涔嬪鎴戜滑鍦ㄤ笅闈㈢殑璇︾粏浠嬬粛涓姞浠ヨ鏄庯級
	 * 涓夎�呭瓨鍌ㄧ殑淇℃伅鍐呭濡備笅锛�
	 * 
	 * Web database锛屼篃鍙玏ebDB锛屽叾涓瓨鍌ㄧ殑鏄埇铏墍鎶撳彇缃戦〉涔嬮棿鐨勯摼鎺ョ粨鏋勪俊鎭紝
	 * 瀹冨彧鍦ㄧ埇铏獵rawler宸ヤ綔涓娇鐢ㄨ�屽拰Searcher鐨勫伐浣滄病鏈変换浣曞叧绯汇�俉ebDB鍐呭瓨鍌ㄤ簡涓ょ瀹炰綋鐨勪俊鎭細page鍜宭ink銆�
	 * Page瀹炰綋閫氳繃鎻忚堪缃戠粶涓婁竴涓綉椤电殑鐗瑰緛淇℃伅鏉ヨ〃寰佷竴涓疄闄呯殑缃戦〉锛屽洜涓虹綉椤垫湁寰堝涓渶瑕佹弿杩帮紝
	 * WebDB涓�氳繃缃戦〉鐨刄RL鍜岀綉椤靛唴瀹圭殑MD5涓ょ绱㈠紩鏂规硶瀵硅繖浜涚綉椤靛疄浣撹繘琛屼簡绱㈠紩銆侾age瀹炰綋鎻忚堪鐨勭綉椤电壒寰佷富瑕佸寘鎷綉椤靛唴鐨刲ink鏁扮洰锛�
	 * 鎶撳彇姝ょ綉椤电殑鏃堕棿绛夌浉鍏虫姄鍙栦俊鎭紝瀵规缃戦〉鐨勯噸瑕佸害璇勫垎绛夈�傚悓鏍风殑锛孡ink瀹炰綋鎻忚堪鐨勬槸涓や釜page瀹炰綋涔嬮棿鐨勯摼鎺ュ叧绯汇��
	 * WebDB鏋勬垚浜嗕竴涓墍鎶撳彇缃戦〉鐨勯摼鎺ョ粨鏋勫浘锛岃繖涓浘涓璓age瀹炰綋鏄浘鐨勭粨鐐癸紝鑰孡ink瀹炰綋鍒欎唬琛ㄥ浘鐨勮竟銆傦紙鎴戝湪瀹炶返涓娇鐢ㄧ殑鏄痭utch1.7+
	 * solr4.4锛屽叾涓皢WebDB鍒嗕负crawlerdb鍜宭inkdb涓や釜閮ㄥ垎锛�
	 * 
	 * 涓�娆＄埇琛屼細浜х敓寰堝涓猻egment锛屾瘡涓猻egment鍐呭瓨鍌ㄧ殑鏄埇铏獵rawler鍦ㄥ崟鐙竴娆℃姄鍙栧惊鐜腑鎶撳埌鐨勭綉椤典互鍙婅繖浜涚綉椤电殑绱㈠紩銆�
	 * Crawler鐖鏃朵細鏍规嵁WebDB涓殑link鍏崇郴鎸夌収涓�瀹氱殑鐖绛栫暐鐢熸垚姣忔鎶撳彇寰幆鎵�闇�鐨刦etchlist锛�
	 * 鐒跺悗Fetcher閫氳繃fetchlist涓殑URLs鎶撳彇杩欎簺缃戦〉骞剁储寮曪紝鐒跺悗灏嗗叾瀛樺叆segment銆係egment鏄湁鏃堕檺鐨勶紝
	 * 褰撹繖浜涚綉椤佃Crawler閲嶆柊鎶撳彇鍚庯紝鍏堝墠鎶撳彇浜х敓鐨剆egment灏变綔搴熶簡銆傚湪瀛樺偍涓�係egment鏂囦欢澶规槸浠ヤ骇鐢熸椂闂村懡鍚嶇殑锛�
	 * 鏂逛究鎴戜滑鍒犻櫎浣滃簾鐨剆egments浠ヨ妭鐪佸瓨鍌ㄧ┖闂淬��
	 * 
	 * Index鏄疌rawler鎶撳彇鐨勬墍鏈夌綉椤电殑绱㈠紩锛屽畠鏄�氳繃瀵规墍鏈夊崟涓猻egment涓殑绱㈠紩杩涜鍚堝苟澶勭悊鎵�寰楃殑銆侼utch鍒╃敤Lucene鎶�鏈繘琛岀储寮�
	 * 锛屾墍浠ucene涓绱㈠紩杩涜鎿嶄綔鐨勬帴鍙ｅNutch涓殑index鍚屾牱鏈夋晥銆備絾鏄渶瑕佹敞鎰忕殑鏄紝Lucene涓殑segment鍜孨utch涓殑涓嶅悓锛�
	 * Lucene涓殑segment鏄储寮昳ndex鐨勪竴閮ㄥ垎锛屼絾鏄疦utch涓殑segment鍙槸WebDB涓悇涓儴鍒嗙綉椤电殑鍐呭鍜岀储寮曪紝
	 * 鏈�鍚庨�氳繃鍏剁敓鎴愮殑index璺熻繖浜泂egment宸茬粡姣棤鍏崇郴浜嗐�傦紙鐢变簬鎴戜滑浣跨敤solr鏉ュ疄鐜版煡璇紝
	 * 鎵�浠ュ湪nutch鐨勬暟鎹枃浠朵腑鎵句笉鍒癷ndex杩欎釜鏂囦欢澶癸紝瀹冭瀛樺叆鍒皊olr鐨勬枃浠剁洰褰曚笅锛�
	 * 
	 * 鍦ㄦ竻妤氫簡crawler鐨勬枃浠剁粨鏋勫悗锛屾垜浠潵浠嬬粛crawler鐨勫伐浣滄祦绋嬶細
	 * 
	 * Crawler鐨勫伐浣滃師鐞嗕富瑕佹槸锛氶鍏圕rawler鏍规嵁WebDB鐢熸垚涓�涓緟鎶撳彇缃戦〉鐨刄RL闆嗗悎鍙仛Fetchlist锛�
	 * 鎺ョ潃涓嬭浇绾跨▼Fetcher寮�濮嬫牴鎹瓼etchlist灏嗙綉椤垫姄鍙栧洖鏉ワ紝濡傛灉涓嬭浇绾跨▼鏈夊緢澶氫釜锛岄偅涔堝氨鐢熸垚寰堝涓狥etchlist锛�
	 * 涔熷氨鏄竴涓狥etcher瀵瑰簲涓�涓狥etchlist銆傜劧鍚嶤rawler鏍规嵁鎶撳彇鍥炴潵鐨勭綉椤礧ebDB杩涜鏇存柊锛�
	 * 鏍规嵁鏇存柊鍚庣殑WebDB鐢熸垚鏂扮殑Fetchlist锛岄噷闈㈡槸鏈姄鍙栫殑鎴栬�呮柊鍙戠幇鐨刄RLs锛岀劧鍚庝笅涓�杞姄鍙栧惊鐜噸鏂板紑濮嬨�傝繖涓惊鐜繃绋嬪彲浠ュ彨鍋氣�滀骇鐢�/鎶撳彇
	 * /鏇存柊鈥濆惊鐜��
	 * 
	 * 鎸囧悜鍚屼竴涓富鏈轰笂Web璧勬簮鐨刄RLs閫氬父琚垎閰嶅埌鍚屼竴涓狥etchlist涓紝
	 * 杩欐牱鐨勮瘽闃叉杩囧鐨凢etchers瀵逛竴涓富鏈哄悓鏃惰繘琛屾姄鍙栭�犳垚涓绘満璐熸媴杩囬噸銆傚彟澶朜utch閬靛畧Robots Exclusion
	 * Protocol锛岀綉绔欏彲浠ラ�氳繃鑷畾涔塕obots.txt鎺у埗Crawler鐨勬姄鍙栥��
	 * 
	 * 鍦∟utch涓紝Crawler鎿嶄綔鐨勫疄鐜版槸閫氳繃涓�绯诲垪瀛愭搷浣滅殑瀹炵幇鏉ュ畬鎴愮殑銆傝繖浜涘瓙鎿嶄綔Nutch閮芥彁渚涗簡瀛愬懡浠よ鍙互鍗曠嫭杩涜璋冪敤銆�
	 * 涓嬮潰灏辨槸杩欎簺瀛愭搷浣滅殑鍔熻兘鎻忚堪浠ュ強鍛戒护琛岋紝鍛戒护琛屽湪鎷彿涓��
	 * 
	 * 1. 鍒涘缓涓�涓柊鐨刉ebDb (admin db -create).
	 * 
	 * 2. 灏嗘姄鍙栬捣濮婾RLs鍐欏叆WebDB涓� (inject).
	 * 
	 * 3. 鏍规嵁WebDB鐢熸垚fetchlist骞跺啓鍏ョ浉搴旂殑segment(generate).
	 * 
	 * 4. 鏍规嵁fetchlist涓殑URL鎶撳彇缃戦〉 (fetch).
	 * 
	 * 5. 鏍规嵁鎶撳彇缃戦〉鏇存柊WebDb (updatedb).
	 * 
	 * 6. 寰幆杩涜3锛�5姝ョ洿鑷抽鍏堣瀹氱殑鎶撳彇娣卞害銆�
	 * 
	 * 7. 鏍规嵁WebDB寰楀埌鐨勭綉椤佃瘎鍒嗗拰links鏇存柊segments (updatesegs).
	 * 
	 * 8. 瀵规墍鎶撳彇鐨勭綉椤佃繘琛岀储寮�(index).
	 * 
	 * 9. 鍦ㄧ储寮曚腑涓㈠純鏈夐噸澶嶅唴瀹圭殑缃戦〉鍜岄噸澶嶇殑URLs (dedup).
	 * 
	 * 10. 灏唖egments涓殑绱㈠紩杩涜鍚堝苟鐢熸垚鐢ㄤ簬妫�绱㈢殑鏈�缁坕ndex(merge).
	 * 
	 * Crawler璇︾粏宸ヤ綔娴佺▼鏄細鍦ㄥ垱寤轰竴涓猈ebDB涔嬪悗(姝ラ1),
	 * 鈥滀骇鐢�/鎶撳彇/鏇存柊鈥濆惊鐜�(姝ラ3锛�6)鏍规嵁涓�浜涚瀛怳RLs寮�濮嬪惎鍔ㄣ�傚綋杩欎釜寰幆褰诲簳缁撴潫锛孋rawler鏍规嵁鎶撳彇涓敓鎴愮殑segments鍒涘缓绱㈠紩锛�
	 * 姝ラ7锛�10锛夈�傚湪杩涜閲嶅URLs娓呴櫎锛堟楠�9锛変箣鍓嶏紝姣忎釜segment鐨勭储寮曢兘鏄嫭绔嬬殑锛堟楠�8锛夈�傛渶缁堬紝
	 * 鍚勪釜鐙珛鐨剆egment绱㈠紩琚悎骞朵负涓�涓渶缁堢殑绱㈠紩index锛堟楠�10锛夈��
	 * 
	 * nutch1.7+solr4.4閰嶇疆
	 * 
	 * 浠嶯utch 缃戠珯涓嬭浇apache-nutch-1.7-bin.tar.gz 鍥炴潵瑙ｅ紑(~/apache-nutch-1.7/)
	 * 
	 * 鎶奡olr 棰勮鐨刢ore 鑼冧緥collection1 澶嶅埗涓篶ore-nutch
	 * 
	 * 鎶奛utch 鎻愪緵鐨刢onf/schema-solr4.xml 瑕嗙洊鎺塖olr core-nutch 鐨刢onf/schema.xml
	 * 
	 * 灏哠olr core-nutch conf/schema.xml琛ヤ笂涓�琛屾紡鎺夌殑鏍忎綅璁惧畾<field name=鈥漘version_鈥�
	 * type=鈥漧ong鈥� stored=鈥漷rue鈥� indexed=鈥漷rue鈥� multiValued=鈥漟alse鈥�/>
	 * 
	 * 閲嶅紑Solr锛岃繘Web绠＄悊浠嬮潰鐨凜ore Admin鏂板涓�涓猚ore-nutch core锛屽苟涓斾笉璇ユ湁杩欎釜鏂板core鐨勯敊璇鎭��
	 * 
	 * 鍥炴潵~/apache-nutch-1.7/ 璁惧畾Nutch銆� conf/ 搴曚笅鍏堢紪杈憂utch-site.xml
	 * 琛ヤ笂http.agent.name 鐨刢rawler 鍚嶇О璁惧畾銆�
	 * 
	 * 鍐嶇紪杈憆egex-urlfilter.txt灏嗘渶鍚庝竴琛�+.娉ㄨВ鎺夛紝鏀逛负+^http://my.site.domain.name鍙姄鎴戣嚜宸辩殑缃戠珯
	 * 
	 * 鍐嶅洖鍒皛/apache-nutch-1.7/锛屾柊澧炰竴涓猽rls/
	 * 鐩綍锛岄噷澶存斁涓�涓猻eed.txt锛屽唴瀹规斁鑷繁鎯宠鎶撶殑绉嶅瓙缃戝潃锛岃繖閲屽洜涓哄彧鎯宠鎶撹嚜宸辩殑缃戠珯锛屾墍浠ュ彧瑕佹斁涓�琛宧ttp://my.site.domain
	 * .name/灏卞ソ
	 * 
	 * 鎺ヤ笅鏉ヨ窇./bin/nutch crawl urls/ -solr
	 * http://solr.server.name/solr/core-nutch/ -threads 20 -depth 2 -topN
	 * 3娴嬭瘯鑳戒笉鑳芥姄鍒扮綉绔欎笂灞傜殑鍑犱釜缃戦〉锛屽埌Solr 绠＄悊浠嬮潰閲屽ご鐢ㄦ煡璇㈠姛鑳藉鏋滄湁璧勬枡锛屽氨鏄垚鍔熶簡銆�
	 * 
	 */
	public void testInject() throws IOException {

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
				try {
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
				} catch (IOException e) {
					e.printStackTrace();
				}
				// update crawldb
				crawldb.run(NUTCH_ARGS.get(MODES.UPDATEDB));
			}

			// invertlinks
			linkdb.run(NUTCH_ARGS.get(MODES.INVERTLINKS));

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
			if (parser != null) parser.close();
			if (linkdb != null) linkdb.close();
			if (linkdbReader != null) linkdbReader.close();
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
