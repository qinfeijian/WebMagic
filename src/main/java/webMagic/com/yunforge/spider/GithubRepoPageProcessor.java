package webMagic.com.yunforge.spider;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.springframework.core.io.PathResource;

import com.alibaba.druid.pool.DruidPooledConnection;

import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.processor.PageProcessor;
import us.codecraft.webmagic.selector.Html;
import us.codecraft.webmagic.selector.Selectable;
import webMagic.com.yunforge.spider.config.DruidDBConfig;
import webMagic.com.yunforge.spider.model.ANSBAgrMarket;
import webMagic.com.yunforge.spider.model.SpiderData;

public class GithubRepoPageProcessor implements PageProcessor {
	
	private static Map<String, ANSBAgrMarket> regionMarketMap;
	private static Map<String, String> prodmap;
	
	public static Map<String, ANSBAgrMarket> getRegionMarketMap() {
		if (regionMarketMap != null) {
			return regionMarketMap;
		}
		regionMarketMap = new HashMap<String, ANSBAgrMarket>();
		// 获取连接池单例
		DruidDBConfig dbp = DruidDBConfig.getInstance();
		DruidPooledConnection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			conn = dbp.getConnection();
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select MARKETID,MARKETNAME,REGION_CODE,REGION_NAME from ANSB_AGR_MARKET");
			while (rs.next()) {
				ANSBAgrMarket bean = new ANSBAgrMarket();
				String marketid = rs.getString("MARKETID");
				String marketname = rs.getString("MARKETNAME");
				String region_code = rs.getString("REGION_CODE");
				String region_name = rs.getString("REGION_NAME");
				bean.setMartket_id(marketid);
				bean.setMarket_name(marketname);
				bean.setRegion_code(region_code);
				bean.setRegion_name(region_name);
				regionMarketMap.put(marketid, bean);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return regionMarketMap;
	}

	public static Map<String, String> getProMap() {
		if (prodmap != null) {
			return prodmap;
		}
		prodmap = new HashMap<String, String>();
		// 获取连接池单例
		DruidDBConfig dbp = DruidDBConfig.getInstance();
		DruidPooledConnection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			conn = dbp.getConnection();
			stmt = conn.createStatement();
			rs = stmt.executeQuery(
					"select t.code,t.name  from JC_GL_CODE t where t.codesetdetaileid='b7b189ae-e897-490a-9c82-3b4b5981292a'");
			while (rs.next()) {
				String name = rs.getString("name");
				String code = rs.getString("code");
				prodmap.put(name, code);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return prodmap;
	}
	
	// 部分一：抓取网站的相关配置，包括编码、抓取间隔、重试次数等
    private Site site = Site.me().setRetryTimes(3).setSleepTime(5000);

    
    
    // process是定制爬虫逻辑的核心接口，在这里编写抽取逻辑
    public void process(Page page) {
    	// 当前下载的HTML页面的url
    	String url = page.getRequest().getUrl();
    	
    	// HTML页面
    	Html html = page.getHtml();
    	// 选择抓取部分
    	Selectable selectable = html.$("div.w890.fl script");
    	
    	
    	
    	// 存放抓取到的数据
    	List<SpiderData> dataList = new ArrayList<SpiderData>();
    	
    	if (url.indexOf("&page=") == -1) {
    		// 设置不处理抓取结果
    		page.setSkip(true);
    	}
    	
    	// 如果当前页面不是分页页面，并且有产品代码时获取分页信息，添加分页抓取url
    	if (url.indexOf("&page=") == -1 && url.indexOf("craft_index=") != -1) {
    		// 通过正则获取分页数
    		String pageCount = selectable.regex("v_PageCount = \\d+;").get();
    		// 截取v_PageCount =  ; 确保分页数不为空
    		int begin = pageCount.indexOf("v_PageCount = ");
			int end = pageCount.lastIndexOf(";");
			int size = 1;
			if (begin != -1 && end != -1) {
				try {
					pageCount = pageCount.substring(begin + 14, end);
					size = Integer.parseInt(pageCount);
				} catch (Exception e) {
					System.out.println(pageCount);
					size = 1;
				}
			}
    		// 添加分页url进入抓取队列
    		for (int i = 0; i <= size; i++) {
    			page.addTargetRequest(url + "&page=" + i);
    		}
    		
    	} else if(url.indexOf("&page=") != -1 && url.indexOf("craft_index=") != -1) { //是分页url，并且拥有产品代码条件，开始处理抓取数据
    		System.out.println(url);
    		Selectable trs = html.xpath("//table//tr");
    		List<Selectable> nodes = trs.nodes();
    		for (Selectable node : nodes) {
    			List<Selectable> tds = node.xpath("//td").nodes();
    			if (tds.size() != 0) {
    				SpiderData data = new SpiderData();
	    			data.setStattime(tds.get(0).regex("(<td>(.*)</td>)", 2).get());
	    			data.setVari_name(tds.get(1).regex("(<span class=\"c-orange\">(.*)</span>)", 2).get());
	    			data.setAvg_price(tds.get(2).regex("(<span class=\"c-orange\">(.*)</span>)", 2).get());
	    			data.setMartket_id(tds.get(3).regex("(id=(.*)\">)", 2).get());
	    			data.setMarket_name(tds.get(3).regex("(\">(.*)</a>)", 2).get());
	    			dataList.add(data);
    			}
    			
    		}
    	}
    	if (dataList.size() == 0) {
    		// 设置不处理抓取结果
    		page.setSkip(true);
    	}
    	page.putField("regionMarketMap", getRegionMarketMap());
    	page.putField("prodmap", getProMap());
    	page.putField("repo", dataList);
    	
    	
    	
    }

    public Site getSite() {
        return site;
    }

    public static List<String> getIndex() throws SQLException {
		List<String> list = new ArrayList<String>();

		// 获取连接池单例
		DruidDBConfig dbp = DruidDBConfig.getInstance();
		DruidPooledConnection conn = null;
		Statement stmt = null;
		ResultSet rs = null;

		try {
			conn = dbp.getConnection();
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select * from ANSB_AGR_INDEX t where t.status=2");
			while (rs.next()) {
				list.add(rs.getString("idxcode"));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}

		return list;
	}
 // 获得所有链接
 	public static List<String> getUrlList(List<String> idxList, String startTime, String endTime) {

 		ArrayList<String> links = new ArrayList<String>();
 		String url_1 = "";
 		for (int i = 0; i < idxList.size(); i++) {// 遍历指标
 			url_1 = "http://nc.mofcom.gov.cn/channel/jghq2017/price_list.shtml?craft_index=" + idxList.get(i)
 					+ "&startTime=" + startTime + "&endTime=" + endTime + "&par_p_index=&p_index=&keyword=";
 			links.add(url_1);
 		}
 		return links;
 	}
    public static void main(String[] args) {
    	try {
			List<String> urlList = getUrlList(getIndex(), "2017-12-21", "2018-01-14");
			Spider create = Spider.create(new GithubRepoPageProcessor());
			for (String url : urlList) {
				create.addUrl(url);
			}
			// 配置数据处理
			//开启5个线程抓取
			create.addPipeline(new SpiderDataPieline())
			.thread(25)
			//启动爬虫
			.run();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    } 
    
    
    
}
