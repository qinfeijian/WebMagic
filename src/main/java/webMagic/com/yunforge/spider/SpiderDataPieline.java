package webMagic.com.yunforge.spider;


import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.alibaba.druid.pool.DruidPooledConnection;

import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;
import webMagic.com.yunforge.spider.config.DruidDBConfig;
import webMagic.com.yunforge.spider.model.ANSBAgrMarket;
import webMagic.com.yunforge.spider.model.SpiderData;


public class SpiderDataPieline implements Pipeline{

	public void process(ResultItems resultItems, Task task) {
		// TODO Auto-generated method stub
		List<SpiderData> dataList = (List<SpiderData>)resultItems.get("repo");
		Map<String, ANSBAgrMarket> regionMarketMap = (Map<String, ANSBAgrMarket>)resultItems.get("regionMarketMap");
		Map<String, String> prodmap = (Map<String, String>)resultItems.get("prodmap");
		insertDatas(dataList, regionMarketMap, prodmap);
	}

	public int insertDatas(List<SpiderData> data, Map<String, ANSBAgrMarket> regionMarketMap, Map<String, String> prodmap) {
		DruidDBConfig dbp = DruidDBConfig.getInstance();
		DruidPooledConnection conn = null;
		PreparedStatement pst = null;
		int result = -1;
		try {
			conn = dbp.getConnection();
			conn.setAutoCommit(false);
			pst = conn.prepareStatement(
					"INSERT INTO s_trade_market_day (STATTIME, REGION_NAME, MARKET_NAME, VARI_NAME, AVG_PRICE, REGION_CODE, PROD_NAME, PROD_CODE, MARKET_CODE, VARI_NO, CREATEON) VALUES (?,?,?,?,?,?,?,?,?,?,?);");
			for (int i = 0; i < data.size(); i++) {
				SpiderData bean = data.get(i);
				getInsertValue(pst, bean,regionMarketMap, prodmap);
			}

			pst.executeBatch();
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (pst != null) {
				try {
					pst.close();
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
		return result;
	}
	public static void getInsertValue(PreparedStatement pst, SpiderData data, Map<String, ANSBAgrMarket> regionMarketMap, Map<String, String> prodmap) throws SQLException {

		String region_name = "";
		String region_code = "";
		String prodcode = "";
		try {
			region_name = regionMarketMap.get(data.getMartket_id()).getRegion_name();
			region_code = regionMarketMap.get(data.getMartket_id()).getRegion_code();

		} catch (Exception e) {

		}
		try {
			prodcode = prodmap.get(data.getVari_name());
		} catch (Exception e) {
			
		}

		// stattime
		pst.setString(1, data.getStattime());
		// region_name
		pst.setString(2, region_name);
		// market_name
		pst.setString(3, data.getMarket_name());
		// VARI_NAME
		pst.setString(4, data.getVari_name());
		// avr_prive
		pst.setString(5, data.getAvg_price());
		// region_code
		pst.setString(6, region_code);
		// prodname
		pst.setString(7, data.getVari_name());
		// PROD_CODE
		pst.setString(8, prodcode);
		// MARKET_CODE
		pst.setString(9, data.getMartket_id());
		// VARI_NO
		pst.setString(10, data.getVari_no());
		// CREATEON
		pst.setString(11, "qinfj");

		pst.addBatch();

	}
}
