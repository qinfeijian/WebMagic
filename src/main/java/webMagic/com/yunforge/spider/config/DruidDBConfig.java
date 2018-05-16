package webMagic.com.yunforge.spider.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.alibaba.druid.pool.DruidPooledConnection;

/**
* 要实现单例模式，保证全局只有一个数据库连接池
*/
public class DruidDBConfig {
	static Logger log = Logger.getLogger(DruidDBConfig.class);
	private static DruidDBConfig dbPoolConnection = null;
	private static DruidDataSource druidDataSource = null;

	static {
		Properties properties = loadPropertiesFile("db_server.properties");
		try {
			druidDataSource = (DruidDataSource) DruidDataSourceFactory.createDataSource(properties); // DruidDataSrouce工厂模式
		} catch (Exception e) {
			log.error("获取配置失败");
		}
	}

	/**
	 * 数据库连接池单例
	 * 
	 * @return
	 */
	public static synchronized DruidDBConfig getInstance() {
		if (null == dbPoolConnection) {
			dbPoolConnection = new DruidDBConfig();
		}
		return dbPoolConnection;
	}

	/**
	 * 返回druid数据库连接
	 * 
	 * @return
	 * @throws SQLException
	 */
	public DruidPooledConnection getConnection() throws SQLException {
		return druidDataSource.getConnection();
	}

	/**
	 * @param string
	 *            配置文件名
	 * @return Properties对象
	 */
	private static Properties loadPropertiesFile(String fullFile) {
		String webRootPath = null;
		if (null == fullFile || fullFile.equals("")) {
			throw new IllegalArgumentException("Properties file path can not be null" + fullFile);
		}
		
		webRootPath = DruidDBConfig.class.getClassLoader().getResource(fullFile).getPath();
//		webRootPath = new File(webRootPath).getParent();
		InputStream inputStream = null;
		Properties p = null;
		try {
			inputStream = new FileInputStream(new File(webRootPath));
			p = new Properties();
			p.load(inputStream);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (null != inputStream) {
					inputStream.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return p;
	}

}
