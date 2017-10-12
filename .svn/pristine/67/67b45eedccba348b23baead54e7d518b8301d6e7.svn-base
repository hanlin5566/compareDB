package com.hit.compare.dao;

import java.sql.Connection;
import java.util.Properties;

import org.apache.tomcat.jdbc.pool.DataSource;
import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hit.compare.misc.Constant;
import com.hit.compare.misc.DBType;

public class ConnectionPool {
	private static final Logger logger = LoggerFactory.getLogger(ConnectionPool.class);
	DataSource srcDataSource = new DataSource();// 源库的数据源
	DataSource destDataSource = new DataSource();// 目标库的数据源
	private static ConnectionPool pool;

	public static ConnectionPool getInstance() {
		if (pool == null) {
			pool = new ConnectionPool();
		}
		return pool;
	}

	public Connection getConn(DBType dbType) throws Exception {
		if (dbType == DBType.SOURCE) {
			return srcDataSource.getConnection();
		}else{
			return destDataSource.getConnection();
		}
	}

	private ConnectionPool() {
		Properties pros = new Properties();
		try {
			pros.load(ConnectionPool.class.getResourceAsStream("/conf.properties"));
			// 初始化源连接池
		} catch (Exception e) {
			logger.error("加载配置文件出错");
			System.exit(0);
		}
		PoolProperties srcPoolPro = initPoolProperties(pros.getProperty("srcUrl"), pros.getProperty("srcUser"),
				pros.getProperty("srcPasswd"));
		PoolProperties destPoolPro = initPoolProperties(pros.getProperty("destUrl"), pros.getProperty("destUser"),
				pros.getProperty("destPasswd"));
		srcDataSource.setPoolProperties(srcPoolPro); // 池与数据源绑定
		destDataSource.setPoolProperties(destPoolPro); // 池与数据源绑定
	}

	private PoolProperties initPoolProperties(String url, String usrName, String pwd) {
		PoolProperties p = new PoolProperties(); // 池管理对象实例化
		p.setUrl(url); // 设置url
		p.setDriverClassName(Constant.DRIVER);// 设置驱动
		p.setUsername(usrName);// 设置用户名
		p.setPassword(pwd);// 设置密码
		p.setJmxEnabled(false);// 设置java管理扩展是否可用 是否支持JMX
		p.setTestWhileIdle(false);// 设置空闲是否可用 空闲对象回收器开启状态
		p.setTestOnBorrow(true);// 取回连接对链接有效性进行检查
								// 在borrow一个池实例时，是否提前进行池操作；如果为true，则得到的池实例均是可用的；
		p.setTestOnReturn(false);// 测试连接是否有返回
		p.setValidationQuery("SELECT 1");// 验证连接有效性的方式，这步不能省 测试连接语句
		p.setValidationInterval(30000);// 验证间隔时间
		// timeBetweenEvictionRunsMillis 和 minEvictableIdleTimeMillis，
		// 他们两个配合，可以持续更新连接池中的连接对象，
		// 当timeBetweenEvictionRunsMillis 大于0时，每过timeBetweenEvictionRunsMillis
		// 时间，
		// 就会启动一个线程，校验连接池中闲置时间超过minEvictableIdleTimeMillis的连接对象。
		p.setTimeBetweenEvictionRunsMillis(30000);
		p.setMinEvictableIdleTimeMillis(30000);

		p.setMaxActive(300);// 连接池最大并发容量
		p.setInitialSize(10);// 初始化连接池时,创建连接个数
		p.setMaxWait(10000);// 超时等待时间以毫秒为单位
		p.setRemoveAbandonedTimeout(60);// 超时时间(以秒数为单位)
		p.setMaxIdle(100);// 最大空闲连接数
		p.setMinIdle(10);// 最小空闲连接数
		p.setLogAbandoned(true); // 是否在自动回收超时连接的时候打印连接的超时错误
		p.setRemoveAbandoned(true);// 是否自动回收超时连接

		// 設置jdbc拦截器
		p.setJdbcInterceptors("org.apache.tomcat.jdbc.pool.interceptor.ConnectionState;"
				+ "org.apache.tomcat.jdbc.pool.interceptor.StatementFinalizer");

		return p;
	}

}
