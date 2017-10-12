package com.hit.compare.dao;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hit.compare.misc.DBType;
import com.hit.compare.model.ColumnModel;

public class CommonDAO {
	private static final Logger logger = LoggerFactory.getLogger(CommonDAO.class);

	private static CommonDAO dao;

	private static Map<String, String> primaryKeyMap = new HashMap<String, String>();

	public static CommonDAO getInstance() {
		if (dao == null) {
			dao = new CommonDAO();
		}
		return dao;
	}

	public String getPrimaryKey(String tableName, DBType dbType) {
		String primaryKey = "";
		if (primaryKeyMap.containsKey(tableName)) {
			primaryKey = primaryKeyMap.get(tableName);
		} else {
			Connection conn = null;
			try {
				conn = ConnectionPool.getInstance().getConn(dbType);
				if (conn != null) {
					DatabaseMetaData metaData = conn.getMetaData();
					ResultSet primaryKeyResultSet = metaData.getPrimaryKeys(conn.getCatalog(), null, tableName);
					primaryKeyResultSet.next();
					String primaryKeyColumnName = primaryKeyResultSet.getString("COLUMN_NAME");
					primaryKey = primaryKeyColumnName;
					primaryKeyMap.put(tableName, primaryKey);
				}
			} catch (Exception e) {
				logger.error("getPrimaryKey", e);
			} finally {
				if (conn != null)
					try {
						conn.close();
					} catch (Exception ignore) {
						logger.error("getPrimaryKey关闭连接池异常：", ignore);
					}
			}
		}
		return primaryKey;
	}

	public Map<String, Object> selectOne(String sql, Map<String, Object> param, DBType dbType) {
		Map<String, Object> retMap = new HashMap<String, Object>();
		Connection conn = null;
		try {
			conn = ConnectionPool.getInstance().getConn(dbType);
			if (conn != null) {
				Statement st = conn.createStatement();
				sql = parseSQL(sql, param);
				logger.info("select sql:  " + sql);
				ResultSet rs = st.executeQuery(sql);

				ResultSetMetaData rsMetaData = rs.getMetaData();
				int colCount = rsMetaData.getColumnCount();
				while (rs.next()) {
					for (int i = 1; i <= colCount; i++) {
						String colName = rsMetaData.getColumnLabel(i);
						retMap.put(colName, rs.getString(colName));
					}
					break;
				}
				rs.close();
				st.close();
			}
		} catch (Exception e) {
			logger.error("selectOne sql：" + sql, e);
		} finally {
			if (conn != null)
				try {
					conn.close();
				} catch (Exception ignore) {
					logger.error("关闭连接池异常：" + sql, ignore);
				}
		}
		return retMap;
	}

	public Map<String,ColumnModel> getTableStruct(String tableName,DBType dbType){
		Map<String,ColumnModel> ret = new HashMap<String,ColumnModel>();
		Connection conn = null;
		try {
			conn = ConnectionPool.getInstance().getConn(dbType);
			if(conn !=null){
				Statement st = conn.createStatement();
				DatabaseMetaData dbmd = conn.getMetaData(); 
				ResultSet rs = dbmd.getColumns(conn.getCatalog(), "%", tableName, null); 
				while(rs.next()) {
					String columnName = rs.getString("COLUMN_NAME"); 
					String columnType = rs.getString("TYPE_NAME");
					int datasize = rs.getInt("COLUMN_SIZE"); 
					int digits = rs.getInt("DECIMAL_DIGITS"); 
					int nullable = rs.getInt("NULLABLE"); 
					String autoincrement = rs.getString("IS_AUTOINCREMENT"); 
					ColumnModel col = new ColumnModel(columnName, columnType, datasize, digits, nullable, autoincrement);
					ret.put(columnName, col);
				}
				rs.close();
				st.close();
			}
		} catch (Exception e) {
			logger.error("getTableStruct tableName："+tableName,e);
		} finally {  
            if (conn != null)  
            try {  
            	conn.close();  
            } catch (Exception ignore) {  
            	logger.error("关闭连接池异常："+tableName,ignore);
            }  
        }
		return ret;
	}

	public List<Map<String, Object>> selectList(String sql, Map<String, Object> param, DBType dbType) {
		List<Map<String, Object>> retList = new ArrayList<Map<String, Object>>();
		Connection conn = null;
		try {
			conn = ConnectionPool.getInstance().getConn(dbType);
			if (conn != null) {
				Statement st = conn.createStatement();
				sql = parseSQL(sql, param);
				logger.info("select sql:  " + sql);
				ResultSet rs = st.executeQuery(sql);
				ResultSetMetaData rsMetaData = rs.getMetaData();
				int colCount = rsMetaData.getColumnCount();
				while (rs.next()) {
					Map<String, Object> rowMap = new HashMap<String, Object>();
					for (int i = 1; i <= colCount; i++) {
						String colName = rsMetaData.getColumnLabel(i);
						rowMap.put(colName, rs.getString(colName));
					}
					retList.add(rowMap);
				}
				rs.close();
				st.close();
			}
		} catch (Exception e) {
			logger.error("selectList sql：" + sql, e);
		} finally {
			if (conn != null)
				try {
					conn.close();
				} catch (Exception ignore) {
					logger.error("关闭连接池异常：" + sql, ignore);
				}
		}
		return retList;
	}

	public boolean executeSQL(String sql, DBType dbType) {
		boolean ret = false;
		Connection conn = null;
		try {
			conn = ConnectionPool.getInstance().getConn(dbType);
			if (conn != null) {
				logger.info("execute sql:" + sql);
				Statement st = conn.createStatement();
				st.execute(sql);
				st.close();
			}
		} catch (Exception e) {
			logger.error("sql：" + sql, e);
		} finally {
			if (conn != null)
				try {
					conn.close();
				} catch (Exception ignore) {
					logger.error("关闭连接池异常：" + sql, ignore);
				}
		}
		return ret;
	}

	private String parseSQL(String sql, Map<String, Object> param) {
		try {
			String openToken = "#{";
			String closeToken = "}";
			String text = sql;
			StringBuilder builder = new StringBuilder();
			if (text != null && text.length() > 0) {
				char[] src = text.toCharArray();
				int offset = 0;
				int start = text.indexOf(openToken, offset);
				while (start > -1) {
					if (start > 0 && src[start - 1] == '\\') {
						// the variable is escaped. remove the backslash.
						builder.append(src, offset, start - offset - 1).append(openToken);
						offset = start + openToken.length();
					} else {
						int end = text.indexOf(closeToken, start);
						if (end == -1) {
							builder.append(src, offset, src.length - offset);
							offset = src.length;
						} else {
							builder.append(src, offset, start - offset);
							offset = start + openToken.length();
							String content = new String(src, offset, end - offset);
							builder.append(param.containsKey(content)
									? StringEscapeUtils.escapeSql(param.get(content).toString()) : "");
							offset = end + closeToken.length();
						}
					}
					start = text.indexOf(openToken, offset);
				}
				if (offset < src.length) {
					builder.append(src, offset, src.length - offset);
				}
			}
			sql = builder.toString();
		} catch (Exception e) {
			logger.error(" SQL:" + sql + " param:" + param + " case:" + e);
		}
		return sql;
	}
}
