package com.hit.compare.service;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hit.compare.dao.CommonDAO;
import com.hit.compare.misc.DBType;
import com.hit.compare.misc.JackSon;
import com.hit.compare.model.ColumnModel;

public class CompareDBService {
	private static final Logger logger = LoggerFactory.getLogger(CompareDBService.class);
	private static CompareDBService service;
	public static CompareDBService getInstance() {
		if (service == null) {
			service = new CompareDBService();
		}
		return service;
	}
	
	//获得表名
	public List<String> getTables(DBType dbType){
		List<String> ret = new ArrayList<String>();
		List<Map<String, Object>> result = CommonDAO.getInstance().selectList("show tables", null,dbType);
		for (Map<String, Object> map : result) {
			List<Object> values = new ArrayList<Object>(map.values());
			ret.add(values.get(0).toString());
		}
		return ret;
	}
	//比对俩个库所有表的差异
	public void compare(){
		List<String> srcTables = this.getTables(DBType.SOURCE);
		List<String> destTables = this.getTables(DBType.DESTINATION);
		StringBuffer totalReportStr = new StringBuffer();
		String title = String.format("源库共 %s 张表，目标库 %s 张表\r\n", srcTables.size(),destTables.size());
		totalReportStr.append(title);
		logger.info(title);
		int index = 0;
		for (String srcTable : srcTables) {
			index++;
			StringBuffer reportStr = new StringBuffer();
			StringBuffer sqlStr = new StringBuffer();
			reportStr.append(title);
			totalReportStr.append(String.format("%s. *******************%s表  开始*************** \r\n",index,srcTable));
			if(destTables.contains(srcTable)){
				//比对表结构
				Map<String, ColumnModel> srcTableStruct = CommonDAO.getInstance().getTableStruct(srcTable, DBType.SOURCE);
				Map<String, ColumnModel> destTableStruct = CommonDAO.getInstance().getTableStruct(srcTable, DBType.DESTINATION);
				String compareTableStruct = compareTableStruct(srcTableStruct, destTableStruct);
				if(StringUtils.isNotEmpty(compareTableStruct)){
					totalReportStr.append(String.format("%s表，有不一致字段！！！ \r\n",srcTable));
					totalReportStr.append(compareTableStruct+"\r\n");
				}
				logger.info(String.format("正在处理第%s张表，名称为%s",index,srcTable));
				String primarykey = CommonDAO.getInstance().getPrimaryKey(srcTable, DBType.SOURCE); 
				if(StringUtils.isEmpty(primarykey)){
					String err = srcTable+"未获取到主键，请检查表结构 \r\n";
					totalReportStr.append(err);
					logger.error(err);
					totalReportStr.append(String.format("%s. *******************%s表  结束*************** \r\n\r\n",index,srcTable));
					continue;
				}
				String querySQL = String.format("select * from %s", srcTable);
				//取得源表结果集
				List<Map<String, Object>> srcResult = CommonDAO.getInstance().selectList(querySQL, null, DBType.SOURCE);
				//取得目标表结果集
				List<Map<String, Object>> destResult = CommonDAO.getInstance().selectList(querySQL, null, DBType.DESTINATION);
				long srcTotal = srcResult.size();
				long destTotal = destResult.size();
				long count = 0;//相差总数
				String content = String.format("源表共%s条记录，目标表共%s条记录 \r\n", srcTotal,destTotal);
				logger.info(content);
				reportStr.append(content);
				//取目标表主键
				List<String> destPrimaryIds = getPrimaryIds(destResult,primarykey);
				for (Map<String, Object> map : srcResult) {
					String srcPrimaryKey = map.get(primarykey).toString();
					//目标库中没有此ID数据则记录report和sql
					if(!destPrimaryIds.contains(srcPrimaryKey)){
						//记录报告
						content = String.format("目标表没有此记录%s \r\n", JackSon.beanToJsonStr(map));
						reportStr.append(content);
						String insertSql = insertSql(map,srcTable);
						//记录insert语句
						sqlStr.append(insertSql+"\r\n");
						count++;
					}
				}
				if(count > 0 ){
					totalReportStr.append(String.format("%s表，源表共有%s条数据不存在于目标表 \r\n",srcTable,count));
				}
			}else{
				String content = String.format("目标库%s表不存在\r\n", srcTable);
				reportStr.append(content);
				totalReportStr.append(content);
			}
			totalReportStr.append(String.format("%s. *******************%s表  结束*************** \r\n\r\n",index,srcTable));
			//写入文件
			writeFile(srcTable, reportStr.toString(), sqlStr.toString());
		}
		//写入总report文件
		writeFile(totalReportStr.toString());
	}
	
	private void writeFile(String str){
		File folder = new File("/report/");
		if(!folder.exists()){
			folder.mkdirs();
		}
		try {
			//report
			FileUtils.writeByteArrayToFile(new File(folder.getAbsolutePath()+"\\"+"report.txt"), str.getBytes());
			logger.info("文件输出至目录："+folder.getAbsolutePath());
		} catch (IOException e) {
			logger.error("写入文件失败");
		}
	}
	
	private void writeFile(String tableName,String report,String sql){
		
		File folder = new File("/report/"+tableName);
		if(!folder.exists()){
			folder.mkdirs();
		}
		try {
			//report
			FileUtils.writeByteArrayToFile(new File(folder.getAbsolutePath()+"\\"+tableName+"_report.txt"), report.getBytes());
			//sql
			FileUtils.writeByteArrayToFile(new File(folder.getAbsolutePath()+"\\"+tableName+"_SQL.txt"), sql.getBytes());
			logger.info("文件输出至目录："+folder.getAbsolutePath());
		} catch (IOException e) {
			logger.error("写入文件失败");
		}
	}
	
	private String compareTableStruct(Map<String, ColumnModel> src,Map<String, ColumnModel> dest){
		StringBuffer ret = new StringBuffer();
		//源比对目标
		for (String key : src.keySet()) {
			//源表字段在目标表不存在
			if(!dest.containsKey(key)){
				ret.append(String.format("源表%s字段在目标表不存在 \r\n", key));
			}else{
				//比对字段类型
				ColumnModel srcCol = src.get(key);
				ColumnModel destCol = dest.get(key);
				String compare = srcCol.compare(destCol);
				if(StringUtils.isNotEmpty(compare)){
					ret.append(compare);
				}
			}
		}
		return ret.toString();
	}
	
	private String insertSql(Map<String,Object> map,String tableName){
		StringBuffer ret = new StringBuffer();
		StringBuffer key = new StringBuffer();
		key.append("INSERT INTO "+tableName+"(");
		StringBuffer value = new StringBuffer();
		for (String mapKey : map.keySet()) {
			//key 
			key.append(mapKey+", ");
			//value
			value.append("'"+map.get(mapKey)+"', ");
		}
		//去掉最后一个逗号
		value.replace(value.lastIndexOf(","), value.length(), "");
		key.replace(key.lastIndexOf(","), key.length(), "");
		//添加括号
		key.append(") values(");
		value.append(");");
		ret.append(key.toString());
		ret.append(value.toString());
		return ret.toString();
	}
	
	private List<String> getPrimaryIds(List<Map<String, Object>> result,String primaryKey){
		List<String> ret = new ArrayList<String>();
		for (Map<String, Object> map : result) {
			ret.add(map.get(primaryKey).toString());
		}
		return ret;
	}
}
