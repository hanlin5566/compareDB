package com.hit.compare.model;

public class ColumnModel {
	private String columnName;
	private String columnType;
	private int datasize;
	private int digits;
	private int nullable;
	private String autoincrement;

	public ColumnModel(String columnName, String columnType, int datasize, int digits, int nullable,
			String autoincrement) {
		super();
		this.columnName = columnName;
		this.columnType = columnType;
		this.datasize = datasize;
		this.digits = digits;
		this.nullable = nullable;
		this.autoincrement = autoincrement;
	}

	public String getColumnName() {
		return columnName;
	}

	public String getColumnType() {
		return columnType;
	}

	public int getDatasize() {
		return datasize;
	}

	public int getDigits() {
		return digits;
	}

	public int getNullable() {
		return nullable;
	}

	public String getAutoincrement() {
		return autoincrement;
	}

	public String compare(ColumnModel destCol) {
		StringBuffer ret = new StringBuffer();
		if(!columnType.equals(destCol.getColumnType())){
			ret.append(String.format("%s字段类型不一致，源字段类型为:%s,目标字段类型为:%s \r\n",columnName,columnType,destCol.getColumnType()));
		}
		if(datasize != destCol.getDatasize()){
			ret.append(String.format("%s字段长度不一致，源字段长度为:%s,目标字段长度为:%s \r\n",columnName,datasize,destCol.getDatasize()));
		}
		if(digits != destCol.getDigits()){
			ret.append(String.format("%s字段小数点不一致，源字段小数点为:%s,目标字段小数点为:%s \r\n",columnName,digits,destCol.getDigits()));
		}
		if(nullable != destCol.getNullable()){
			ret.append(String.format("%s字段非空性不一致，源字段%s为空,目标字段%s为空 \r\n",columnName,nullable==1?"YES允许":"NO不允许",destCol.getNullable()==1?"YES允许":"NO不允许"));
		}
		if(!autoincrement.equals(destCol.getAutoincrement())){
			ret.append(String.format("%s字段自增性不一致，源字段%s自增,目标字段%s自增 \r\n",columnName,"YES".equals(autoincrement)?"YES允许":"NO不允许","YES".equals(destCol.getAutoincrement())?"YES允许":"NO不允许"));
		}
		return ret.toString();
	}
}
