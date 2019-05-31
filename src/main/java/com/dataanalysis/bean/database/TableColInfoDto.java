package com.dataanalysis.bean.database;

import java.util.List;
import java.util.Map;

/**
 * 表、主键字段、日期字段、所有字段实体
 * 
 * @author soyuan
 *
 */
public class TableColInfoDto {

	private String tableName; // 表名
	private String pkColName; // 主键字段
	private String dateColName; // 日期字段
	private String colNameStr; // 所有字段拼接字符串
	private List<Map<String, String>> filterColName; // 筛选字段

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getPkColName() {
		return pkColName;
	}

	public void setPkColName(String pkColName) {
		this.pkColName = pkColName;
	}

	public String getDateColName() {
		return dateColName;
	}

	public void setDateColName(String dateColName) {
		this.dateColName = dateColName;
	}

	public String getColNameStr() {
		return colNameStr;
	}

	public void setColNameStr(String colNameStr) {
		this.colNameStr = colNameStr;
	}

	public List<Map<String, String>> getFilterColName() {
		return filterColName;
	}

	public void setFilterColName(List<Map<String, String>> filterColName) {
		this.filterColName = filterColName;
	}
}
