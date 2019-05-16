package com.dataanalysis.bean.database;

import java.util.List;

/**
 * 数据库结构实体
 * @author soyuan
 *
 */
public class DatabaseViewDto {

	private String dbName; // 数据库名称
	private List<TableViewDto> tables; // 数据库表数组
	public String getDbName() {
		return dbName;
	}
	public void setDbName(String dbName) {
		this.dbName = dbName;
	}
	public List<TableViewDto> getTableArr() {
		return tables;
	}
	public void setTableArr(List<TableViewDto> tableArr) {
		this.tables = tableArr;
	}
}
