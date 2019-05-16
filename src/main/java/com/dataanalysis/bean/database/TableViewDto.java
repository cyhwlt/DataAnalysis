package com.dataanalysis.bean.database;

import java.util.List;

/**
 * 表结构实体
 * @author soyuan
 *
 */
public class TableViewDto {
	private String tableName; // 表名称
	private List<FieldViewDto> fields;

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public List<FieldViewDto> getFieldArr() {
		return fields;
	}

	public void setFieldArr(List<FieldViewDto> fieldArr) {
		this.fields = fieldArr;
	}
}
