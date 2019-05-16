package com.dataanalysis.bean.database;

/**
 * 字段实体
 * @author soyuan
 *
 */
public class FieldViewDto {
	private String colName; // 字段名称
	private String typeName; // 字段类型
	private boolean isPK; // 是否是主键
	public boolean getIsPK() {
		return isPK;
	}
	public void setIsPK(boolean isPK) {
		this.isPK = isPK;
	}
	public String getColName() {
		return colName;
	}
	public void setColName(String colName) {
		this.colName = colName;
	}
	public String getTypeName() {
		return typeName;
	}
	public void setTypeName(String typeName) {
		this.typeName = typeName;
	}
}
