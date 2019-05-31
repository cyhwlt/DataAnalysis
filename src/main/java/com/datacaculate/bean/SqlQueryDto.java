package com.datacaculate.bean;

/**
 * sql查询实体
 */
public class SqlQueryDto {
    private String dbName; // 数据库名称
    private String sql; //sql语句

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }
}
