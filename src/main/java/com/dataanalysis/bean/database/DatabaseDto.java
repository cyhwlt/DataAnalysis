package com.dataanalysis.bean.database;

import com.dataanalysis.enums.DataBaseType;

public class DatabaseDto {
    private String ip; // 服务器IP地址
    private Integer port; // 端口号
    private String userName; // 用户名
    private String password; // 密码
    private String remarkName; // 连接名称
    private String serverName; // 实例名(oracle数据库连接使用)
    private DataBaseType dataBaseType; // 连接类型
    private String accessType; // 连接方式
    private String dbEncodingKey; // 设置数据库编码命名参数
    private String dbEncodingValue; // 设置数据库编码值

    private String dbName; // 数据库名称


    // private List<String> tableNames; //表名
    private String hiveDb; // 导入到hive哪个库里
    private String primaryKey; // 主键列

    private String dateKey; // 日期字段


    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }


    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getRemarkName() {
        return remarkName;
    }

    public void setRemarkName(String remarkName) {
        this.remarkName = remarkName;
    }

    public String getServerName() {
        return serverName;
    }

    public void setServerName(String serverName) {
        this.serverName = serverName;
    }

    public DataBaseType getDataBaseType() {
        return dataBaseType;
    }

    public void setDataBaseType(DataBaseType dataBaseType) {
        this.dataBaseType = dataBaseType;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getAccessType() {
        return accessType;
    }

    public void setAccessType(String accessType) {
        this.accessType = accessType;
    }

    public String getDbEncodingKey() {
        return dbEncodingKey;
    }

    public void setDbEncodingKey(String dbEncodingKey) {
        this.dbEncodingKey = dbEncodingKey;
    }

    public String getDbEncodingValue() {
        return dbEncodingValue;
    }

    public void setDbEncodingValue(String dbEncodingValue) {
        this.dbEncodingValue = dbEncodingValue;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public Integer getPort() {
        return port;
    }

    // public List<String> getTableNames() {
    // return tableNames;
    // }
    //
    // public void setTableNames(List<String> tableNames) {
    // this.tableNames = tableNames;
    // }

    public String getHiveDb() {
        return hiveDb;
    }

    public void setHiveDb(String hiveDb) {
        this.hiveDb = hiveDb;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public String getDateKey() {
        return dateKey;
    }

    public void setDateKey(String dateKey) {
        this.dateKey = dateKey;
    }

}
