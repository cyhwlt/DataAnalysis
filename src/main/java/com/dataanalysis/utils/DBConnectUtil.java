package com.dataanalysis.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.dataanalysis.bean.database.DatabaseDto;
import com.dataanalysis.enums.DataBaseType;

public class DBConnectUtil {

	private static Logger logger = LogManager.getLogger(DBConnectUtil.class);

	/**
	 * 获取数据库连接信息
	 * 
	 * @param dto数据库连接实体
	 * @return
	 */
	public static Connection getConnection(DatabaseDto dto) {
		String dbName = null;
		String dbPort = String.valueOf(dto.getPort()).trim();
		String hostName = dto.getIp().trim();
		String userName = dto.getUserName();
		String password = dto.getPassword();
		Connection conn = null;
		try {
			switch (dto.getDataBaseType()) {
			case MySQL:
				dbName = dto.getDbName();
				Class.forName("com.mysql.jdbc.Driver");
				if (dbName != null && dbName.length() != 0) { // 获取表、字段
					conn = DriverManager.getConnection("jdbc:mysql://" + hostName + ":" + dbPort + "/" + dbName + ""
							+ "?useUnicode=true&characterEncoding=utf8", userName, password);
				} else { // 获取数据库
					conn = DriverManager.getConnection("jdbc:mysql://" + hostName + ":" + dbPort, userName,
							password);
				}
				break;
			case SQLServer:
				dbName = dto.getDbName();
				Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
				if (dbName != null && dbName.length() != 0) { // 获取表、字段
					conn = DriverManager.getConnection("jdbc:sqlserver://" + hostName + ":" + dbPort + ";DatabaseName=" + dbName, userName,
							password);
				} else {
					conn = DriverManager.getConnection("jdbc:sqlserver://" + hostName + ":" + dbPort, userName,
							password);
				}
				break;
			case Oracle:
				dbName = dto.getServerName();
				Class.forName("oracle.jdbc.OracleDriver");
				conn = DriverManager.getConnection("jdbc:oracle:thin:@" + hostName + ":" + dbPort + ":" + dbName,
						userName, password);
				break;
			case Hive:
				dbName = dto.getDbName();
				Class.forName("org.apache.hive.jdbc.HiveDriver");
				conn = DriverManager.getConnection("jdbc:hive2://" + hostName + ":" + dbPort + "/" + dbName, userName,
						password);
				break;
			default:
				break;
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
		return conn;
	}

	/**
	 * 拼写hive数据库实体
	 * @param dto
	 * @return
	 */
	public static DatabaseDto hiveDtoJoint(DatabaseDto dto) {
		DatabaseDto hiveDto = new DatabaseDto();
		Properties property = PropertyUtil.getProperty("/config.properties");
		String remoteIp = property.getProperty("remoteip");
		String remotePort = property.getProperty("remoteport");
		String remoteUser = property.getProperty("remoteuser");
		String remotePassword = property.getProperty("remotepassword");
		hiveDto.setIp(remoteIp);
		hiveDto.setPort(Integer.parseInt(remotePort));
		hiveDto.setUserName(remoteUser);
		hiveDto.setPassword(remotePassword);
		hiveDto.setDbName(dto.getDbName());
		hiveDto.setDataBaseType(DataBaseType.Hive);
		return hiveDto;
	}
}
