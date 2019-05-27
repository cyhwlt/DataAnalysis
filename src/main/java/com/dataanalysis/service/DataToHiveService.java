package com.dataanalysis.service;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.dataanalysis.bean.LoadDataDto;
import com.dataanalysis.bean.database.DatabaseDto;
import com.dataanalysis.bean.database.FieldViewDto;
import com.dataanalysis.bean.database.TableViewDto;
import com.dataanalysis.enums.DataBaseType;
import com.dataanalysis.utils.DBConnectUtil;
import com.dataanalysis.utils.PropertyUtil;
import com.dataanalysis.utils.RemoteShellUtil;

@Service
public class DataToHiveService {

	private static Logger logger = LogManager.getLogger(DataToHiveService.class);

	Properties property = PropertyUtil.getProperty("/config.properties");
	String remoteIp = property.getProperty("remoteip");
	String remotePort = property.getProperty("remoteport");
	String remoteUser = property.getProperty("remoteuser");
	String remotePassword = property.getProperty("remotepassword");
	

	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	@Autowired
	private DatabaseConnectService dbcService;

	public void dataToHdfs(DatabaseDto dto) {
		String jobPath = property.getProperty("datajob");
		StringBuilder sBuilder = new StringBuilder();
		jobPath = jobPath + dto.getDbName() + "_" + dto.getIp(); //linux下存放每个数据库所有表的文件夹
		//创建脚本文件
		RemoteShellUtil rs = new RemoteShellUtil(remoteIp, remoteUser, remotePassword, "utf-8");
		rs.exec("mkdir " + jobPath, true); //创建库文件夹
		String writeJob = jobPath + "/" + dto.getDbName() + ".sh";
		logger.info(">>>>>>>>>>>>" + writeJob);
		rs.exec("touch " + writeJob, true); //创建生成job的脚本文件
		logger.info(">>>>>>>touchFile成功");
		List<String> tables = new ArrayList();
		List<String> tabInfos = new ArrayList<>();
		
		// 获取所有表
		List<TableViewDto> tables2 = this.dbcService.getTables(dto);
		for (TableViewDto tvDto : tables2) {
			StringBuilder sb = new StringBuilder();
			String table = tvDto.getTableName();
			tables.add(table);
			sb.append(table).append(",");
			List<FieldViewDto> fieldArr = tvDto.getFieldArr();
			for (FieldViewDto fvDto : fieldArr) {
				if(fvDto.getIsPK()){
					sb.append(fvDto.getColName()).append(",");
					break;
				}
			}
			for (FieldViewDto fvDto : fieldArr) {
				String typeName = fvDto.getTypeName();
				if(typeName.toUpperCase().equals("DATETIME")){
					sb.append(fvDto.getColName());
					break;
				}
			}
			tabInfos.add(sb.toString());
		}
		
		//写入sqoop命令行
		for (String tabInfo : tabInfos) {
			String sqoop = RemoteShellUtil.sqooptoHdfs(dto, tabInfo);
			rs.exec("echo \t\"" + sqoop + "\" >> " + writeJob, true);
			logger.info(">>>>>>>insertSqoop成功");
		}

		// 文件赋权限
		rs.exec("chmod 777 " + writeJob, true);
		logger.info(">>>>>>>changeLimit成功");

		// 执行sqoop命令(创建job)
		rs.exec("sh " + writeJob, true);
		logger.info(">>>>>>>execSqoop成功");

		String execJob = jobPath + "/execJob.sh";
		// 执行job
		for (int i = 0; i < tables.size(); i++) {

			rs.exec("touch " + execJob, true); // 创建执行job的脚本文件
			rs.exec("echo \t\"sqoop job --exec " + tables.get(i) + "\" >> " + execJob, true); // 将执行job的命令行写入脚本
		}
		rs.exec("chmod 777 " + execJob, true);
		rs.exec("sh " + execJob, false);
		logger.info(">>>>>>>execJob成功");

	}

	public void loadDataToHive(LoadDataDto dto) {
		//连接hive
		Connection conn = DBConnectUtil.getConnection(dto.getHiveDb());
		Statement cs = null;
		try {
			cs = conn.createStatement();
			String partitionTime = sdf.format(new Date());
			
			List<String> tables = new ArrayList();
			// 获取所有表
			List<TableViewDto> tables2 = this.dbcService.getTables(dto.getOriginDb());
			for (TableViewDto tvDto : tables2) {
				tables.add(tvDto.getTableName());
			}
			
			cs.execute("use " + dto.getOriginDb().getDbName());
			for (String table : tables) {
				String path = "/user/hive/warehouse/" + dto.getOriginDb().getDbName() + "/" + table + "/*";
				cs.execute("load data inpath '" + path + "' into table " + table + " partition(time_partition='" + partitionTime + "')");
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		} finally {
			try {
				cs.close();
				conn.close();
			} catch (SQLException e) {
				logger.error(e.getMessage());
			}
		}
	}

	/**
	 * 创建hive表命令行拼接
	 * @param dto
	 * @param table
	 * @return
	 */
	public String createHiveTableCmdJoint(DatabaseDto dto, String table) {
		StringBuilder sBuilder = new StringBuilder();
		sBuilder.append("sqoop create-hive-table");

		switch (dto.getDataBaseType()) {
		case MySQL:
			sBuilder.append(" --connect jdbc:mysql://").append(dto.getIp()).append(":").append(dto.getPort())
					.append("/").append(dto.getDbName());
			sBuilder.append(" --username ").append(dto.getUserName()).append(" --password ").append(dto.getPassword());
			break;
		case SQLServer:
			sBuilder.append(" --connect 'jdbc:sqlserver://").append(dto.getIp()).append(":").append(dto.getPort());
			sBuilder.append(";username=").append(dto.getUserName()).append(";password=").append(dto.getPassword())
					.append(";database=").append(dto.getDbName()).append("'");
			break;
		case Oracle:
			sBuilder.append(" --connect jdbc:oracle:thin:@").append(dto.getIp()).append(":").append(dto.getPort())
					.append(":").append(dto.getServerName());
			sBuilder.append(" --username ").append(dto.getUserName()).append(" --password ").append(dto.getPassword());
			break;
		default:
			break;
		}
		sBuilder.append(" --table ").append(table);
		sBuilder.append(" --hive-database ").append(dto.getDbName());
		sBuilder.append(" --hive-table ").append(table);
		sBuilder.append(" --fields-terminated-by ',' --hive-partition-key time_partition");
		return sBuilder.toString();
	}

	/**
	 * 创建hive数据库
	 * @param dto
	 */
	public void createHiveDatabase(DatabaseDto dto) {
		// 连接hive	
		DatabaseDto databaseDto = DBConnectUtil.hiveDtoJoint(dto);
		Connection conn = DBConnectUtil.getConnection(databaseDto);
		Statement cs = null;
		try {
			cs = conn.createStatement();
			cs.execute("create database if not exists " + dto.getDbName());
			logger.error("创建hive数据库成功");
		} catch (SQLException e) {
			logger.error("创建hive数据库错误信息:"+e.getMessage());
		} finally {
			try {
				cs.close();
				conn.close();
			} catch (SQLException e) {
				logger.error("创建hive数据库错误信息:"+e.getMessage());
			}
		}
	}

	/**
	 * 创建hive分区表
	 * 
	 * @param dto
	 */
	public void createHiveTable(DatabaseDto dto) {
		String jobPath = property.getProperty("datajob");
		jobPath = jobPath + dto.getDbName() + "_" + dto.getIp(); //linux下存放每个数据库所有表的文件夹
		// 创建hive数据库
		this.createHiveDatabase(dto);
		StringBuilder sBuilder = new StringBuilder();
		sBuilder.append(dto.getDbName()).append("_").append(dto.getIp());
		// 创建脚本文件
		String script = jobPath + "/" + sBuilder.toString() + ".sh";
		RemoteShellUtil rs = new RemoteShellUtil(remoteIp, remoteUser, remotePassword, "utf-8");
		rs.exec("touch " + script, true);
		logger.info("createHiveTable>>>>>>>touchFile成功");
		List<String> tables = new ArrayList<>();
		
		// 获取所有表
		List<TableViewDto> tables2 = this.dbcService.getTables(dto);
		for (TableViewDto tvDto : tables2) {
			tables.add(tvDto.getTableName());
		}
		// 写入sqoop创建hive分区表命令行
		for (String table : tables) {
			String sqoop = this.createHiveTableCmdJoint(dto, table);
			rs.exec("echo \t\"" + sqoop + "\" >> " + script, true);
			logger.info("createHiveTable>>>>>>>insertSqoop成功");
		}
		// 文件赋权限
		rs.exec("chmod 777 " + script, true);
		logger.info("createHiveTable>>>>>>>changeLimit成功");
		// 执行sqoop命令
		rs.exec("sh " + script, true);
		logger.info("createHiveTable>>>>>>>execSqoop成功");
	}
}
