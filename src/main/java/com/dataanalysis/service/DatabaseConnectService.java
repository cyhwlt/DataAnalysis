package com.dataanalysis.service;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import com.dataanalysis.bean.database.DatabaseDto;
import com.dataanalysis.bean.database.DatabaseViewDto;
import com.dataanalysis.bean.database.FieldViewDto;
import com.dataanalysis.bean.database.TableColInfoDto;
import com.dataanalysis.bean.database.TableViewDto;
import com.dataanalysis.enums.DataBaseType;
import com.dataanalysis.utils.DBConnectUtil;

@Service
public class DatabaseConnectService {

	private static Logger logger = LogManager.getLogger(DatabaseConnectService.class);

	/**
	 * 获取数据库结构
	 * 
	 * @param dto 实体
	 * @return
	 * @throws SQLException
	 */
	public List<DatabaseViewDto> getAllDBInfo(DatabaseDto dto) {
		Connection conn = DBConnectUtil.getConnection(dto);
		List<DatabaseViewDto> dbs = new ArrayList<DatabaseViewDto>();
		List<TableViewDto> tables = new ArrayList<TableViewDto>();
		ResultSet dbRs = null;
		try {
			// 获取数据库
			DatabaseMetaData dm = conn.getMetaData();
			switch (dto.getDataBaseType()) {
			case MySQL:
				dbRs = dm.getCatalogs();
				while (dbRs.next()) {
					if ((!dbRs.getString("TABLE_CAT").contains("schema"))
							&& (!dbRs.getString("TABLE_CAT").contains("mysql"))
							&& (!dbRs.getString("TABLE_CAT").contains("sys"))) {
						DatabaseViewDto dbViewDto = new DatabaseViewDto();

						dbViewDto.setDbName(dbRs.getString("TABLE_CAT"));
						// 获取表、字段结构
						dto.setDbName(dbRs.getString("TABLE_CAT"));
						tables = this.getTables(dto);
						dbViewDto.setTableArr(tables);
						dbs.add(dbViewDto);

					}
				}
				break;
			case SQLServer:
				dbRs = dm.getCatalogs();
				while (dbRs.next()) {
					if ((!dbRs.getString("TABLE_CAT").contains("master"))
							&& (!dbRs.getString("TABLE_CAT").contains("model"))
							&& (!dbRs.getString("TABLE_CAT").contains("msdb"))
							&& (!dbRs.getString("TABLE_CAT").contains("tempdb"))) {
						DatabaseViewDto dbViewDto = new DatabaseViewDto();

						dbViewDto.setDbName(dbRs.getString("TABLE_CAT"));
						// 获取表、字段结构
						dto.setDbName(dbRs.getString("TABLE_CAT"));
						tables = this.getTables(dto);
						dbViewDto.setTableArr(tables);
						dbs.add(dbViewDto);

					}
				}
				break;
			case Oracle:
				dbRs = dm.getTables("null", dto.getUserName(), "%", new String[] { "TABLE" });
				while (dbRs.next()) {
					DatabaseViewDto dbViewDto = new DatabaseViewDto();
					dbViewDto.setDbName(dto.getServerName());
					dto.setDbName(dto.getServerName());
					tables = this.getTables(dto);
					dbViewDto.setTableArr(tables);
					dbs.add(dbViewDto);

					break;
				}
				break;
			default:
				break;
			}
		} catch (SQLException e) {
			logger.error("获取数据库信息异常: " + e.getMessage());
		} finally {
			if (conn != null) {
				try {
					dbRs.close();
					conn.close();
				} catch (SQLException e) {
					logger.error("SQLException: " + e.getMessage());
				}
			}
		}
		return dbs;
	}

	/**
	 * 获取表和字段结构
	 * @param dbDto 数据库实体
	 * @return
	 */
	public List<TableViewDto> getTables(DatabaseDto dbDto) {
		Connection conn = DBConnectUtil.getConnection(dbDto);
		ResultSet tableRs = null;
		ResultSet fieldRs = null;
		final List<String> tableNames = new ArrayList<>();
		List<TableViewDto> tables = new ArrayList<TableViewDto>();
		try {
			// 获取表结构
			DatabaseMetaData dm = conn.getMetaData();
			if (dbDto.getDataBaseType().equals(DataBaseType.Oracle)) {
				tableRs = dm.getTables("null", dbDto.getUserName(), "%", new String[] { "TABLE" });
			} else {
				tableRs = dm.getTables(dbDto.getDbName(), null, null, new String[] { "TABLE" });
			}
			while (tableRs.next()) {
				List<FieldViewDto> fields = new ArrayList<FieldViewDto>();
				TableViewDto tableViewDto = new TableViewDto();
				if ((!tableRs.getString("TABLE_NAME").contains("trace_xe_action_map")) // 过滤系统表
						&& (!tableRs.getString("TABLE_NAME").contains("trace_xe_event_map"))) {
					String tableName = tableRs.getString("TABLE_NAME");
					tableViewDto.setTableName(tableName);
					tableNames.add(tableName);
					// 获取字段结构
					fieldRs = dm.getColumns(null, "%", tableName, "%");
					ResultSet rs = conn.getMetaData().getPrimaryKeys(null, null, tableName); // 获取主键字段
					while (fieldRs.next()) {
						FieldViewDto fieldViewDto = new FieldViewDto();
						fieldViewDto.setColName(fieldRs.getString("COLUMN_NAME"));
						fieldViewDto.setTypeName(fieldRs.getString("TYPE_NAME"));
						while (rs.next()) {
							if (fieldRs.getString("COLUMN_NAME").equals(rs.getString(4))) {
								fieldViewDto.setIsPK(true);
							} else {
								fieldViewDto.setIsPK(false);
							}
						}
						fields.add(fieldViewDto);
					}
					tableViewDto.setFieldArr(fields);
					tables.add(tableViewDto);
				}
			}
		} catch (SQLException e) {
			logger.error("获取数据库信息: " + e.getMessage());
		} finally {
			if (conn != null) {
				try {
					fieldRs.close();
					tableRs.close();
					conn.close();
				} catch (SQLException e) {
					logger.error("SQLException: " + e.getMessage());
				}
			}
		}
		return tables;
	}

	/**
	 * 获取表主键日期字段信息
	 * 
	 * @param dto
	 * @return
	 */
	public List<TableColInfoDto> getColInfo(DatabaseDto dto) {

		List<TableColInfoDto> tcDtos = new ArrayList<>();
		TableColInfoDto tcDto = new TableColInfoDto();
		List<Map<String, String>> filterColName = new ArrayList<Map<String, String>>();

		// 获取所有表
		List<TableViewDto> tables = this.getTables(dto);
		for (TableViewDto tvDto : tables) {
			tcDto.setTableName(tvDto.getTableName());
			List<FieldViewDto> fieldArr = tvDto.getFieldArr();
			StringBuilder colName = new StringBuilder();// 所有字段
			Map<String, String> colNameMap = new HashMap<>();
			for (FieldViewDto fvDto : fieldArr) {
				String typeName = fvDto.getTypeName();
				if (fvDto.getIsPK()) {
					tcDto.setPkColName(fvDto.getColName());
				} else if (typeName.toUpperCase().equals("DATETIME")) {
					tcDto.setDateColName(fvDto.getColName());
					colNameMap.put("colName", fvDto.getColName());
				}
				colName.append(fvDto.getColName().toLowerCase());
				colName.append(",");
				// else if(fvDto.getColName().equals("totalNum")){
				// colNameMap.put("colName", fvDto.getColName());
				// } // 测试使用代码

			}
			filterColName.add(colNameMap);
			tcDto.setColNameStr(colName.toString().substring(0, colName.toString().length() - 1));
			tcDto.setFilterColName(filterColName);
			tcDtos.add(tcDto);
		}
		return tcDtos;
	}

}
