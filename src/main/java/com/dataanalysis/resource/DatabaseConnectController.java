package com.dataanalysis.resource;

import com.dataanalysis.bean.ResultDto;
import com.dataanalysis.bean.database.DatabaseDto;
import com.dataanalysis.bean.database.DatabaseViewDto;
import com.dataanalysis.bean.database.TableViewDto;
import com.dataanalysis.service.DatabaseConnectService;
import com.dataanalysis.utils.DBConnectUtil;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/database")
public class DatabaseConnectController {

	private static Logger logger = LogManager.getLogger(DatabaseConnectController.class);

	@Autowired
	private DatabaseConnectService dbcService;

	/**
	 * 测试数据库连接
	 * @param dto 测试数据库连接
	 * @return
	 */
	@PostMapping("/testConnect")
	@ResponseBody
	public ResponseEntity<ResultDto> testConnect(@RequestBody DatabaseDto dto) {
		ResultDto resultDto = new ResultDto();
		Connection testConnection = DBConnectUtil.getConnection(dto);
		if (testConnection != null) {
			try {
				if (!testConnection.isClosed()) {
					resultDto.setCode(0);
					resultDto.setData(true);
					resultDto.setMessage("数据库连接成功");
					logger.info("数据库连接成功");
					return new ResponseEntity<ResultDto>(resultDto, HttpStatus.OK);
				} else {
					resultDto.setCode(-1);
					resultDto.setMessage("数据库连接失败");
					logger.error("数据库连接失败");
					return new ResponseEntity<ResultDto>(resultDto, HttpStatus.INTERNAL_SERVER_ERROR);
				}
			} catch (SQLException e) {
				e.printStackTrace();
				resultDto.setCode(-1);
				resultDto.setMessage("数据库连接失败");
				logger.error("数据库连接失败");
				return new ResponseEntity<ResultDto>(resultDto, HttpStatus.INTERNAL_SERVER_ERROR);
			}
		} else {
			resultDto.setCode(-1);
			resultDto.setMessage("数据库连接失败");
			logger.error("数据库连接失败");
			return new ResponseEntity<ResultDto>(resultDto, HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}

	/**
	 * 连接成功以后获取数据库列表(含表、字段)
	 * 
	 * @param dto
	 * @return
	 * @throws
	 * @throws SQLException
	 */
	@PostMapping("/getDBs")
	@ResponseBody
	public ResponseEntity<ResultDto> getDBs(@RequestBody DatabaseDto dto) {
		ResultDto resultDto = new ResultDto();
		try {
			List<DatabaseViewDto> resultData = this.dbcService.getAllDBInfo(dto);
			resultDto.setCode(0);
			resultDto.setData(resultData);
			logger.info("获取数据库结构成功");
			return new ResponseEntity<ResultDto>(resultDto, HttpStatus.OK);
		} catch (Exception e) {
			resultDto.setCode(-1);
			resultDto.setMessage(e.getMessage());
			logger.error("获取数据库结构失败：" + e.getMessage());
			return new ResponseEntity<ResultDto>(resultDto, HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}

	@PostMapping("/getdbs")
	@ResponseBody
	public ResponseEntity<ResultDto> getdbs(@RequestBody DatabaseDto dto) {
		ResultDto resultDto = new ResultDto();
		List<String> dbs = new ArrayList<>();
		try {
			List<DatabaseViewDto> resultData = this.dbcService.getAllDBInfo(dto);
			for (DatabaseViewDto dbDto : resultData) {
				dbs.add(dbDto.getDbName());
			}
			resultDto.setCode(0);
			resultDto.setData(dbs);
			logger.info("获取数据库结构成功");
			return new ResponseEntity<ResultDto>(resultDto, HttpStatus.OK);
		} catch (Exception e) {
			resultDto.setCode(-1);
			resultDto.setMessage(e.getMessage());
			logger.error("获取数据库结构失败：" + e.getMessage());
			return new ResponseEntity<ResultDto>(resultDto, HttpStatus.INTERNAL_SERVER_ERROR);
		}
	}
	
	@PostMapping("/test")
	public List<TableViewDto> test(@RequestBody DatabaseDto dto) throws Exception{
//		Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
//		Connection connection = DriverManager.getConnection("jdbc:sqlserver://172.16.100.151:1433;DatabaseName=Sqoop_Test","sa","Admin123");
//		PreparedStatement prepareStatement = connection.prepareStatement("select * from flower");
//		ResultSet executeQuery = prepareStatement.executeQuery();
//		while (executeQuery.next()) {
//			logger.info(">>>>>>" + executeQuery.getString(2));
//		}
		return this.dbcService.getTables(dto);
	}
}
