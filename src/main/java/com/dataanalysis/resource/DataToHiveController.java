package com.dataanalysis.resource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.dataanalysis.bean.LoadDataDto;
import com.dataanalysis.bean.database.DatabaseDto;
import com.dataanalysis.service.DataToHiveService;
import com.dataanalysis.utils.DBConnectUtil;

@RestController
@RequestMapping("/sqoop")
public class DataToHiveController {

	@Autowired
	private DataToHiveService dthService;

	/**
	 * 将数据源导入到hdfs:1、将要导入的数据库表写入脚本，2、执行生成的job
	 * 
	 * @date 2019年5月9日
	 * @author chenyh
	 * @param jobInfo
	 */
	@PostMapping("/tohdfs")
	public void dataToHdfs(@RequestBody DatabaseDto dto) {
		this.dthService.dataToHdfs(dto);
	}

	/**
	 * 
	 * @date 2019年5月10日
	 * @author chenyh
	 * @param dto
	 */
	@PostMapping("/loadHive")
	public void loadDataToHive(@RequestBody LoadDataDto dto) {
		this.dthService.loadDataToHive(dto);
	}

	/**
	 * 创建hive分区表
	 * 
	 * @param dto
	 */
	@PostMapping("/createhivetable")
	public void createHiveTable(@RequestBody DatabaseDto dto) {
		this.dthService.createHiveTable(dto);
	}

	/**
	 * 将数据从关系型数据库导入hive
	 * 
	 * @param dto
	 */
	@PostMapping("/importtohive")
	public void importToHive(@RequestBody DatabaseDto dto) {
		// 1、将数据导入hdfs
		this.dthService.dataToHdfs(dto);
		// 2、创建hive分区表
		this.dthService.createHiveTable(dto);
		DatabaseDto hiveDto = DBConnectUtil.hiveDtoJoint(dto);
		LoadDataDto loadDataDto = new LoadDataDto();
		loadDataDto.setHiveDb(hiveDto);
		loadDataDto.setOriginDb(dto);
		// 3、将数据load到hive数据库
		this.dthService.loadDataToHive(loadDataDto);
		// 4、数据简单清洗
		this.dthService.dataClean(loadDataDto);
	}

	/**
	 * 数据清洗方法测试(数据去重、过滤空值)
	 * 
	 * @param dto
	 */
	@PostMapping("/dataClean")
	public void dataClean(@RequestBody DatabaseDto dto) {
		DatabaseDto hiveDto = DBConnectUtil.hiveDtoJoint(dto);
		LoadDataDto loadDataDto = new LoadDataDto();
		loadDataDto.setHiveDb(hiveDto);
		loadDataDto.setOriginDb(dto);
		this.dthService.dataClean(loadDataDto);
	}
}
