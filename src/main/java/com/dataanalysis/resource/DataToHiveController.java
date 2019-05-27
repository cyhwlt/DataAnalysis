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
	* @date 2019年5月9日
	* @author chenyh
	* @param dto
	 */
	@PostMapping("/tohdfs")
	public void dataToHdfs(@RequestBody DatabaseDto dto){
		this.dthService.dataToHdfs(dto);
	}
	
	/**
	 * 
	* @date 2019年5月10日
	* @author chenyh
	* @param dto 
	 */
	@PostMapping("/loadHive")
	public void loadDataToHive(@RequestBody LoadDataDto dto){
		this.dthService.loadDataToHive(dto);
	}
	
	/**
	 * 创建hive分区表
	 * @param dto
	 */
	@PostMapping("/createhivetable")
	public void createHiveTable(@RequestBody DatabaseDto dto) {
		this.dthService.createHiveTable(dto);
	}

	@PostMapping("/importtohive")
	public void importToHive(@RequestBody DatabaseDto dto) {
		this.dthService.dataToHdfs(dto);
		this.dthService.createHiveTable(dto);
		DatabaseDto hiveDto = DBConnectUtil.hiveDtoJoint(dto);
		LoadDataDto loadDataDto = new LoadDataDto();
		loadDataDto.setHiveDb(hiveDto);
		loadDataDto.setOriginDb(dto);
		this.dthService.loadDataToHive(loadDataDto);
	}
}
