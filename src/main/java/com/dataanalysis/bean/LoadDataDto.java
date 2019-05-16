package com.dataanalysis.bean;

import com.dataanalysis.bean.database.DatabaseDto;

public class LoadDataDto {
	private DatabaseDto hiveDb; //连接hive信息
	private DatabaseDto originDb; //连接源数据库信息

	public DatabaseDto getHiveDb() {
		return hiveDb;
	}
	public void setHiveDb(DatabaseDto hiveDb) {
		this.hiveDb = hiveDb;
	}
	public DatabaseDto getOriginDb() {
		return originDb;
	}
	public void setOriginDb(DatabaseDto originDb) {
		this.originDb = originDb;
	}
	
	
}
