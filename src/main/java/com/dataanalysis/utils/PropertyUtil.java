package com.dataanalysis.utils;

import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

public class PropertyUtil {
	
	private static Logger logger = LogManager.getLogger(PropertyUtil.class);
	
	public static Properties getProperty(String file){
		
		Properties prop = new Properties();
//		InputStream in = null;
		try {
			// 读取属性文件
//			in = Object.class.getResourceAsStream(file);
//			prop.load(in);
			Resource classPathResource = new ClassPathResource(file);
			prop = PropertiesLoaderUtils.loadProperties(classPathResource);
		} catch (Exception e) {
			logger.error("读取文件失败" + e.getMessage());
		} 
		return prop;
	}
}
