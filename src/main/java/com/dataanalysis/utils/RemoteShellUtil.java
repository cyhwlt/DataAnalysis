package com.dataanalysis.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.Charset;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.dataanalysis.bean.database.DatabaseDto;

import ch.ethz.ssh2.ChannelCondition;
import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;

public class RemoteShellUtil {

    private Connection conn;
    private String ipAddr;
    private String charset = Charset.defaultCharset().toString();
    private String userName;
    private String password;
    
    private static Logger logger = LogManager.getLogger(RemoteShellUtil.class);

    public RemoteShellUtil(String ipAddr, String userName, String password, String charset) {
        this.ipAddr = ipAddr;
        this.userName = userName;
        this.password = password;
        if (charset != null) {
            this.charset = charset;
        }
    }

    public boolean login() throws IOException {
        conn = new Connection(ipAddr);
        conn.connect(); // 连接
        return conn.authenticateWithPassword(userName, password); // 认证
        
    }

    public String exec(String cmds, boolean flag) {
        InputStream in = null;
        StringBuffer buffer = new StringBuffer();
        String result = "";
        try {
            if (this.login()) {
            	Session session = conn.openSession();// 打开一个会话
            	session.requestPTY("bash");
            	session.startShell();
            	PrintWriter out = new PrintWriter(session.getStdin());
	    	    out.println(cmds);
	            out.println("exit");
	            out.close();
	            
	            session.waitForCondition(ChannelCondition.CLOSED | ChannelCondition.EOF | ChannelCondition.EXIT_STATUS , 30000);
	            InputStream stdout = new StreamGobbler(session.getStdout());
	            BufferedReader br = new BufferedReader(new InputStreamReader(stdout));
	            String line = null;
	            while ((line = br.readLine()) != null) {
	                buffer.append(line + "\n");
	            }
	            if(!flag){
	            	if (session != null) {
	            		session.close();
	            	}
	            	if (conn != null) {
	            		conn.close();
	            	}
	            }
            }
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        System.out.println(buffer.toString());
        return result;
    }

    public static String sqooptoHdfs(DatabaseDto dto, String tabInfo){
    	String[] tabs = tabInfo.split(","); // table, primaryKey, dataKey
    	
    	StringBuilder sBuilder = new StringBuilder();
		sBuilder.append("sqoop job '-Dorg.apache.sqoop.splitter.allow_text_splitter=true' --create ");
		//创建脚本
		switch(dto.getDataBaseType()){
		case Oracle:
			sBuilder.append(tabs[0]);
			sBuilder.append(" -- import --connect ").append("jdbc:oracle:thin:@").append(dto.getIp()).append(":").append(dto.getPort()).append(":").append(dto.getServerName());
			sBuilder.append(" --username ").append(dto.getUserName());
			sBuilder.append(" --password ").append(dto.getPassword());
			sBuilder.append(" --table ").append(tabs[0]);
//			sBuilder.append(" --hive-import ");
			sBuilder.append(" --target-dir ").append("/user/hive/warehouse/").append(dto.getDbName()).append("/").append(tabs[0]);
			if(dto.getHiveDb() != null){
				sBuilder.append("--hive-database ").append(dto.getHiveDb());
			}
			sBuilder.append(" --check-column ").append(tabs[2]);
			sBuilder.append(" --incremental lastmodified --last-value '1970-01-01 00:00:00'");
			sBuilder.append(" --merge-key ").append(tabs[1]);
			break;
		case SQLServer:
			sBuilder.append(tabs[0]);
			sBuilder.append(" -- import --connect 'jdbc:sqlserver://").append(dto.getIp()).append(";username=").append(dto.getUserName());
			sBuilder.append(";password=").append(dto.getPassword()).append(";database=").append(dto.getDbName()).append("'");
			sBuilder.append(" --table ").append(tabs[0]);
			sBuilder.append(" --target-dir ").append("/user/hive/warehouse/").append(dto.getDbName()).append("/").append(tabs[0]);
			if(dto.getHiveDb() != null){
				sBuilder.append("--hive-database ").append(dto.getHiveDb());
			}
			sBuilder.append(" --check-column ").append(tabs[2]);
			sBuilder.append(" --incremental lastmodified --last-value '1970-01-01 00:00:00'");
			sBuilder.append(" --merge-key ").append(tabs[1]);
			sBuilder.append(" -m 1");
			break;
		case MySQL:
			sBuilder.append(tabs[0]);
			sBuilder.append(" -- import --connect ").append("jdbc:mysql://").append(dto.getIp()).append(":").append(dto.getPort());
			sBuilder.append("/");
			sBuilder.append(dto.getDbName());
			sBuilder.append(" --username ").append(dto.getUserName());
			sBuilder.append(" --password ").append(dto.getPassword());
			sBuilder.append(" --table ").append(tabs[0]);
        	sBuilder.append(" --target-dir ").append("/user/hive/warehouse/").append(dto.getDbName()).append("/").append(tabs[0]);
			sBuilder.append(" --check-column ").append(tabs[2]);
			sBuilder.append(" --incremental lastmodified --last-value '1970-01-01 00:00:00'");
			sBuilder.append(" --merge-key ").append(tabs[1]);
			break;
		default:
			logger.warn("暂不支持该类型数据库的数据源");
			break;
		}
		return sBuilder.toString();
    }
    
}