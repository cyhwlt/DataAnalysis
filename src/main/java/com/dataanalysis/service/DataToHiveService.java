package com.dataanalysis.service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
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
import com.dataanalysis.bean.database.TableColInfoDto;
import com.dataanalysis.bean.database.TableViewDto;
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

    /**
     * 关系型数据库导入hdfs
     *
     * @param dto
     */
    public void dataToHdfs(DatabaseDto dto) {
        String jobPath = property.getProperty("datajob");
        jobPath = jobPath + dto.getDbName() + "_" + dto.getIp(); // linux下存放每个数据库所有表的文件夹
        // 创建脚本文件
        RemoteShellUtil rs = new RemoteShellUtil(remoteIp, remoteUser, remotePassword, "utf-8");
        rs.exec("mkdir " + jobPath, true); // 创建库文件夹
        String writeJob = jobPath + "/" + dto.getDbName() + ".sh";
        logger.info(">>>>>>>>>>>>" + writeJob);
        rs.exec("touch " + writeJob, true); // 创建生成job的脚本文件
        logger.info(">>>>>>>touchFile成功");
        List<String> tables = new ArrayList<>();
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
                if (fvDto.getIsPK()) {
                    sb.append(fvDto.getColName()).append(",");
                    break;
                }
            }
            for (FieldViewDto fvDto : fieldArr) {
                String typeName = fvDto.getTypeName();
                if (typeName.toUpperCase().equals("DATETIME")) {
                    sb.append(fvDto.getColName());
                    break;
                }
            }
            tabInfos.add(sb.toString());
        }

        // 写入sqoop命令行
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
            rs.exec("echo \t\"sqoop job --exec " + tables.get(i) + " --meta-connect jdbc:hsqldb:hsql://172.16.101.33:16000/sqoop" + "\" >> " + execJob, true); // 将执行job的命令行写入脚本
        }
        rs.exec("chmod 777 " + execJob, true);
        rs.exec("sh " + execJob, false);
        logger.info(">>>>>>>execJob成功");

    }

    /**
     * 将数据导入hive表
     *
     * @param dto
     */
    public void loadDataToHive(LoadDataDto dto) {
        // 连接hive
        Connection conn = DBConnectUtil.getConnection(dto.getHiveDb());
        Statement cs = null;
        try {
            cs = conn.createStatement();
            String partitionTime = sdf.format(new Date());

            List<String> tables = new ArrayList<>();
            // 获取所有表
            List<TableViewDto> tables2 = this.dbcService.getTables(dto.getOriginDb());
            for (TableViewDto tvDto : tables2) {
                tables.add(tvDto.getTableName());
            }

            cs.execute("use " + dto.getOriginDb().getDbName());
            for (String table : tables) {
                String path = "/user/hive/warehouse/" + dto.getOriginDb().getDbName() + "/" + table + "/*";
                cs.execute("load data inpath '" + path + "' into table " + table + " partition(time_partition='"
                        + partitionTime + "')");

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
     *
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
     *
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
            logger.error("创建hive数据库错误信息:" + e.getMessage());
        } finally {
            try {
                cs.close();
                conn.close();
            } catch (SQLException e) {
                logger.error("创建hive数据库错误信息:" + e.getMessage());
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
        jobPath = jobPath + dto.getDbName() + "_" + dto.getIp(); // linux下存放每个数据库所有表的文件夹
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

    /**
     * 数据清洗
     *
     * @param dto
     */
    public void dataClean(LoadDataDto dto) {

        List<TableColInfoDto> tcDtos = this.dbcService.getColInfo(dto.getOriginDb());
        for (TableColInfoDto tcDto : tcDtos) {
            // 连接hive
            Connection conn = DBConnectUtil.getConnection(dto.getHiveDb());
            Statement cs = null;
            try {
                cs = conn.createStatement();
                cs.execute("use " + dto.getOriginDb().getDbName());
                String colNameStr = tcDto.getColNameStr(); // 所有字段拼接字符串
                String colNameRemoveDateStr = ""; // 去除日期字段拼接字符串
                int index = colNameStr.indexOf(tcDto.getDateColName());
                String dateStr = colNameStr.substring(index, index + tcDto.getDateColName().length() + 1);
                if (dateStr.contains(",")) {
                    colNameRemoveDateStr = colNameStr.replace(dateStr, "");
                } else {
                    colNameRemoveDateStr = colNameStr.replace(tcDto.getDateColName(), "");
                }

                StringBuilder whereStr = new StringBuilder();
                for (Map<String, String> m : tcDto.getFilterColName()) {
                    for (String k : m.keySet()) {
                        whereStr.append("t.");
                        whereStr.append(m.get(k));
                        whereStr.append(" IS NOT NULL");
                        whereStr.append(" and");
                    }
                }
                String partitionTime = sdf.format(new Date());
                StringBuilder sBuilder = new StringBuilder();
                sBuilder.append("insert overwrite table " + tcDto.getTableName()).append(" partition (time_partition='" + partitionTime + "')");
                sBuilder.append(" select " + colNameStr);
                sBuilder.append(" from (select ").append(colNameRemoveDateStr).append(",").append("date_format(")
                        .append(tcDto.getDateColName()).append(",'yyyy-MM-dd HH:mm:ss')").append(tcDto.getDateColName())
                        .append(",row_number() over(partition by " + tcDto.getPkColName() + ") num from "
                                + tcDto.getTableName() + ") t");
                sBuilder.append(" where t.num=1 and ");
                sBuilder.append(whereStr.toString().substring(0, whereStr.toString().length() - 3));
                cs.execute(sBuilder.toString());
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
    }

    /**
     * 将关系型数据库导入hive
     *
     * @param dto
     */
    public void importToHive(DatabaseDto dto) {
        // 1、将数据导入hdfs
        this.dataToHdfs(dto);
        // 2、创建hive分区表
        this.createHiveTable(dto);
        DatabaseDto hiveDto = DBConnectUtil.hiveDtoJoint(dto);
        LoadDataDto loadDataDto = new LoadDataDto();
        loadDataDto.setHiveDb(hiveDto);
        loadDataDto.setOriginDb(dto);
        // 3、将数据load到hive数据库
        this.loadDataToHive(loadDataDto);
        // 4、数据简单清洗
        this.dataClean(loadDataDto);
    }
}
