package com.datacaculate.services;


import com.datacaculate.HiveSpark;
import org.springframework.stereotype.Service;
import scala.collection.immutable.Map;
import scala.collection.immutable.Set;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

@Service
public class DataCaculateService {

    public List<HashMap<String, Object>> getResults(String sql, String dbName) {
        sql = this.jointSql(sql.toLowerCase(), dbName);
        ArrayList<Map<String, Object>> lists = HiveSpark.sqlCaculate(sql);
        Iterator<Map<String, Object>> iterator = lists.iterator();
        List<HashMap<String, Object>> resultList = new ArrayList<>();
        while (iterator.hasNext()) {
            Map<String, Object> map = iterator.next();
            HashMap<String, Object> resultMap = new HashMap();
            Set<String> keyset = map.keySet();
            scala.collection.Iterator<String> iterator1 = keyset.iterator();
            while (iterator1.hasNext()) {
                String key = iterator1.next();
                Object value = map.get(key).get();
                resultMap.put(key, value);
            }
            resultList.add(resultMap);
        }
        return resultList;
    }

    /**
     * 拼接sql
     *
     * @param sql
     * @param dbName
     * @return
     */
    private String jointSql(String sql, String dbName) {
        // 只替换from后的表名
        String[] fromLatters = sql.split(" from ");
        String fromLatter1 = fromLatters[1]; // from后的sql
        StringBuilder sql1 = new StringBuilder();
        if (fromLatter1.toLowerCase().contains(" join ")) {
            // 含有join的多表关联查询
            String[] joins = fromLatter1.split(" join ");
            for (int i = 0; i < joins.length; i++) {
                if (i == 0) {
                    sql1.append(dbName).append(".").append(joins[i].trim());
                } else {
                    String join = joins[i];
                    if (join.contains(" on ")) {
                        String[] ons = join.split(" on ");
                        sql1.append(" join ").append(dbName).append(".").append(ons[0].trim()).append(" on ").append(ons[1]);
                    } else {
                        sql1.append(" join ").append(dbName).append(".").append(joins[i].trim());
                    }
                }
            }
            sql = fromLatters[0] + " from " + sql1;
        } else {
            if (fromLatter1.contains(" in ")) {
                String[] ins = fromLatter1.split(" in ");
                sql = dealsql(ins[0], fromLatters, dbName, sql);
            } else {
                sql = dealsql(fromLatter1, fromLatters, dbName, sql);
            }
        }
        return sql;
    }

    /**
     * where多表关联公共代码提取
     *
     * @param in
     * @param froms
     * @param dbName
     * @param sql
     * @return
     */
    private String dealsql(String in, String[] froms, String dbName, String sql) {
        if (!in.contains(",")) {
            // 只有一个表的查询
            if (froms[1].contains(" where ")) {
                String[] wheres = froms[1].split(" where ");
                String table = wheres[0];
                sql = froms[0] + " from " + dbName + "." + table.trim() + " where " + wheres[1];
            } else if (froms[1].contains("group by")) {
                String[] groupbys = froms[1].split("group by");
                String table = groupbys[0];
                sql = froms[0] + " from " + dbName + "." + table.trim() + " group by " + groupbys[1];
            } else if (froms[1].contains("order by")) {
                String[] orderbys = froms[1].split("order by");
                String table = orderbys[0];
                sql = froms[0] + " from " + dbName + "." + table.trim() + " order by " + orderbys[1];
            }
        } else {
            // 通过where的多表关联查询
            String[] wheres = froms[1].split(" where ");
            String tableStr = wheres[0];
            String[] tables = tableStr.split(",");
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i < tables.length; i++) {
                String table = dbName + "." + tables[i].trim();
                if (i == tables.length - 1) {
                    stringBuilder.append(table);
                } else {
                    stringBuilder.append(table).append(",");
                }
            }
            sql = froms[0] + " from " + stringBuilder.toString() + " where " + wheres[1];
        }
        return sql;
    }
}
