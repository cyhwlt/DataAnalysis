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

    public  List<HashMap<String, Object>> getResults(String sql) {
//        this.jointSql(sql);
        ArrayList<Map<String, Object>> lists = HiveSpark.sqlCaculate(sql);
        Iterator<Map<String, Object>> iterator = lists.iterator();
        List<HashMap<String, Object>> resultList = new ArrayList<>();
        while(iterator.hasNext()){
            Map<String, Object> map = iterator.next();
            HashMap<String, Object> resultMap = new HashMap();
            Set<String> keyset = map.keySet();
            scala.collection.Iterator<String> iterator1 = keyset.iterator();
            while(iterator1.hasNext()){
                String key = iterator1.next();
                Object value = map.get(key).get();
                resultMap.put(key,value);
//                System.out.println("====================================" + key + ":" + value);
            }
            resultList.add(resultMap);
        }

        return resultList;

    }

    private String jointSql(String sql) {
        String dbName = "zhilian_test";
        String[] froms = sql.split("from");
        String[] from1s = froms[1].split(" "); // from后面的sql通过空格分割
        String[] tables = from1s[0].split(",");
        if(froms[1].toLowerCase().contains("join")){ // 通过join进行表关联的处理

        } else if(tables.length == 1){ // 单表查询
            String newTab = dbName + "." + tables[0];
            sql.replace(tables[0], newTab);
        } else { // 多表关联通过where查询

        }
        return null;
    }
}
