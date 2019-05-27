package com.datacaculate.resources;

import com.datacaculate.services.DataCaculateService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;

@RestController
@RequestMapping("/datacaculate")
public class DataCaculateController {

    @Autowired
    private DataCaculateService dcService;

    /**
     * 获取计算后的结果(sql)
     * @param sql
     */
    @GetMapping("/getresults")
    public List<HashMap<String, Object>> getResults(@RequestParam String sql){
        List results = this.dcService.getResults(sql);
        return results;
    }
}
