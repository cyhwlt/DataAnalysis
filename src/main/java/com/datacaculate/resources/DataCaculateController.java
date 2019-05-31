package com.datacaculate.resources;

import com.dataanalysis.bean.ResultDto;
import com.datacaculate.bean.SqlQueryDto;
import com.datacaculate.services.DataCaculateService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;

@RestController
@RequestMapping("/datacaculate")
public class DataCaculateController {

    @Autowired
    private DataCaculateService dcService;

    /**
     * 获取计算后的结果(sql)
     *
     * @param sql
     */
//    @GetMapping("/getresults")
//    public List<HashMap<String, Object>> getResults(@RequestParam String sql, @RequestParam String dbName){
//        List results = this.dcService.getResults(sql, dbName);
//        return results;
//    }
    @PostMapping("/getresults")
    public ResultDto getResults(@RequestBody SqlQueryDto dto) {
        List results = this.dcService.getResults(dto.getSql(), dto.getDbName());
        ResultDto resultDto = new ResultDto();
        resultDto.setCode(0);
        resultDto.setData(results);
        return resultDto;
    }
}
