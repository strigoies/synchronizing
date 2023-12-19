package com.yisa.model;

import com.alibaba.fastjson2.JSONObject;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Relation {

    private String _from;
    private String _to;
    private int relation_type;
    private String relation_name;
    private JSONObject[] details;
    private JSONObject[] sources;
    private String describe;
    private int create_time;
}
