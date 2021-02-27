package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

/**
 * @author Aol
 * @create 2021-02-27 11:54
 */
public interface DimJoinFunction<T> {

    String getKey(T input);
    void join(JSONObject dimInfo,T input) throws Exception;
}
