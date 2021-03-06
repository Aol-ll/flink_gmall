package com.atguigu.gmall.gmallpublisher.service;

import com.atguigu.gmall.gmallpublisher.bean.VisitorStats;

import java.util.List;

/**
 * Desc: 访客统计业务层接口
 */
public interface VisitorStatsService {

    List<VisitorStats> getVisitorStatsByNewFlag(int date);

    List<VisitorStats> getVisitorStatsByHr(int date);

}
