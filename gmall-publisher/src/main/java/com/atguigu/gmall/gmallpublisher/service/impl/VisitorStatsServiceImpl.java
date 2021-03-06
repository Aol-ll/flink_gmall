package com.atguigu.gmall.gmallpublisher.service.impl;


import com.atguigu.gmall.gmallpublisher.bean.VisitorStats;
import com.atguigu.gmall.gmallpublisher.mapper.VisitorStatsMapper;
import com.atguigu.gmall.gmallpublisher.service.VisitorStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Desc: 访客统计接口的实现类
 */
@Service
public class VisitorStatsServiceImpl implements VisitorStatsService {

    @Autowired
    VisitorStatsMapper visitorStatsMapper;
    @Override
    public List<VisitorStats> getVisitorStatsByNewFlag(int date) {
        return visitorStatsMapper.selectVisitorStatsByNewFlag(date);
    }

    @Override
    public List<VisitorStats> getVisitorStatsByHr(int date) {
        return visitorStatsMapper.selectVisitorStatsByHr(date);
    }
}
