package com.atguigu.gmall.gmallpublisher.service.impl;


import com.atguigu.gmall.gmallpublisher.bean.KeywordStats;
import com.atguigu.gmall.gmallpublisher.mapper.KeywordStatsMapper;
import com.atguigu.gmall.gmallpublisher.service.KeywordStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Desc:关键词统计接口实现类
 */
@Service
public class KeywordStatsServiceImpl implements KeywordStatsService {

    @Autowired
    KeywordStatsMapper keywordStatsMapper;

    @Override
    public List<KeywordStats> getKeywordStats(int date, int limit) {
        return keywordStatsMapper.selectKeywordStats(date,limit);
    }
}
