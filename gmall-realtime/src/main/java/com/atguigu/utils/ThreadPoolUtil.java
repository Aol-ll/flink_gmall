package com.atguigu.utils;

import org.apache.flink.streaming.runtime.io.BlockingQueueBroker;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author Aol
 * @create 2021-02-27 11:54
 */
public class ThreadPoolUtil {
    public static ThreadPoolExecutor pool;

    private ThreadPoolUtil(){}

    public static ThreadPoolExecutor getInstance(){

        if (pool == null) {
            synchronized (ThreadPoolUtil.class) {
                if (pool == null) {
                    pool = new ThreadPoolExecutor(
                            4,
                            20,
                            300L,
                            TimeUnit.SECONDS,
                            new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE));
                }
            }
        }

        return pool;
    }
}
