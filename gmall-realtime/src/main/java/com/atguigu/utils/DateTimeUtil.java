package com.atguigu.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * @author Aol
 * @create 2021-02-27 14:03
 */
public class DateTimeUtil {

    private static DateTimeFormatter  sdf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static String toYMDhms(Date date){
        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        return sdf.format(localDateTime);
    }

    public static Long toTs(String YmDHms) throws ParseException {
        LocalDateTime localDateTime = LocalDateTime.parse(YmDHms,sdf);
        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }
}
