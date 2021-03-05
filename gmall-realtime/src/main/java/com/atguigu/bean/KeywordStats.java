package com.atguigu.bean;

/**
 * @author Aol
 * @create 2021-03-05 18:51
 */
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
/**
 * Desc: 关键词统计实体类
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class KeywordStats {
    private String word;
    private Long ct;
    private String source;
    private String stt;
    private String edt;
    private Long ts;
}
