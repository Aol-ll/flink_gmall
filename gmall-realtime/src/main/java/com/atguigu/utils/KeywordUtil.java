package com.atguigu.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Aol
 * @create 2021-03-05 18:17
 */
public class KeywordUtil {
    public static List<String> analyze (String keyWord) {

        ArrayList<String> list = new ArrayList<>();

        StringReader reader = new StringReader(keyWord);

        IKSegmenter ikSegmenter = new IKSegmenter(reader, true);

        Lexeme next = null;

        while (true) {
            try {
                if ((next = ikSegmenter.next()) != null) {
                    String lexemeText = next.getLexemeText();
                    list.add(lexemeText);
                } else {
                    break;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return list;
    }
    public static void main(String[] args) {
        String text = "Apple iPhoneXSMax (A2104) 256GB 深空灰色 移动联通电信4G手机 双卡双待";
        System.out.println(KeywordUtil.analyze(text));
    }

}
