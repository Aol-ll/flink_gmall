package com.atguigu.app.func;

import com.atguigu.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * @author Aol
 * @create 2021-03-05 18:26
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeyWordUDTF extends TableFunction<Row> {
    public void eval(String str) {
        List<String> analyze = KeywordUtil.analyze(str);
        for (String word : analyze) {

            collect(Row.of(word));
        }
    }
}
