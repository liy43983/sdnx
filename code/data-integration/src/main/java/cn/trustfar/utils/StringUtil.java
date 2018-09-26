package cn.trustfar.utils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * 字符串工具类
 */
public class StringUtil {

    private StringUtil() {
    }

    public static String getFiledValueFromXML(String xml, String field) {
        String head = "<" + field + ">";
        String end = "</" + field + ">";

        int headIndex = xml.indexOf(head);
        int endIndex = xml.indexOf(end);
        if (headIndex < 0 || endIndex < 0) {
            return "";
        }
        return xml.substring(headIndex + head.length(), endIndex);
    }

}
