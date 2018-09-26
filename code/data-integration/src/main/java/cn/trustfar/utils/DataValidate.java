package cn.trustfar.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 数据类型验证
 */
public class DataValidate {

    private static Pattern integerPattern = Pattern.compile("^-?\\d+$");

    private static Pattern floatPattern = Pattern.compile("^(-?\\d+)(\\.\\d+)?$");

    /***
     * 判断 String 是否是 整型 通过正则表达式判断
     * @param input
     * @return
     */
    public static boolean isInteger(String input) {

        Matcher mer = integerPattern.matcher(input);
        return mer.find();
    }

    /**
     * 判断 String 是否是 浮点型 通过正则表达式判断
     *
     * @param input
     * @return
     */
    public static boolean isDouble(String input) {
        Matcher mer = floatPattern.matcher(input);
        return mer.find();
    }

    /**
     * 检测字符串是否为 number 类型的数字
     *
     * @param input
     * @return
     */
    public static boolean isNumeric(String input) {
        return isDouble(input);
    }

    public static void main(String[] args) {
        System.out.println(isInteger("564564564"));
        System.out.println(isDouble(null));
        System.out.println(Double.valueOf("564564564.00"));
    }

}
