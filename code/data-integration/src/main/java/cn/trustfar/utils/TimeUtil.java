package cn.trustfar.utils;

public class TimeUtil {

    public synchronized static String getDMCTimeStamp(){
        String currentTimeMillisStr = String.valueOf(System.currentTimeMillis());
        String str1 = currentTimeMillisStr.substring(0, currentTimeMillisStr.length() - 3);
        String str2 = currentTimeMillisStr.substring(currentTimeMillisStr.length() - 3);
        currentTimeMillisStr = str1 + "." + str2;
        return currentTimeMillisStr;
    }
}
