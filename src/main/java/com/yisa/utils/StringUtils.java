package com.yisa.utils;

import java.util.ArrayList;

public class StringUtils {
    /**
     * 生成指定个数的问号，用于拼接到sql语句里
     * @param num
     * @return
     */
    public static String generateMark(int num) {
        ArrayList<String> list = new ArrayList<>(num);
        for (int i = 0; i < num; i++) {
            list.add("?");
        }
        return String.join(",",list);
    }
}
