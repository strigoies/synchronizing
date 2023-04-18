package com.yisa;

import com.yisa.utils.ConfigEntity;
import com.yisa.utils.ReadConfig;
import org.apache.commons.codec.binary.Base64;
import org.junit.Assert;
import org.junit.Test;

public class ProgramTest {

    @Test
    public void showConfig(){
        // 获取配置文件配置
        ConfigEntity config = ReadConfig.getConfigEntity();
        ConfigEntity.MongoDB mongodb = config.getMongodb();
        ConfigEntity.LightningDB lightningDB = config.getLightningdb();
        System.out.println(lightningDB.getDatabase());
        System.out.println(mongodb.getPassword());
        System.out.println(config.getKafka().getTopic());
        System.out.println(config.getParameter().isSystemRecovery());
    }

    @Test
    public void testStringAndBase64() {

        String s1 = "hello world";

        byte[] bytes1 = s1.getBytes();
        System.out.println("1. byte1数组的内存地址：" + bytes1);

        s1 = new String(bytes1);
        System.out.println("2. 通过new String()将bytes1转回字符串：" + s1);

        String s2 = Base64.encodeBase64String(bytes1);
        System.out.println("3. 将byte1数组转为Base64编码字符串：" + s2);

        byte[] bytes2 = Base64.encodeBase64(bytes1);
        String s3 = new String(bytes2);
        System.out.println("4. 通过new String()将bytes2转回字符串：" + s3);

        // Base64.encodeBase64String(bytes1) 相当于 new String(Base64.encodeBase64(bytes1))
        Assert.assertEquals(s2, s3);

        byte[] bytes3 = Base64.decodeBase64(s3);
        System.out.println("5. 将Base64字符串解码回字节数组bytes3：" + bytes3);
        System.out.println("6. 通过new String()将bytes3转回字符串：" + new String(bytes3));
    }
}
