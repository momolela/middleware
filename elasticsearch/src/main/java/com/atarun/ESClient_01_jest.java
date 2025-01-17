package com.atarun;

import com.google.gson.GsonBuilder;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Get;
import io.searchbox.core.Ping;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ESClient_01_jest {

    private static JestClient jestClient;

    static {
        String gsonDateFormat = "yyyy-MM-dd'T'HH:mm:ss";
        List<String> esAddressList = new ArrayList<>();
        esAddressList.add("http://172.31.1.176:9200");
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig.Builder(esAddressList)
                .defaultCredentials("elastic", "bsoft01")
                .maxTotalConnection(1)
                .defaultMaxTotalConnectionPerRoute(1)
                .connTimeout(10)
                .readTimeout(10)
                .discoveryEnabled(true)
                .discoveryFrequency(500L, TimeUnit.MILLISECONDS)
                .multiThreaded(true)
                .gson(new GsonBuilder().setDateFormat(gsonDateFormat).create())
                .build());
        jestClient = factory.getObject();
    }

    public static void main(String[] args) throws Exception {

        for (int i = 0; i < 1000; i++) {
            new Thread(() -> {
                try {
                    System.out.println(jestClient.execute(new Ping.Builder().build()).isSucceeded());
                    // Thread.sleep(100);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("ping es error");
                }
            }).start();
        }

        // new Thread(() -> {
        //     while (true) {
        //         try {
        //             System.out.println(jestClient.execute(new Ping.Builder().build()).isSucceeded());
        //             Thread.sleep(100);
        //         } catch (IOException | InterruptedException e) {
        //             e.printStackTrace();
        //             System.out.println("ping es error");
        //         }
        //     }
        // }).start();
        //
        // new Thread(() -> {
        //     while (true) {
        //         try {
        //             System.out.println(jestClient.execute(new Ping.Builder().build()).isSucceeded());
        //             Thread.sleep(100);
        //         } catch (IOException | InterruptedException e) {
        //             e.printStackTrace();
        //             System.out.println("ping es error");
        //         }
        //     }
        // }).start();

        // new Thread(() -> {
        //     List<Object> list = new ArrayList<>();
        //
        //     while (true) {
        //         try {
        //             // 你可以通过休眠来减缓添加速度，便于观察
        //             Thread.sleep(2000);
        //             // 不断地向列表中添加新的对象
        //             list.add(new byte[1024 * 1024]); // 创建一个1MB大小的byte数组
        //             System.out.println("Added 1MB of data to the list.");
        //         } catch (OutOfMemoryError | InterruptedException e) {
        //             // 捕获OutOfMemoryError并输出错误信息
        //             System.err.println("Error: " + e.getMessage());
        //             // e.printStackTrace();
        //             // break; // 退出循环
        //         }
        //         try {
        //             System.out.println(jestClient.execute(new Ping.Builder().build()).isSucceeded());
        //         } catch (Exception e) {
        //             e.printStackTrace();
        //             System.out.println("ping es error");
        //         }
        //     }
        // }).start();


        // String gsonDateFormat = "yyyy-MM-dd'T'HH:mm:ss";
        // List<String> esAddressList = new ArrayList<>();
        // esAddressList.add("http://172.31.1.189:9200");
        // JestClientFactory factory = new JestClientFactory();
        // factory.setHttpClientConfig(new HttpClientConfig.Builder(esAddressList)
        //         .defaultCredentials("elastic", "bsoft01")
        //         .maxTotalConnection(30)
        //         .defaultMaxTotalConnectionPerRoute(10)
        //         .multiThreaded(true)
        //         .connTimeout(60000)
        //         .readTimeout(60000)
        //         .discoveryEnabled(true)
        //         .discoveryFrequency(500L, TimeUnit.MILLISECONDS)
        //         .gson(new GsonBuilder().setDateFormat(gsonDateFormat).create())
        //         .build());
        // JestClient jestClient = factory.getObject();

        // 简单查询
        // Get get = new Get.Builder("procedure", "50276e2f14e54568b6232273a8daa71b").type("-type").build();
        // JestResult jestResult = jestClient.execute(get);
        //
        // if (jestResult != null && jestResult.isSucceeded()) {
        //     Map<String, String> map = jestResult.getSourceAsObject(Map.class);
        //     System.out.println(map.get("bsoft.ServerIP"));
        // }
    }
}
