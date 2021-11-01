package com.atarun;

import com.google.gson.GsonBuilder;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Get;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ESClient_01_demo {
    public static void main(String[] args) throws Exception {
        String gsonDateFormat = "yyyy-MM-dd'T'HH:mm:ss";
        List<String> esAddressList = new ArrayList<>();
        esAddressList.add("http://10.10.2.81:9200");
        esAddressList.add("http://10.10.2.82:9200");
        esAddressList.add("http://10.10.2.109:9200");
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig.Builder(esAddressList)
                .defaultCredentials("elastic", "bsoft01")
                .maxTotalConnection(30)
                .defaultMaxTotalConnectionPerRoute(10)
                .multiThreaded(true)
                .connTimeout(60000)
                .readTimeout(60000)
                .discoveryEnabled(true)
                .discoveryFrequency(500L, TimeUnit.MILLISECONDS)
                .gson(new GsonBuilder().setDateFormat(gsonDateFormat).create())
                .build());
        JestClient jestClient = factory.getObject();

        // 简单查询
        Get get = new Get.Builder("procedure", "50276e2f14e54568b6232273a8daa71b").type("-type").build();
        JestResult jestResult = jestClient.execute(get);

        if (jestResult != null && jestResult.isSucceeded()) {
            Map<String, String> map = jestResult.getSourceAsObject(Map.class);
            System.out.println(map.get("bsoft.ServerIP"));
        }
    }
}
