package com.atarun;

import com.alibaba.fastjson.JSON;
import com.atarun.entity.Hotel;
import com.atarun.entity.HotelDoc;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.atarun.constant.HotelConstant.MAPPING_TEMPLATE;

/**
 * 版权：版权所有 bsoft 保留所有权力。
 *
 * @author <a href="mailto:sunzj@bsoft.com.cn">sunzj</a>
 * @description
 * @date 2024/3/28 23:00
 */
public class ESClient_02_rest {
    private RestHighLevelClient client;

    @Test
    public void testInit() {
        System.out.println(client);
    }

    /**
     * 创建索引
     *
     * @throws IOException
     */
    @Test
    public void createIndex() throws IOException {
        // 1.创建 request 对象
        CreateIndexRequest request = new CreateIndexRequest("hotel");
        // 2.准备请求的参数：DSL 语句
        request.source(MAPPING_TEMPLATE, XContentType.JSON);
        // 3.发送请求
        client.indices().create(request, RequestOptions.DEFAULT);
    }

    /**
     * 删除索引
     *
     * @throws IOException
     */
    @Test
    public void deleteIndex() throws IOException {
        // 1.创建 request 对象
        DeleteIndexRequest request = new DeleteIndexRequest("hotel");
        // 2.发送请求
        client.indices().delete(request, RequestOptions.DEFAULT);
    }

    /**
     * 判断索引是否存在
     *
     * @throws IOException
     */
    @Test
    public void existIndex() throws IOException {
        // 1.创建 request 对象
        GetIndexRequest request = new GetIndexRequest("hotel");
        // 2.发送请求
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists ? "存在索引" : "不存在索引");
    }

    /**
     * 新增文档
     *
     * @throws IOException
     */
    @Test
    public void addDocument() throws IOException {
        // 1.假设从数据库查询出来 hotel 对象
        Hotel hotel = new Hotel();
        hotel.setId(1);
        hotel.setAddress("江西省吉安市泰和县");
        hotel.setBrand("亚朵");
        hotel.setName("龙信大酒店");
        hotel.setBusiness("泰和高新开发区");
        hotel.setCity("江西");
        hotel.setPrice(10000);
        hotel.setScore(95);
        hotel.setPic("https://www.baidu.com");
        hotel.setLatitude("30.890867");
        hotel.setLongitude("121.937241");
        hotel.setStarName("四钻");
        // 2.将 hotel 转换成 es 需要的 hotelDoc 对象
        HotelDoc hotelDoc = new HotelDoc(hotel);
        // 3.对象转 json
        String jsonString = JSON.toJSONString(hotelDoc);
        // 4.创建 request 对象
        IndexRequest request = new IndexRequest("hotel").id(String.valueOf(hotel.getId()));
        // 5.准备对象数据的 json
        request.source(jsonString, XContentType.JSON);
        // 6.发送请求
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        System.out.println(response);
    }

    /**
     * 查询文档
     *
     * @throws IOException
     */
    @Test
    public void getDocument() throws IOException {
        // 1.创建 request 对象
        GetRequest request = new GetRequest("hotel", "1");
        // 2.发送请求
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        // 3.获取 source 中的 json 返回
        String jsonStr = response.getSourceAsString();
        // 4.json 转换对象
        HotelDoc hotelDoc = JSON.parseObject(jsonStr, HotelDoc.class);
        System.out.println(hotelDoc);
    }

    /**
     * 更新文档
     *
     * @throws IOException
     */
    @Test
    public void updateDocument() throws IOException {
        // 1.创建 request 对象
        UpdateRequest request = new UpdateRequest("hotel", "1");
        // 2.准备请求参数
        request.doc(
                "price", "9999",
                "startName", "五钻"
        );
        // 3.发送请求
        client.update(request, RequestOptions.DEFAULT);
    }

    /**
     * 删除文档
     *
     * @throws IOException
     */
    @Test
    public void deleteDocument() throws IOException {
        // 1.创建 request 对象
        DeleteRequest request = new DeleteRequest("hotel", "1");
        // 2.发送请求
        client.delete(request, RequestOptions.DEFAULT);
    }

    /**
     * 批量插入文档
     *
     * @throws IOException
     */
    @Test
    public void bulkAddDocument() throws IOException {
        List<Hotel> hotelList = new ArrayList<>(2);
        Hotel hotel1 = new Hotel();
        hotel1.setId(123);
        hotel1.setAddress("江西省吉安市泰和县");
        hotel1.setBrand("亚朵");
        hotel1.setName("龙信大酒店");
        hotel1.setBusiness("泰和高新开发区");
        hotel1.setCity("江西");
        hotel1.setPrice(10000);
        hotel1.setScore(95);
        hotel1.setPic("https://www.baidu.com");
        hotel1.setLatitude("30.890867");
        hotel1.setLongitude("121.937241");
        hotel1.setStarName("四钻");
        hotelList.add(hotel1);
        Hotel hotel2 = new Hotel();
        hotel2.setId(456);
        hotel2.setAddress("江西省南昌市青云谱县");
        hotel2.setBrand("全季");
        hotel2.setName("全季酒店");
        hotel2.setBusiness("南昌高新开发区");
        hotel2.setCity("江西南昌");
        hotel2.setPrice(20000);
        hotel2.setScore(96);
        hotel2.setPic("https://www.baidu.com123");
        hotel2.setLatitude("30.890867");
        hotel2.setLongitude("121.937241");
        hotel2.setStarName("五钻");
        hotelList.add(hotel2);
        // 1.创建 request 对象
        BulkRequest request = new BulkRequest();
        for (Hotel hotel : hotelList) {
            HotelDoc hotelDoc = new HotelDoc(hotel);
            request.add(new IndexRequest("hotel").id(String.valueOf(hotelDoc.getId())).source(JSON.toJSONString(hotelDoc), XContentType.JSON));
        }
        // 2.发送请求
        client.bulk(request, RequestOptions.DEFAULT);
    }

    // @BeforeEach
    // private void setup() {
    //     this.client = new RestHighLevelClient(RestClient.builder(
    //             HttpHost.create("http://10.10.2.81:9200"),
    //             HttpHost.create("http://10.10.2.82:9200"),
    //             HttpHost.create("http://10.10.2.109:9200")
    //     ));
    // }

    @BeforeEach
    private void setup() {
        ArrayList<String> esAddressList = new ArrayList<>(3);
        esAddressList.add("http://10.10.2.81:9200");
        esAddressList.add("http://10.10.2.82:9200");
        esAddressList.add("http://10.10.2.109:9200");
        // 创建 HttpHost
        HttpHost[] httpHosts = esAddressList.stream().map(HttpHost::create).toArray(HttpHost[]::new);
        // 创建 RestClientBuilder
        RestClientBuilder builder = RestClient.builder(httpHosts);
        // 开始设置用户名和密码
        String auth = Base64.encodeBase64String(("elastic:bsoft01").getBytes());
        builder.setDefaultHeaders(new BasicHeader[]{new BasicHeader("Authorization", "Basic " + auth)});

        // CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        // credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(esusername, espassword));
        // builder.setHttpClientConfigCallback(f -> f.setDefaultCredentialsProvider(credentialsProvider));

        // 异步连接延时配置
        builder.setRequestConfigCallback(requestConfigBuilder -> {
            requestConfigBuilder.setConnectTimeout(5000);
            requestConfigBuilder.setConnectionRequestTimeout(60000);
            return requestConfigBuilder;
        });

        // 异步连接数配置
        builder.setHttpClientConfigCallback(httpClientBuilder -> {
            httpClientBuilder.setMaxConnTotal(30);
            httpClientBuilder.setMaxConnPerRoute(10);
            return httpClientBuilder;
        });

        // 创建 RestHighLevelClient
        this.client = new RestHighLevelClient(builder);
    }

    @AfterEach
    private void teardown() throws IOException {
        this.client.close();
    }
}
