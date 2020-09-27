package com.roncoo.es.score.first;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;


/**
 * @ClassName EmployeeCRUDAppTest
 * @Description TODO
 * @Author 洛上云雾
 * @Date 2020/9/25 15:48
 * @Version 1.0
 */

@SpringBootTest
public class EmployeeCRUDAppTest {

    final public static String IP = "127.0.0.1";
    final public static Integer PORT = 9300;
    private TransportClient client = null;

    private static Logger logger = LoggerFactory.getLogger(EmployeeCRUDAppTest.class);

    @Before
    public void buildClient(){
        // 先构建client
        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch")
                .build();
        try {
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
            logger.info("构建完成：name:{}, es client","elasticsearch");
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    @After
    public void closeClient(){
        if(client!=null){
            logger.info("关闭es client");
            client.close();
        }
    }

    @Test
    public void execute() throws Exception {
//    createEmployee(client);
//		getEmployee(client);
//		updateEmployee(client);
//		deleteEmployee(client);
//        createDocs(client);
        complexSearch(client);
    }





    /**
     * 创建员工信息（创建一个document）
     * @param client
     */
    private static void createEmployee(TransportClient client) throws Exception {
        IndexResponse response = client.prepareIndex("company", "employee", "1")
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("name", "jack")
                        .field("age", 27)
                        .field("position", "technique")
                        .field("country", "china")
                        .field("join_date", "2017-01-01")
                        .field("salary", 10000)
                        .endObject())
                .get();
        System.out.println(response.getResult());
    }

    /**
     * 获取员工信息
     * @param client
     * @throws Exception
     */
    private static void getEmployee(TransportClient client) throws Exception {
        GetResponse response = client.prepareGet("company", "employee", "1").get();
        System.out.println(response.getSourceAsString());
    }

    /**
     * 修改员工信息
     * @param client
     * @throws Exception
     */
    private static void updateEmployee(TransportClient client) throws Exception {
        UpdateResponse response = client.prepareUpdate("company", "employee", "1")
                .setDoc(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("position", "technique manager")
                        .endObject())
                .get();
        System.out.println(response.getResult());
    }

    /**
     * 删除 员工信息
     * @param client
     * @throws Exception
     */
    private static void deleteEmployee(TransportClient client) throws Exception {
        DeleteResponse response = client.prepareDelete("company", "employee", "1").get();
        System.out.println(response.getResult());
    }

    /**
     * 需求：
     *
     * （1）搜索职位中包含technique的员工
     * （2）同时要求age在20到30岁之间
     * （3）分页查询，查找第一页
     */
    private static void complexSearch(TransportClient client){
        SearchResponse searchResponse = client.prepareSearch("company")
                .setTypes("employee")
                .setQuery(QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("position", "technique")))
                .setPostFilter(QueryBuilders.rangeQuery("age").gte(30).lte(40))
                .setFrom(0).setSize(1).get();
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        Arrays.stream(searchHits).forEach((searchHit)->{logger.info("查询可得：{}",searchHit.getSourceAsString());});
    }

    private static void createDocs(TransportClient client) throws IOException {
        IndexRequestBuilder doc1 = client.prepareIndex("company", "employee", Integer.toString(1))
                .setSource(XContentFactory.jsonBuilder()
                .startObject()
                    .field("name", "jack")
                    .field("age", 27)
                    .field("position", "technique software")
                    .field("country", "china")
                    .field("join_date", "2017-01-01")
                    .field("salary", 10000)
                .endObject());
            if (doc1.get().getResult()!=null){
                logger.info("填充数据-doc{}：successful",doc1.get().getId());
            }
        IndexRequestBuilder doc2 = client.prepareIndex("company", "employee", Integer.toString(2))
                .setSource(XContentFactory.jsonBuilder()
                .startObject()
                    .field("name", "marry")
                    .field("age", 35)
                    .field("position", "technique manager")
                    .field("country", "china")
                    .field("join_date", "2017-01-01")
                    .field("salary", 12000)
                .endObject());
        if (doc1.get().getResult()!=null){
            logger.info("填充数据-doc{}：successful",doc2.get().getId());
        }
        IndexRequestBuilder doc3 = client.prepareIndex("company", "employee", Integer.toString(3))
                .setSource(XContentFactory.jsonBuilder()
                .startObject()
                    .field("name", "tom")
                    .field("age", 32)
                    .field("position", "senior technique software")
                    .field("country", "china")
                    .field("join_date", "2016-01-01")
                    .field("salary", 11000)
                .endObject() );
        if (doc1.get().getResult()!=null){
            logger.info("填充数据-doc{}：successful",doc3.get().getId());
        }
        IndexRequestBuilder doc4 = client.prepareIndex("company", "employee", Integer.toString(4))
                .setSource(XContentFactory.jsonBuilder()
                .startObject()
                    .field("name", "jen")
                    .field("age", 25)
                    .field("position", "junior finance")
                    .field("country", "usa")
                    .field("join_date", "2016-01-01")
                    .field("salary", 7000)
                .endObject());
        if (doc1.get().getResult()!=null){
            logger.info("填充数据-doc{}：successful",doc4.get().getId());
        }
        IndexRequestBuilder doc5 = client.prepareIndex("company", "employee", Integer.toString(5))
                .setSource(XContentFactory.jsonBuilder()
                .startObject()
                    .field("name", "mike")
                    .field("age", 37)
                    .field("position", "finance manager")
                    .field("country", "usa")
                    .field("join_date", "2015-01-01")
                    .field("salary", 15000)
                .endObject());
        if (doc1.get().getResult()!=null){
            logger.info("填充数据-doc{}：successful",doc5.get().getId());
        }

    }

    /**
     *  需求：
     * （1）首先按照country国家来进行分组
     * （2）然后在每个country分组内，再按照入职年限进行分组
     * （3）最后计算每个分组内的平均薪资
     */
    public static void Aggregation(TransportClient client){

    }

}
