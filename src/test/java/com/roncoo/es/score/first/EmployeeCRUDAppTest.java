package com.roncoo.es.score.first;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;

import java.net.InetAddress;
import java.net.UnknownHostException;

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

    private Logger logger = LoggerFactory.getLogger(EmployeeCRUDAppTest.class);

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
		getEmployee(client);
//		updateEmployee(client);
//		deleteEmployee(client);
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

}
