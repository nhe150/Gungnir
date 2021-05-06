package com.cisco.gungnir.utils;

import com.cisco.gungnir.config.ConfigProvider;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.ConsoleAppender;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.jboss.netty.util.internal.StringUtil;
import scala.collection.immutable.Seq;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class HttpRequest implements Serializable {
    private ConfigProvider configProvider;
    private SparkSession spark;

    public HttpRequest(SparkSession spark, ConfigProvider configProvider) throws Exception {
        this.spark = spark;
        this.configProvider = configProvider;
    }

    private JsonNode getHttpConfig(JsonNode providedConfig) throws Exception {
        JsonNode merged = ConfigProvider.merge(configProvider.getAppConfig().deepCopy(), providedConfig);
        return merged;
    }


    public Dataset readFromHttpRequest(Dataset dataset, String processType, JsonNode providedConfig) throws Exception {
        if (dataset == null)
            throw new IllegalArgumentException("can't send request without body: the input dataset is NULL, please check previous query");

        switch (processType) {
            case "batch":

                JsonNode httpConfig = getHttpConfig(providedConfig);
                //dataset.show();
                //System.out.println("No of records" + dataset.count());
                String server = ConfigProvider.retrieveConfigValue(httpConfig, "http.server");
                String endpoint = ConfigProvider.retrieveConfigValue(httpConfig, "http.endPoint");
                System.out.println("Server :" + server);
                System.out.println("endPoints :" + endpoint);
                String qUrl = server + "/" + endpoint;


                //dataset.show();

                ThreadSafeClientConnManager cm = new ThreadSafeClientConnManager();

                DefaultHttpClient client = new DefaultHttpClient(cm);

                int timeout = 6000; // seconds
                HttpParams httpParams = client.getParams();
                HttpConnectionParams.setConnectionTimeout(
                        httpParams, timeout * 1000); // http.connection.timeout
                HttpConnectionParams.setSoTimeout(
                        httpParams, timeout * 1000); // http.socket.timeout
                client.setHttpRequestRetryHandler(new HttpRequestRetryHandler() {
                    public boolean retryRequest(IOException exception, int executionCount, HttpContext context) {
                        if (executionCount > 50) {
                            System.out.println("Maximum tries reached for client http pool ");
                            try {
                                Thread.sleep(20);
                            } catch (InterruptedException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }
                            return false;
                        }
                        if (exception instanceof org.apache.http.NoHttpResponseException) {
                            System.out.println("No response from server on " + executionCount + " call");
                            try {
                                Thread.sleep(2000);
                            } catch (InterruptedException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }
                            return true;
                        }
                        if (exception instanceof org.apache.http.conn.HttpHostConnectException) {
                            System.out.println("No response1 from server on " + executionCount + " call");
                            try {
                                Thread.sleep(2000);
                            } catch (InterruptedException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }
                            return true;
                        }
                        return false;
                    }
                });
                //############################

                //HttpPost post = new HttpPost("http://dpr1caw008.webex.com:8089/v1/api/todo/botInfo/getBotOwnerInfoBatch");
                //HttpPost post = new HttpPost("https://sjc-gop.webex.com/v1/api/todo/botInfo/getBotOwnerInfoBatch");
                HttpPost post = new HttpPost(qUrl);

                //HttpPost post = new HttpPost(qUrl);
                post.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");

                String json_str = dataset.toJSON().toJavaRDD().collect().toString();
                System.out.println("Data:"+json_str);

                post.setEntity(new StringEntity(json_str));
                HttpResponse response = client.execute(post);
                String entity = IOUtils.toString(response.getEntity().getContent());

                Dataset result = spark.read().json(spark.createDataset(Arrays.asList(entity), Encoders.STRING()) );

                result.show();

                return result;

            case "stream":
                throw new IllegalArgumentException("streaming processs readHttpRequest is not supported yet");
            default:
                throw new IllegalArgumentException("Invalid process type: " + processType + " for readFromHttpRequest");
        }
    }


}
