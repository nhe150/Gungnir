package com.cisco.gungnir.utils;

import com.cisco.gungnir.config.ConfigProvider;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Arrays;


public class HttpRequest implements Serializable {
    private ConfigProvider configProvider;
    private SparkSession spark;

    public HttpRequest(SparkSession spark, ConfigProvider configProvider) throws Exception {
        this.spark = spark;
        this.configProvider = configProvider;
    }

    public Dataset readFromHttpRequest(Dataset dataset, String processType, JsonNode providedConfig) throws Exception {
        if(dataset==null) throw new IllegalArgumentException("can't send request without body: the input dataset is NULL, please check previous query");

        switch (processType) {
            case "batch":
                dataset.show();
                HttpClient client = new DefaultHttpClient();
                HttpPost post = new HttpPost("http://dpr2caw007.webex.com:8089/v1/api/todo/userInfo/getUserInfoBatch");
                post.addHeader(HttpHeaders.CONTENT_TYPE,"application/json");
                String json_str=dataset.toJSON().toJavaRDD().collect().toString();
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