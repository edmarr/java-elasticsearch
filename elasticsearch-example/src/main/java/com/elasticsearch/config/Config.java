package com.elasticsearch.config;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class Config {
    private static final String HOST = "localhost";
    private static final int PORT_ONE = 9200;
    private static final int PORT_TWO = 9201;
    private static final String SCHEME = "http";

    private static RestHighLevelClient restHighLevelClient;


    /**
     * Implemented Singleton pattern here so that there is just one connection at a
     * time.
     * 
     * @return RestHighLevelClient
     */
    public static synchronized RestHighLevelClient makeConnection() {

        if (restHighLevelClient == null) {
            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(HOST, PORT_ONE, SCHEME), new HttpHost(HOST, PORT_TWO, SCHEME)));
        }

        return restHighLevelClient;
    }

    public static synchronized void closeConnection() {
        try {
            restHighLevelClient.close();
            restHighLevelClient = null;
        } catch (IOException e) {
            throw new RuntimeException(e);

        }
        
    }
    

}