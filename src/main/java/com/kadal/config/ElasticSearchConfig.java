package com.kadal.config;

import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.RestClients;

public class ElasticSearchConfig {

    @Bean
    RestHighLevelClient elasticsearchClient() {
        final ClientConfiguration clientConfiguration =
                ClientConfiguration.builder().connectedTo("localhost:9200").build();
        System.out.println("elasticsearch Client Success");
        return RestClients.create(clientConfiguration).rest();
    }


}
