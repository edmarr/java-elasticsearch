package com.elasticsearch.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import com.elasticsearch.config.Config;
import com.elasticsearch.model.Person;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

public class ElasticSearchService {

    private static final String INDEX = "persondata";
    private static final String TYPE = "person";

    public Person insertPerson(Person person) {
        person.setPersonId(UUID.randomUUID().toString());
        Map<String, Object> dataMap = new HashMap<String, Object>();
        dataMap.put("personId", person.getPersonId());
        dataMap.put("name", person.getName());
        IndexRequest indexRequest = new IndexRequest(INDEX, TYPE, person.getPersonId()).source(dataMap);
        try {
            Config.makeConnection().index(indexRequest);
        } catch (ElasticsearchException e) {
            e.getDetailedMessage();
        } catch (java.io.IOException ex) {
            ex.getLocalizedMessage();
        } finally {
            Config.closeConnection();
        }

        return person;
    }

    public Person getPersonById(String id) {
        GetRequest getPersonRequest = new GetRequest(INDEX, TYPE, id);
        GetResponse getResponse = null;
        try {
            getResponse = Config.makeConnection().get(getPersonRequest);
        } catch (java.io.IOException e) {
            throw new RuntimeException(e);
        } finally {
            Config.closeConnection();
        }
        return getResponse != null ? new ObjectMapper().convertValue(getResponse.getSourceAsMap(), Person.class) : null;
    }

    public List<Person> getAll() {

        ObjectMapper mapper = new ObjectMapper();
        List<String> list = selectMatchAll(INDEX, TYPE, null, null);
        return list.stream().map(p -> {
            try {
                return mapper.readValue(p, Person.class);
            
            } catch (IOException e) {
               throw new RuntimeException(e);
            }
        }
            ).collect(Collectors.toList());

    }

    private List<String> selectMatchAll(String indexs, String types, String field, String value) {
        try {
            SearchSourceBuilder search = new SearchSourceBuilder();

            if (!StringUtils.isEmpty(field) && !StringUtils.isEmpty(value)) {
                search.query(QueryBuilders.matchQuery(field, value));
            }

            search.aggregation(AggregationBuilders.terms("data").field(field + ".keyword"));
            search.explain(false);
            SearchRequest request = new SearchRequest();
            request.searchType(SearchType.DFS_QUERY_THEN_FETCH);
            request.source(search);
            request.indices(indexs.split(","));
            request.types(types.split(","));

            SearchResponse response = Config.makeConnection().search(request);
            List<String> list = new ArrayList<>();
            
            for(SearchHit obj: response.getHits().getHits()){
                list.add(obj.getSourceAsString());
            }
           return list;
        } catch (Exception e) {
           throw new RuntimeException(e);
        }finally{
            Config.closeConnection();
        }
    }

}