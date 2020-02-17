package com.elasticsearch.example;

import java.util.Date;

import com.elasticsearch.model.Person;
import com.elasticsearch.service.ElasticSearchService;

/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) {

        ElasticSearchService service = new ElasticSearchService();
        Person p = new Person();
        p.setName("nick deutch -  ".concat(new Date().toString()));
        p.setPersonId("");
        service.insertPerson(p);

        System.out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
        System.out.println(service.getPersonById(p.getPersonId()));


        System.out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
        System.out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
        System.out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
        System.out.println(service.getAll());
    }
}
