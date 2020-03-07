package com.example.simmilarweb.repository;

import com.example.simmilarweb.pojo.WebStat;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface WebStatRepo extends CassandraRepository<WebStat,String> {
}
