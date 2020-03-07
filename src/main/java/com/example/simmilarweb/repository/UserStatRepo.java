package com.example.simmilarweb.repository;

import com.example.simmilarweb.pojo.UserStats;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface UserStatRepo extends CassandraRepository<UserStats,String> {
}
