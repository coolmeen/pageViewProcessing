package com.example.simmilarweb.service.command;

import com.example.simmilarweb.pojo.UserStats;
import com.example.simmilarweb.pojo.WebStat;
import org.apache.beam.sdk.io.cassandra.CassandraIO;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
public class SinkWriter {

    public PDone writeTosink(PCollection<WebStat> medianPerSiteString) {
        return medianPerSiteString.apply(CassandraIO.<WebStat>write()
                .withKeyspace("beam")
                .withPort(9042)
                .withHosts(Arrays.asList("127.0.0.01"))
                .withEntity(WebStat.class));
    }

    public PDone writeToSink(PCollection<UserStats> visitsString) {
        return visitsString.apply(CassandraIO.<UserStats>write()
                .withKeyspace("beam")
                .withPort(9042)
                .withHosts(Arrays.asList("127.0.0.01"))
                .withEntity(UserStats.class));
    }

}
