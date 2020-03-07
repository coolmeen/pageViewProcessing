package com.example.simmilarweb.pojo;

import com.datastax.driver.mapping.annotations.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "web_stat",keyspace = "beam")
@org.springframework.data.cassandra.core.mapping.Table("web_stat")
public class WebStat {
    @PrimaryKey
    String baseUrl;
    Double medianSessionLength;
    Long count;


    public String toString() {
        return "Num sessions for site " + this.getBaseUrl() + " = " + this.getCount() + " median session length = " +  this.medianSessionLength + ")";
    }
}
