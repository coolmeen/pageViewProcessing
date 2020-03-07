package com.example.simmilarweb.pojo;

import com.datastax.driver.mapping.annotations.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "user_stat",keyspace = "beam")
@org.springframework.data.cassandra.core.mapping.Table("user_stat")
public class UserStats {
    @PrimaryKey
    String userId;
    Long uniquesitescount;


    public String toString() {
        return "Num of unique sites for " + this.getUserId() + " = " + this.getUniquesitescount() + " ";
    }
}
