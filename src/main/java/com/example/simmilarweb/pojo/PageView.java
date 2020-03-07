package com.example.simmilarweb.pojo;

import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.*;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.Serializable;
@DefaultCoder(AvroCoder.class)

@JsonPropertyOrder({ "userId", "baseUrl", "fullUrl", "timeStamp" })
@AllArgsConstructor
@NoArgsConstructor
@Setter
@Getter
@ToString
@EqualsAndHashCode
@Table(name = "PageView",keyspace = "beam")
public class PageView  implements Serializable {
    @PartitionKey
    String userId;
    String baseUrl;
    String fullUrl;
    Long timeStamp;

}
