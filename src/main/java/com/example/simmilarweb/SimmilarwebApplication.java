package com.example.simmilarweb;

import com.example.simmilarweb.pojo.PageView;
import com.example.simmilarweb.pojo.UserStats;
import com.example.simmilarweb.pojo.WebStat;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.*;
import org.apache.beam.sdk.extensions.kryo.KryoCoderProvider;
import org.apache.beam.sdk.extensions.kryo.KryoOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.cassandra.CassandraIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.function.UnaryOperator.identity;

@SpringBootApplication
@Slf4j
public class SimmilarwebApplication {

    public static void main(String[] args) {
        SpringApplication.run(SimmilarwebApplication.class, args);
    }

    public static class CSvParser extends DoFn<String, PageView> {


        @DoFn.ProcessElement
        public void processElement(ProcessContext c) throws IOException {
            String element = c.element();
            String[] split = element.split(",");
            c.output(new PageView(split[0], split[1], split[2], Long.parseLong(split[3])));

        }
    }

}


//            PCollection<KV<String, Long>> wordGrouping = p.apply(TextIO.read().from("gs://apache-beam-samples/shakespeare/winterstale-personae*"))
//                    .apply(FlatMapElements
//                            .into(TypeDescriptors.strings())
//                            .via(line -> Arrays.asList((line.split("[^\\p{L}]+")))))
//                    .apply(Count.perElement())
//
//        /*    PCollection<KV<String, Integer>> length = wordGrouping.
//                    apply(ParDo.of(new WordLength()))*/;
//
//            PCollection<String> count = wordGrouping.apply(MapElements
//                    .into(TypeDescriptors.strings())
//                    .via(wordCount -> wordCount.getKey() + ":" + wordCount.getValue()));
//
//            count.apply(TextIO.write().to("wordcounts"));