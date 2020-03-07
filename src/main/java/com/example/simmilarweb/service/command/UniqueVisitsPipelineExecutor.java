package com.example.simmilarweb.service.command;

import com.example.simmilarweb.common.PipelineCommon;
import com.example.simmilarweb.pojo.PageView;
import com.example.simmilarweb.pojo.UserStats;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.CountByKey;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.MapElements;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.springframework.stereotype.Service;

@Service
public class UniqueVisitsPipelineExecutor {


    public PCollection<UserStats> processUniqueVisitsPipeline(PCollection<PageView> rawEvents) {
        return PipelineCommon.of(rawEvents)
                .map(this::groupByUserAndBaseUrl)
                .map(this::findAllDistinctVisits)
                .letAndReturn(this::mapToUserStat);

    }

    private PCollection<KV<String, Long>> findAllDistinctVisits(PCollection<KV<String, Iterable<PageView>>> grouped) {

        return CountByKey.named("distinct").of(grouped)
                .keyBy(key -> key.getValue().iterator().next().getUserId())
                .output();

    }

    private PCollection<KV<String, Iterable<PageView>>> groupByUserAndBaseUrl(PCollection<PageView> rawEvents) {
        return MapElements.named("group By user id and base url")
                .of(rawEvents)
                .using(event -> KV.of(event.getUserId() + ";" + event.getBaseUrl(), event), TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptor.of(PageView.class)))
                .output()
                .apply(GroupByKey.create());
    }


    private PCollection<UserStats> mapToUserStat(PCollection<KV<String, Long>> visits) {
        return MapElements
                .named("map2")
                .of(visits)
                .using(kv -> new UserStats(kv.getKey(), kv.getValue()))
                .output();
    }


}