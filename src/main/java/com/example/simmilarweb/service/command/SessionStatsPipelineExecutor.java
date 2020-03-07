package com.example.simmilarweb.service.command;

import com.example.simmilarweb.common.PipelineCommon;
import com.example.simmilarweb.pojo.PageView;
import com.example.simmilarweb.pojo.WebStat;
import io.vavr.Tuple2;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.UnaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.AssignEventTime;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.CountByKey;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.MapElements;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.ReduceByKey;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Service
public class SessionStatsPipelineExecutor {


    public PCollection<WebStat> processSessionStatsPipeline(PCollection<PageView> rawEvents) {
        Tuple2<PCollection<KV<String, Double>>, PCollection<KV<String, Long>>> tupledStream = PipelineCommon.of(rawEvents)
                .map(this::assignEventsToSessionWindow)
                .map(this::calculateSessionLengthPerActivity)
                .branch(this::calculateSessionMedianPerSite, this::countSessionPerSite)
                .getValue();


        return joinStreamsAndMapToWebStats(tupledStream);
    }

    PCollection<WebStat> joinStreamsAndMapToWebStats(Tuple2<PCollection<KV<String, Double>>, PCollection<KV<String, Long>>> tuple) {
        final TupleTag<Double> sessionLengthMedian = new TupleTag();
        final TupleTag<Long> sessionsCount = new TupleTag<Long>();

        return MapElements.named("joinStreams").of(
                joinStream(tuple, sessionLengthMedian, sessionsCount))
                .using(kv ->
                        new WebStat(
                                kv.getKey()
                                , kv.getValue().<Double>getOnly(sessionLengthMedian)
                                , kv.getValue().getOnly(sessionsCount)))
                .output();
    }

    PCollection<KV<String, CoGbkResult>> joinStream(Tuple2<PCollection<KV<String, Double>>, PCollection<KV<String, Long>>> tuple, TupleTag<Double> sessionLengthMedian, TupleTag<Long> sessionsCount) {
        return KeyedPCollectionTuple.of(sessionLengthMedian, tuple._1())
                .and(sessionsCount, tuple._2())
                .apply("tuple", CoGroupByKey.create());
    }

    PCollection<KV<String, Double>> mergeTwoStreams(Tuple2<PCollection<KV<String, Double>>, PCollection<KV<String, Double>>> tuple) {
        return PCollectionList.of(Arrays.asList(tuple._1(), tuple._2())).apply(Flatten.pCollections());
    }

    PCollection<KV<String, Long>> countSessionPerSite(PCollection<KV<String, Long>> sessionLengthPerActivity) {
        return
                CountByKey.named("countSessions").of(sessionLengthPerActivity)
                        .keyBy(what -> what.getKey().split(";")[1])
                        .windowBy(new GlobalWindows()).triggeredBy(DefaultTrigger.of())
                        .accumulatingFiredPanes()
                        .output();

    }

    PCollection<KV<String, Double>> calculateSessionMedianPerSite(PCollection<KV<String, Long>> sessionLengthPerActivity) {
        return ReduceByKey.named("caculateSessions").of(sessionLengthPerActivity)
                .keyBy(what -> what.getKey().split(";")[1])
                .valueBy(KV::getValue)
                .reduceBy((Stream<Long> longStream) -> {
                    List<Long> collection = longStream.sorted().collect(Collectors.toList());
                    int collectionLength = collection.size();
                    if (collectionLength % 2 == 0)
                        return (double) ((collection.get(collectionLength / 2) + collection.get(collectionLength / 2 - 1)) / 2);
                    else
                        return Double.valueOf(collection.get(collectionLength / 2));

                })
                .windowBy(new GlobalWindows()).triggeredBy(DefaultTrigger.of())
                .accumulatingFiredPanes()
                .output();
    }

    PCollection<KV<String, Long>> calculateSessionLengthPerActivity(PCollection<KV<String, Iterable<PageView>>> windowed) {
        return MapElements.named("calculateSessionLength").of(windowed)
                .using((UnaryFunction<KV<String, Iterable<PageView>>, KV<String, Long>>) what -> {
                    PageView max = StreamSupport.stream(what.getValue().spliterator(), false).max(Comparator.comparing(PageView::getTimeStamp)).get();
                    PageView min = StreamSupport.stream(what.getValue().spliterator(), false).min(Comparator.comparing(PageView::getTimeStamp)).get();
                    return KV.of(what.getKey(), max.getTimeStamp() - min.getTimeStamp());
                }).output();
    }

    PCollection<KV<String, Iterable<PageView>>> assignEventsToSessionWindow(PCollection<PageView> rawEvents) {
        Window<PageView> pageViewWindow = Window.into(Sessions.withGapDuration(Duration.standardMinutes(30)));


        return AssignEventTime.named("assignTimestamp").of(rawEvents)
                .using(pageView -> pageView.getTimeStamp() * 1000)
                .output()
                .apply("windowedApplay", pageViewWindow)
                .apply("mapWindows", org.apache.beam.sdk.transforms.MapElements.into(
                        TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptor.of(PageView.class)))
                        .via(input -> KV.of(input.getUserId() + ";" + input.getBaseUrl(), input)))
                .apply("groupedKeys", GroupByKey.create());
    }
}