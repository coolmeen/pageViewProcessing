package com.example.simmilarweb.service;

import com.example.simmilarweb.pojo.PageView;
import com.example.simmilarweb.pojo.UserStats;
import com.example.simmilarweb.pojo.WebStat;
import com.example.simmilarweb.service.command.SessionStatsPipelineExecutor;
import com.example.simmilarweb.service.command.SinkWriter;
import com.example.simmilarweb.service.command.SourceReader;
import com.example.simmilarweb.service.command.UniqueVisitsPipelineExecutor;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.kryo.KryoCoderProvider;
import org.apache.beam.sdk.values.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@Profile("main")
public class PipeLIneRunner implements CommandLineRunner {

    private UniqueVisitsPipelineExecutor uniqueVisitsPipeline;
    private SessionStatsPipelineExecutor sessionSeasPipeline;
    private Pipeline pipeline;
    private SourceReader sourceReader;
    private SinkWriter writer;

    @Autowired
    public PipeLIneRunner(UniqueVisitsPipelineExecutor unuiqueVistsPipelineExecutor, SessionStatsPipelineExecutor sessionStasPipelineExecutor, Pipeline pipeline, SourceReader sourceReader, SinkWriter writer) {
        this.uniqueVisitsPipeline = unuiqueVistsPipelineExecutor;
        this.sessionSeasPipeline = sessionStasPipelineExecutor;
        this.pipeline = pipeline;
        this.sourceReader = sourceReader;
        this.writer = writer;
    }


    @Override
    public void run(String... args) throws Exception {

        String filePattern = args.length == 0 ? "input_*.csv" : args[0];
        registerSerilizable(pipeline);

        PCollection<PageView> rawEvents = sourceReader.readFromFileAndParse(pipeline, filePattern);

        PCollection<UserStats> visits = uniqueVisitsPipeline.processUniqueVisitsPipeline(rawEvents);
        PCollection<WebStat> sessionStats = sessionSeasPipeline.processSessionStatsPipeline(rawEvents);

        writer.writeToSink(visits);
        writer.writeTosink(sessionStats);

        pipeline.run().waitUntilFinish();
        log.info("finished processing data");

    }
    private void registerSerilizable(Pipeline p) {
        KryoCoderProvider.of(
                (kryo) -> {
                    kryo.register(PageView.class);
                })
                .registerTo(p);
    }



}
