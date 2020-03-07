package com.example.simmilarweb;

import com.example.simmilarweb.pojo.PageView;
import com.example.simmilarweb.pojo.UserStats;
import com.example.simmilarweb.pojo.WebStat;
import com.example.simmilarweb.service.PipeLIneRunner;
import com.example.simmilarweb.service.command.SessionStatsPipelineExecutor;
import com.example.simmilarweb.service.command.UniqueVisitsPipelineExecutor;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.kryo.KryoCoderProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import java.time.Duration;
import java.time.Instant;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SpringBootTest
@Import(PipeLIneRunner.class)
public class PipeLIneRunnerTest {


    @Autowired
    UniqueVisitsPipelineExecutor uniqueVisitsPipelineExecutor;

    @Autowired
    SessionStatsPipelineExecutor sessionStatsPipelineExecutor;

    public final transient TestPipeline pipeline = TestPipeline.create();

    @BeforeEach
    public  void test(){
        KryoCoderProvider.of(
                (kryo) -> {
                    kryo.register(PageView.class);
                })
                .registerTo(pipeline);
    }

    @Test
    public void whenTwoSessionWith31MintuesApart() throws Exception {
        PageView firstSession = new PageView("user1", "a", "a", Instant.now().getEpochSecond());
        PageView secondSession = new PageView("user1", "a", "a1", Instant.now().minus(Duration.ofMinutes(31)).getEpochSecond());
        pipeline.enableAbandonedNodeEnforcement(true);
        pipeline.enableAutoRunIfMissing(true);
        PCollection<PageView> input = pipeline.apply(Create.of(firstSession, secondSession));

        PCollection<UserStats> userStats = uniqueVisitsPipelineExecutor.processUniqueVisitsPipeline(input);
        PCollection<WebStat> webStats = sessionStatsPipelineExecutor.processSessionStatsPipeline(input);
        PAssert.that(userStats)
                .containsInAnyOrder(new UserStats("user1",1l));

        PAssert.that(webStats)
                .containsInAnyOrder(new WebStat("a",0d,2l));
        pipeline.run().waitUntilFinish();

    }


    @Test
    public void whenTwoEventsWith1MintueApart() throws Exception {
        PageView firstSession = new PageView("user1", "a", "a", Instant.now().getEpochSecond());
        PageView secondSession = new PageView("user1", "a", "a1", Instant.now().minus(Duration.ofMinutes(1)).getEpochSecond());
        PCollection<PageView> input = pipeline.apply(Create.of(firstSession, secondSession));
        PCollection<UserStats> userStats = uniqueVisitsPipelineExecutor.processUniqueVisitsPipeline(input);
        PCollection<WebStat> webStats = sessionStatsPipelineExecutor.processSessionStatsPipeline(input);
        pipeline.enableAbandonedNodeEnforcement(true);
        pipeline.enableAutoRunIfMissing(true);

        PAssert.that(userStats)
                .containsInAnyOrder(new UserStats("user1",1l));

        //one session with 60 seconds apart
        PAssert.that(webStats)
                .containsInAnyOrder(new WebStat("a",60d,1l));
        pipeline.run().waitUntilFinish();

    }

    @Test
    public void whenTwoEventsWithSameTimeStamp() throws Exception {
        PageView firstSession = new PageView("user1", "a", "a", Instant.now().getEpochSecond());
        PageView secondSession = new PageView("user1", "a", "a1", Instant.now().getEpochSecond());
        PCollection<PageView> input = pipeline.apply(Create.of(firstSession, secondSession));
        PCollection<UserStats> userStats = uniqueVisitsPipelineExecutor.processUniqueVisitsPipeline(input);
        PCollection<WebStat> webStats = sessionStatsPipelineExecutor.processSessionStatsPipeline(input);
        pipeline.enableAbandonedNodeEnforcement(true);
        pipeline.enableAutoRunIfMissing(true);

        PAssert.that(userStats)
                .containsInAnyOrder(new UserStats("user1",1l));

        //0 sessions
        PAssert.that(webStats)
                .containsInAnyOrder(new WebStat("a",0d,1l));
        pipeline.run().waitUntilFinish();

    }

    @Test
    public void whenOnlyOneTimeStampSessionLengthisZeroAndCountAsOneevent() throws Exception {
        PageView firstSession = new PageView("user1", "a", "a", Instant.now().getEpochSecond());
        PCollection<PageView> input = pipeline.apply(Create.of(firstSession));
        PCollection<UserStats> userStats = uniqueVisitsPipelineExecutor.processUniqueVisitsPipeline(input);
        PCollection<WebStat> webStats = sessionStatsPipelineExecutor.processSessionStatsPipeline(input);
        pipeline.enableAbandonedNodeEnforcement(true);
        pipeline.enableAutoRunIfMissing(true);

        PAssert.that(userStats)
                .containsInAnyOrder(new UserStats("user1",1l));

        //0 sessions
        PAssert.that(webStats)
                .containsInAnyOrder(new WebStat("a",0d,1l));
        pipeline.run().waitUntilFinish();

    }


    @Test
    public void whenMultipleConcurentSessionWindowsThatShouldTogetherAndMix() throws Exception {
        PageView firstSession1 = new PageView("user1", "a", "a", Instant.now().getEpochSecond());
        PageView secondSession1 = new PageView("user2", "b", "a", Instant.now().getEpochSecond());
        PageView secondsession2= new PageView("user2", "b", "a", Instant.now().plus(Duration.ofMinutes(3)).getEpochSecond());
        PageView firstSession2 = new PageView("user1", "a", "a", Instant.now().plus(Duration.ofMinutes(9)).getEpochSecond());
        PCollection<PageView> input = pipeline.apply(Create.of(firstSession1,secondsession2,secondSession1,firstSession1,firstSession2));
        PCollection<UserStats> userStats = uniqueVisitsPipelineExecutor.processUniqueVisitsPipeline(input);
        PCollection<WebStat> webStats = sessionStatsPipelineExecutor.processSessionStatsPipeline(input);
        pipeline.enableAbandonedNodeEnforcement(true);
        pipeline.enableAutoRunIfMissing(true);

        PAssert.that(userStats)
                .containsInAnyOrder(new UserStats("user1",1l),new UserStats("user2",1l));
        //0 sessions
        PAssert.that(webStats)
                .containsInAnyOrder(new WebStat("a",9*60d,1l),new WebStat("b",3*60d,1l));
        pipeline.run().waitUntilFinish();

    }


    @Test
    public void whenTwoDiffentSitesTwoDifferentSession() throws Exception {
        PageView firstSession = new PageView("user1", "a", "a", Instant.now().getEpochSecond());
        PageView secondSession = new PageView("user1", "b", "a1", Instant.now().minus(Duration.ofMinutes(1)).getEpochSecond());
        PCollection<PageView> input = pipeline.apply(Create.of(firstSession, secondSession));
        PCollection<UserStats> userStats = uniqueVisitsPipelineExecutor.processUniqueVisitsPipeline(input);
        PCollection<WebStat> webStats = sessionStatsPipelineExecutor.processSessionStatsPipeline(input);
        pipeline.enableAbandonedNodeEnforcement(true);
        pipeline.enableAutoRunIfMissing(true);

        PAssert.that(userStats)
                .containsInAnyOrder(new UserStats("user1",2l));


        //two sessions two different sites
        PAssert.IterableAssert<WebStat> webStatIterableAssert = PAssert.that(webStats)
                .containsInAnyOrder(new WebStat("a", 0d, 1l)
                        , (new WebStat("b", 0d, 1l)));
        pipeline.run().waitUntilFinish();




    }


}