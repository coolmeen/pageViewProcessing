package com.example.simmilarweb.service.command;

import com.example.simmilarweb.SimmilarwebApplication;
import com.example.simmilarweb.pojo.PageView;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.springframework.stereotype.Service;

@Service
public class SourceReader {

    public PCollection<PageView> readFromFileAndParse(Pipeline p, String filename) {
        return p.apply(TextIO.read().from(filename))
                .apply(ParDo.of(new SimmilarwebApplication.CSvParser()));
    }
}