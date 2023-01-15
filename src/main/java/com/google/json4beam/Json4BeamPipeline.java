package com.google.json4beam;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.flogger.GoogleLogger;
import com.google.json4beam.config.DynamicTransformsConfigFactory;
import com.google.json4beam.config.PipelineBuilder;
import com.google.json4beam.config.TransformsConfigFactory;
import com.google.json4beam.pipeline.TransformConfigGraph;
import java.io.IOException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import com.fasterxml.jackson.core.json.JsonWriteFeature;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import java.io.File;

public class Json4BeamPipeline {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  private static TransformsConfigFactory factory = new DynamicTransformsConfigFactory();
  private static PipelineBuilder builder = new PipelineBuilder();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public interface Json4BeamPipelineOptions extends PipelineOptions {

    @Description("Path to the pipeline transform configuration graph file")
    String getGraph();

    void setGraph(String graph);
  }

  public static void main(String[] args) {
    //OBJECT_MAPPER.disable(JsonWriteFeature.QUOTE_FIELD_NAMES.mappedFeature());
    OBJECT_MAPPER.enable(JsonReadFeature.ALLOW_UNQUOTED_FIELD_NAMES.mappedFeature());

    OBJECT_MAPPER.enable(JsonReadFeature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER.mappedFeature());
    //OBJECT_MAPPER.enable(JsonReadFeature.ALLOW_SINGLE_QUOTES.mappedFeature());

    Json4BeamPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(Json4BeamPipelineOptions.class);

    String graphFile = options.getGraph();

    Pipeline pipeline = Pipeline.create(options);

    if (graphFile != null && !graphFile.isEmpty()) {

      TransformConfigGraph graph = null;
      try {
        graph = OBJECT_MAPPER.readValue(new File(graphFile), TransformConfigGraph.class);
      } catch (IOException e) {
        logger.atSevere().log("Failed to parse graph in file: %s", graphFile, e);
        throw new IllegalArgumentException("Invalid json in file: " + graphFile, e);
      }
      pipeline = builder.build(graph, options, factory);
    }
    pipeline.run().waitUntilFinish();
  }
}
