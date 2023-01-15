package com.google.json4beam.pipeline;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

public class TransformConfigGraphTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  public void testGraph() throws JsonProcessingException {
    TransformConfigGraph graph = new TransformConfigGraph();
    TransformConfig config1 = new TransformConfig(1, "TextIO.Read");
    config1.addProperty(
        new TransformProperty("filepattern", "gs://apache-beam-samples/shakespeare/kinglear.txt"));
    graph.addTransformConfig(config1);

    graph.addTransformConfig(new TransformConfig(2, "CountWords"));

    TransformConfig config2 = new TransformConfig(3, "TextIO.Write");
    config2.addProperty(
        new TransformProperty("filenamePrefix", "gs://xianhualiu-bucket-1/results/output"));
    graph.addTransformConfig(config2);

    String json = OBJECT_MAPPER.writeValueAsString(graph);
    System.out.println(json);
  }
}
