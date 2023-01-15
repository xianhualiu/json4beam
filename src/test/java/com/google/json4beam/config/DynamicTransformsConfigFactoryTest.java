package com.google.json4beam.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import com.google.json4beam.model.*;

public class DynamicTransformsConfigFactoryTest {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  DynamicTransformsConfigFactory factory = new DynamicTransformsConfigFactory();

  @Test
  public void testBigQueryTypedReadConfigs() throws Exception{
    TransformClass bqread = factory.getTransformConfigByName("BigQueryIO.TypedRead");
    String json = OBJECT_MAPPER.writeValueAsString(bqread);
    System.out.println(json);
  }

  @Test
  public void testBigQueryReadConfigs() throws Exception{
    TransformClass bqread = factory.getTransformConfigByName("BigQueryIO.Read");
    String json = OBJECT_MAPPER.writeValueAsString(bqread);
    System.out.println(json);
  }

  @Test
  public void testBigQueryWriteConfigs() throws Exception{
    TransformClass bqread = factory.getTransformConfigByName("BigQueryIO.Write");
    String json = OBJECT_MAPPER.writeValueAsString(bqread);
    System.out.println(json);
  }

  @Test
  public void testSpannerConfigs() throws Exception{
    String json = OBJECT_MAPPER.writeValueAsString(factory.getTransformConfigByName("SpannerIO.Write"));
    System.out.println(json);
  }

  @Test
  public void testTableRow2MutationConfigs() throws Exception{
    String json = OBJECT_MAPPER.writeValueAsString(factory.getTransformConfigByName("TableRow2Mutation"));
    System.out.println(json);
  }
}
