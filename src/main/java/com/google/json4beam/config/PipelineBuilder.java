package com.google.json4beam.config;

import com.google.json4beam.model.TransformClass;
import com.google.common.flogger.GoogleLogger;
import com.google.json4beam.pipeline.TransformConfig;
import com.google.json4beam.pipeline.TransformConfigGraph;
import com.google.json4beam.pipeline.TransformProperty;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;

/** PipelineBuilder builds Beam Pipeline based on transform config graph. */
public class PipelineBuilder {
  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  public PipelineBuilder() {}

  public Pipeline build(
      TransformConfigGraph graph, PipelineOptions options, TransformsConfigFactory factory) {

    Pipeline pipeline = Pipeline.create(options);

    Object currentCollection = null;
    for (TransformConfig tc : graph.getTransformConfigList()) {
      TransformClass configClass = factory.getTransformConfigByName(tc.getName());
      if (configClass == null) {
        break;
      }
      Map<String, Object> propertiesMap = new HashMap<>();
      for (TransformProperty property : tc.getProperties()) {
        if (!property.getValue().isEmpty()) {
          logger.atInfo().log("Add property: %s = %s", property.getName(), property.getValue());
          propertiesMap.put(property.getName(), property.getValue());
        }
      }
      Object transform = configClass.createTransform(propertiesMap);
      if (currentCollection == null) {
        currentCollection = apply(pipeline, transform);
      } else {
        currentCollection = apply(currentCollection, transform);
      }
    }
    return pipeline;
  }

  private Object apply(Object collection, Object transform) {
    try {
      Method app = collection.getClass().getDeclaredMethod("apply", PTransform.class);
      return app.invoke(collection, transform);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
