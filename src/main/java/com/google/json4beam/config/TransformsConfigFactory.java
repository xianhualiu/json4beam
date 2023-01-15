package com.google.json4beam.config;

import com.google.json4beam.model.TransformClass;
import java.util.Map;

/**
 * TransformsConfigFactory is the factory interface for generating and serving Beam PTransform
 * configurations.
 */
public interface TransformsConfigFactory {
  public Map<String, TransformClass> getTransformConfigs();

  public TransformClass getTransformConfigByName(String name);

  public Map<String, TransformClass> getTransformConfigByType(String type);
}
