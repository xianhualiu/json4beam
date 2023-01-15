package com.google.json4beam.config;

import com.google.json4beam.model.TransformClass;
import com.google.common.flogger.GoogleLogger;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * DynamicTransformsConfigFactory is an implementation of TransformsConfigFactory to retrieve
 * transform configuration from the Beam SDK
 */
public class DynamicTransformsConfigFactory implements TransformsConfigFactory {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  private static final String[] PACKAGES_TO_SCAN = {
      "org.apache.beam.sdk.io.gcp",
      "org.apache.beam.sdk.io", "com.google.json4beam.trans"
  };
  private Map<String, TransformClass> transformConfigs;

  public DynamicTransformsConfigFactory() {
    init(PACKAGES_TO_SCAN);
  }

  public DynamicTransformsConfigFactory(String[] packagesToScan) {
    init(packagesToScan);
  }

  @Override
  public Map<String, TransformClass> getTransformConfigs() {
    return transformConfigs;
  }

  @Override
  public TransformClass getTransformConfigByName(String name) {
    return transformConfigs.get(name);
  }

  @Override
  public Map<String, TransformClass> getTransformConfigByType(String type) {
    Map<String, TransformClass> results = new HashMap<>();
    for (Map.Entry<String, TransformClass> entry : transformConfigs.entrySet()) {
      if (type.equalsIgnoreCase(entry.getValue().getType())) {
        results.put(entry.getKey(), entry.getValue());
      }
    }
    return results;
  }

  public void init(String[] packagesToScan) {
    TransformConfigVisitor visitor = new TransformConfigVisitor();
    for (String pkg : packagesToScan) {
      for (Class<?> clazz : findAllClasses(pkg)) {
        visitor.visit(clazz);
      }
    }
    transformConfigs = visitor.getConfigMap();
  }

  private Set<Class<?>> findAllClasses(String packageName) {
    Set<Class<?>> classes = new HashSet<>();
    logger.atInfo().log("Initialiting ...");
    try {
      ClassPath classPath = ClassPath.from(DynamicTransformsConfigFactory.class.getClassLoader());
      for (ClassInfo classInfo : classPath.getTopLevelClassesRecursive(packageName)) {
        for (String pkg : PACKAGES_TO_SCAN) {
          if (classInfo.getPackageName().indexOf(pkg) == 0) {
            logger.atInfo().log("Found class: %s", classInfo.getName());
            try {
              classes.add(Class.forName(classInfo.getName()));
            }catch(Exception e){

            }
            break;
          }
        }
      }
    } catch (Exception e) {
      logger.atInfo().withCause(e).log("Error: %s", e.getMessage());
    }
    return classes;
  }
}
