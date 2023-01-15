package com.google.json4beam.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.json4beam.exceptions.TransformServiceException;
import com.google.common.flogger.GoogleLogger;
import com.google.gson.Gson;
import com.google.json4beam.pipeline.TransformProperty;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;

/**
 * TransformClass represents a Beam PTransform class including the class name and method to build
 * and config a PTransform object.
 */
public class TransformClass extends TransformObject {

  private static final GoogleLogger logger = GoogleLogger.forEnclosingClass();
  private String className;
  private TransformMethod builder;
  private String type;

  Set<TransformMethod> attributes = new HashSet<TransformMethod>();

  public TransformClass(String name, String description, String className) {
    super(name, description);
    this.className = className;
  }

  public TransformMethod getBuilder() {
    return builder;
  }

  public void setBuilder(TransformMethod builder) {
    this.builder = builder;
  }

  public String getClassName() {
    return className;
  }

  public void setClassName(String className) {
    this.className = className;
  }

  @JsonIgnore
  public Class<?> getClazz() {
    try {
      return Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new TransformServiceException("Failed to create Class for name: " + className, e);
    }
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public void addAttribute(TransformMethod attribute) {
    attributes.add(attribute);
  }

  public Set<TransformMethod> getAttributes() {
    return attributes;
  }

  public void setAttributes(Set<TransformMethod> attributes) {
    this.attributes = attributes;
  }

  public Object createTransform(Map<String, Object> properties) {
    Object transform = null;
    if (builder == null) {
      // create using defsult constructor
      logger.atInfo().log("Initiate transfrom: %s with constructor.", getClassName());
      try {
        Class<?> clazz = Class.forName(className);
        Constructor<?>[] ctors = Class.forName(className).getDeclaredConstructors();
        Constructor<?> ctor = null;
        for (int i = 0; i < ctors.length; i++) {
          ctor = ctors[i];
          if (ctor.getParameterCount() == 0) {
            break;
          }
        }
        ctor.setAccessible(true);
        transform = ctor.newInstance();
      } catch (Exception e) {
        throw new IllegalArgumentException("Invalid class name.", e);
      }
    } else {
      logger.atInfo().log("Initiate transfrom: %s", builder.getName());
      transform = builder.invoke(properties);
    }
    logger.atInfo().log("Created transfrom: %s", transform.getClass().getName());
    for (TransformMethod method : attributes) {
      if (method == null) {
        logger.atInfo().log("Null method.");
        continue;
      }
      logger.atInfo().log("Set property: %s", method.getName());
      transform = method.invoke(transform, properties);
      logger.atInfo().log("Updated transfrom: %s", transform.getClass().getName());
    }
    logger.atInfo().log("Successfully created transfrom: %s", transform.getClass().getName());
    return transform;
  }

  @JsonIgnore
  public List<TransformProperty> getProperties() {
    List<TransformProperty> properties = new ArrayList<>();
    for (TransformMethod method : attributes) {
      for (TransformClass arg : method.getArguments()) {
        TransformProperty p = new TransformProperty();
        p.setName(arg.getName());
        properties.add(p);
      }
      if (method.getArguments().isEmpty()) {
        // put method itself as property with bool type
        TransformProperty p = new TransformProperty();
        p.setName(method.getName());
        properties.add(p);
      }
    }
    return properties;
  }

  public String toString() {
    Gson gson = new Gson();
    return gson.toJson(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TransformClass)) {
      return false;
    }
    TransformClass that = (TransformClass) o;
    return Objects.equals(className, that.className);
  }

  @Override
  public int hashCode() {
    return Objects.hash(className);
  }
}
