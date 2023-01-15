package com.google.json4beam.pipeline;

import java.util.ArrayList;
import java.util.List;
public class TransformConfig {

  private int id;
  private String name;
  List<TransformProperty> properties = new ArrayList<>();

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public List<TransformProperty> getProperties() {
    return properties;
  }

  public void setProperties(List<TransformProperty> properties) {
    this.properties = properties;
  }

  public void addProperty(TransformProperty property){
    properties.add(property);
  }

  public TransformConfig() {
  }

  public TransformConfig(int id, String name) {
    this.id = id;
    this.name = name;
  }
}
