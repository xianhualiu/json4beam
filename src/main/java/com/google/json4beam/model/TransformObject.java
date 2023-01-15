package com.google.json4beam.model;

/** base class for all transform classes */
class TransformObject {
  private String name;
  private String description;

  public TransformObject() {}

  public TransformObject(String name, String description) {
    this.name = name;
    this.description = description;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }
}
