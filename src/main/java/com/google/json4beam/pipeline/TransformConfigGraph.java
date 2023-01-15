package com.google.json4beam.pipeline;

import java.util.ArrayList;
import java.util.List;

public class TransformConfigGraph {
  List<TransformConfig> transformConfigList =new ArrayList<>();

  public TransformConfigGraph() {
  }

  public List<TransformConfig> getTransformConfigList() {
    return transformConfigList;
  }

  public void setTransformConfigList(
      List<TransformConfig> transformConfigList) {
    this.transformConfigList = transformConfigList;
  }

  public void addTransformConfig(TransformConfig transformConfig){
    transformConfigList.add(transformConfig);
  }

}
