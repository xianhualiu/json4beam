package com.google.json4beam.trans;

import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class MapToString extends PTransform<PCollection<KV<String, Long>>, PCollection<String>> {

  public MapToString() {
  }

  @Override
  public PCollection<String> expand(PCollection<KV<String, Long>> input) {
    return input.apply(MapElements.via(new FormatAsTextFn()));
  }

  public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {

    @Override
    public String apply(KV<String, Long> input) {
      return input.getKey() + ": " + input.getValue();
    }
  }
}
