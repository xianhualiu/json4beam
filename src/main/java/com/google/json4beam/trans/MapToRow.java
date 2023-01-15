package com.google.json4beam.trans;

import com.google.json4beam.trans.MapToString.FormatAsTextFn;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class MapToRow extends PTransform<PCollection<KV<Integer, String>>, PCollection<Row>> {
  static final Schema schema = Schema.builder().addInt32Field("id").addStringField("text").build();
  public MapToRow() {
  }

  @Override
  public PCollection<Row> expand(PCollection<KV<Integer, String>> input) {
    return input.apply(MapElements.via(new MapToRowFn()));
  }

  public static class MapToRowFn extends SimpleFunction<KV<Integer, String>, Row> {

    @Override
    public Row apply(KV<Integer, String> input) {
      return Row.withSchema(schema).withFieldValue("id", input.getKey())
          .withFieldValue("text", input.getValue()).build();
    }
  }
}
