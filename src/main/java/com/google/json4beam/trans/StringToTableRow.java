package com.google.json4beam.trans;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

public class StringToTableRow extends PTransform<PCollection<String>, PCollection<TableRow>> {

  public StringToTableRow() {
  }

  @Override
  public PCollection<TableRow> expand(PCollection<String> input) {
    return input.apply(MapElements.via(new String2TableRowFn()));
  }

  public class String2TableRowFn extends SimpleFunction<String, TableRow> {

    @Override
    public TableRow apply(String input) {
      TableRow row = new TableRow();
      String ss[] = input.split(",");
      if(ss.length == 2) {
        row.set("id", Integer.parseInt(ss[0]));
        row.set("text", ss[1]);
      }
      return row;
    }
  }
}
