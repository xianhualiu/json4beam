package com.google.json4beam.trans;

import com.google.api.services.bigquery.model.TableRow;
import java.util.UUID;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import com.google.cloud.spanner.Mutation;
import org.apache.beam.sdk.transforms.PTransform;

public class TableRow2Mutation extends PTransform<PCollection<TableRow>, PCollection<Mutation>> {

  private String table;

  public TableRow2Mutation() {
  }

  public void setTable(String table) {
    this.table = table;
  }

  public String getTable() {
    return table;
  }

  @Override
  public PCollection<Mutation> expand(PCollection<TableRow> input) {
    return input.apply(MapElements.via(new TableRow2MutationFn()));
  }

  public class TableRow2MutationFn extends SimpleFunction<TableRow, Mutation> {

    @Override
    public Mutation apply(TableRow input) {
      Mutation.WriteBuilder builder = Mutation.newInsertBuilder(table);

      for (Object key : input.keySet()) {
        Object value = input.get(key);
        if (value instanceof String) {
          builder.set(key.toString()).to((String) value);
        } else if (value instanceof Long) {
          builder.set(key.toString()).to((Long) value);
        }
        if (value instanceof Integer) {
          builder.set(key.toString()).to((Integer) value);
        }
        if (value instanceof Boolean) {
          builder.set(key.toString()).to((Boolean) value);
        }
        if (value instanceof Double) {
          builder.set(key.toString()).to((Double) value);
        }
        if (value instanceof Float) {
          builder.set(key.toString()).to((Float) value);
        }
      }

      builder.set("id").to(UUID.randomUUID().toString());
      return builder.build();
    }
  }
}
