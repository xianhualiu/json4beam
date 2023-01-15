package com.google.json4beam.trans;

import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/** transform to split and count words */
final class CountWords extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {
  @Override
  public PCollection<KV<String, Long>> expand(PCollection<String> lines) {

    // Convert lines of text into individual words.
    DoFn<String, String> splitDoFn =
        new DoFn<String, String>() {
          public static final String TOKENIZER_PATTERN = "[^\\p{L}]+";

          @ProcessElement
          public void processElement(@Element String element, OutputReceiver<String> receiver) {

            // Split the line into words.
            String[] words = element.split(TOKENIZER_PATTERN, -1);

            // Output each word encountered into the output PCollection.
            for (String word : words) {
              if (!word.isEmpty()) {
                receiver.output(word);
              }
            }
          }
        };

    PCollection<String> words = lines.apply(ParDo.of(splitDoFn));
    // Count the number of times each word occurs.
    PCollection<KV<String, Long>> wordCounts = words.apply(Count.perElement());

    return wordCounts;
  }

  public CountWords() {}
}
