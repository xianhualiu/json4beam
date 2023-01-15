# json4beam
json4beam is a framework developed in Java to support defining 
and running [Apache Beam](https://beam.apache.org/) 
pipelines in json file.  

## Pipeline Json Config Examples
### Example 1: Pipeline to transfer data from Google BigQuery to Google Spanner.
```
{
   "transformConfigList":[
      {
         "id":1,
         "name":"BigQueryIO.TypedRead",
         "properties":[
            {
               "name":"tableSpec",
               "value":"<google_project>.json4beam_test.person"
            }
         ]
      },
      {
         "id":2,
         "name":"TableRow2Mutation",
         "properties":[
            {
               "name":"table",
               "value":"person"
            }
         ]
      },
      {
         "id":3,
         "name":"SpannerIO.Write",
         "properties":[
            {
               "name":"instanceId",
               "value":"beam-test"
            },
            {
               "name":"databaseId",
               "value":"json4beam_test"
            }
         ]
      }
   ]
}
```
### Example 2: Count Word in File
```
{
   "transformConfigList":[
      {
         "id":1,
         "name":"TextIO.Read",
         "properties":[
            {
               "name":"filepattern",
               "value":"gs://apache-beam-samples/shakespeare/kinglear.txt"
            }
         ]
      },
      {
         "id":2,
         "name":"CountWords"
      },
      {
         "id":3,
         "name":"MapToString"
      },
      {
         "id":4,
         "name":"TextIO.Write",
         "properties":[
            {
               "name":"filenamePrefix",
               "value":"gs://xianhualiu-bucket-1/results/output"
            }
         ]
      }
   ]
}
```

## Run Pipeline
Run mvn command to execute the Json4BeamPipeline java class with graph command 
option pointing to the json config file for the pipeline. For example, 
following command will run the wordcount pipeline defined in wordcount.json file 
on Google Dataflow.

```
mvn compile exec:java      -Dexec.mainClass=com.google.json4beam.Json4BeamPipeline  \
    -Dexec.args="--runner=DataflowRunner \
                  --project=google.com:clouddfe \
                  --stagingLocation=gs://xianhualiu-bucket-1/staging \
                  --graph=/path/to/wordcount.json \
                  --region=us-central1"      -P dataflow-runner
```
