/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.examples.metrics;

import com.google.cloud.dataflow.sdk.coders.MapCoder;
import com.snapchat.hotfuzz.util.TopKCountFinder;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.examples.common.DataflowExampleUtils;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowWorkerLoggingOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.collect.ImmutableMap;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.cloud.dataflow.examples.*;
import com.google.cloud.dataflow.sdk.options.DataflowWorkerLoggingOptions.Level;


/**
 * An example that counts words in text, and can run over either unbounded or bounded input
 * collections.
 *
 * <p> This class, {@link WindowedWordCount}, is the last in a series of four successively more
 * detailed 'word count' examples. First take a look at {@link MinimalWordCount},
 * {@link WordCount}, and {@link DebuggingWordCount}.
 *
 * <p> Basic concepts, also in the MinimalWordCount, WordCount, and DebuggingWordCount examples:
 * Reading text files; counting a PCollection; writing to GCS; executing a Pipeline both locally
 * and using the Dataflow service; defining DoFns; creating a custom aggregator;
 * user-defined PTransforms; defining PipelineOptions.
 *
 * <p> New Concepts:
 * <pre>
 *   1. Unbounded and bounded pipeline input modes
 *   2. Adding timestamps to data
 *   3. PubSub topics as sources
 *   4. Windowing
 *   5. Re-using PTransforms over windowed PCollections
 *   6. Writing to BigQuery
 * </pre>
 *
 * <p> To execute this pipeline locally, specify general pipeline configuration:
 * <pre>{@code
 *   --project=YOUR_PROJECT_ID
 * }
 * </pre>
 *
 * <p> To execute this pipeline using the Dataflow service, specify pipeline configuration:
 * <pre>{@code
 *   --project=YOUR_PROJECT_ID
 *   --stagingLocation=gs://YOUR_STAGING_DIRECTORY
 *   --runner=BlockingDataflowPipelineRunner
 * }
 * </pre>
 *
 * <p> Optionally specify the input file path via:
 * {@code --inputFile=gs://INPUT_PATH},
 * which defaults to {@code gs://dataflow-samples/shakespeare/kinglear.txt}.
 *
 * <p> Specify an output BigQuery dataset and optionally, a table for the output. If you don't
 * specify the table, one will be created for you using the job name. If you don't specify the
 * dataset, a dataset called {@code dataflow-examples} must already exist in your project.
 * {@code --bigQueryDataset=YOUR-DATASET --bigQueryTable=YOUR-NEW-TABLE-NAME}.
 *
 * <p> Decide whether you want your pipeline to run with 'bounded' (such as files in GCS) or
 * 'unbounded' input (such as a PubSub topic). To run with unbounded input, set
 * {@code --unbounded=true}. Then, optionally specify the Google Cloud PubSub topic to read from
 * via {@code --pubsubTopic=projects/PROJECT_ID/topics/YOUR_TOPIC_NAME}. If the topic does not
 * exist, the pipeline will create one for you. It will delete this topic when it terminates.
 * The pipeline will automatically launch an auxiliary batch pipeline to populate the given PubSub
 * topic with the contents of the {@code --inputFile}, in order to make the example easy to run.
 * If you want to use an independently-populated PubSub topic, indicate this by setting
 * {@code --inputFile=""}. In that case, the auxiliary pipeline will not be started.
 *
 * <p> By default, the pipeline will do fixed windowing, on 1-minute windows.  You can
 * change this interval by setting the {@code --windowSize} parameter, e.g. {@code --windowSize=10}
 * for 10-minute windows.
 */
public class MetricsAggregator {
    private static final Logger LOG = LoggerFactory.getLogger(MetricsAggregator.class);
    static final int WINDOW_SIZE = 30;  // Default window duration in seconds

    /** A DoFn that converts a Word and Count into a BigQuery table row. */
    static class FormatAsTableRowFn extends DoFn<KV<String, String>, TableRow> {
        private static final long serialVersionUID = 0;

        @Override
        public void processElement(ProcessContext c) {
            TableRow row = new TableRow()
                    .set("metric", c.element().getKey())
                    .set("json", c.element().getValue())
                            // include a field for the window timestamp
                    .set("window_timestamp", c.timestamp().toString());
            c.output(row);
        }
    }

    /**
     * Helper method that defines the BigQuery schema used for the output.
     */
    private static TableSchema getSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("metric").setType("STRING"));
        fields.add(new TableFieldSchema().setName("json").setType("STRING"));
        fields.add(new TableFieldSchema().setName("window_timestamp").setType("TIMESTAMP"));
        TableSchema schema = new TableSchema().setFields(fields);
        return schema;
    }

    /**
     * Concept #6: We'll stream the results to a BigQuery table. The BigQuery output source is one
     * that supports both bounded and unbounded data. This is a helper method that creates a
     * TableReference from input options, to tell the pipeline where to write its BigQuery results.
     */
    private static TableReference getTableReference(Options options) {
        TableReference tableRef = new TableReference();
        tableRef.setProjectId(options.getProject());
        tableRef.setDatasetId(options.getBigQueryDataset());
        tableRef.setTableId(options.getBigQueryTable());
        return tableRef;
    }

    /**
     * Options supported by {@link WindowedWordCount}.
     *
     * <p> Inherits standard example configuration options, which allow specification of the BigQuery
     * table and the PubSub topic, as well as the {@link WordCount.WordCountOptions} support for
     * specification of the input file.
     */
    public static interface Options
            extends WordCount.WordCountOptions, DataflowExampleUtils.DataflowExampleUtilsOptions {
        @Description("Fixed window duration, in minutes")
        @Default.Integer(WINDOW_SIZE)
        Integer getWindowSize();
        void setWindowSize(Integer value);

        @Description("Whether to run the pipeline with unbounded input")
        boolean isUnbounded();
        void setUnbounded(boolean value);

        @Description("Pubsub Subscription")
        String getPubsubSubscription();
        void setPubsubSubscription(String s);
    }

    public static void main(String[] args) throws IOException {
        Options options = PipelineOptionsFactory.fromArgs(args).as(Options.class);
        options.setBigQuerySchema(getSchema());
        options.setRunner(DataflowPipelineRunner.class);
//        options.setRunner(DirectPipelineRunner.class);
        options.setProject("devsnapchat");
        options.setStagingLocation("gs://gsunder-dataflow/staging");
        options.setOutput("gs://gsunder-dataflow/minimal-output-");
        options.setPubsubTopic("projects/devsnapchat/topics/gsunder-sample-metrics");
        options.setUnbounded(true);
        options.setInputFile("");
        options.setBigQueryDataset("gsunder_dataflow");
        options.setBigQueryTable("aggregated_summaries");

        options.setMaxNumWorkers(1);

        DataflowWorkerLoggingOptions.WorkerLogLevelOverrides overrides = new DataflowWorkerLoggingOptions.WorkerLogLevelOverrides();
        overrides.addOverrideForName("com.google.cloud.dataflow.examples", DataflowWorkerLoggingOptions.Level.DEBUG);
        options.setWorkerLogLevelOverrides(overrides);
        // DataflowExampleUtils creates the necessary input sources to simplify execution of this
        // Pipeline.
        DataflowExampleUtils exampleDataflowUtils = new DataflowExampleUtils(options,
                options.isUnbounded());

        Pipeline pipeline = Pipeline.create(options);
        LOG.info("Reading from PubSub.");
        PCollection<String> input = pipeline
                .apply(PubsubIO.Read.topic(options.getPubsubTopic()));

        PCollection<String> windowedMetrics = input
                .apply(Window.<String>into(
                        FixedWindows.of(Duration.standardSeconds(options.getWindowSize()))));

        PCollection<KV<String, String>> mergedSummaries = windowedMetrics.apply(new SummaryHelper.SummaryProcessor());

        mergedSummaries.apply(ParDo.of(new FormatAsTableRowFn()))
                .apply(BigQueryIO.Write.to(getTableReference(options)).withSchema(getSchema()));
        PipelineResult result = pipeline.run();
        System.out.print("Completed run");
    }
}
