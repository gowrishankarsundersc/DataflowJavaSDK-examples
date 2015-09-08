package com.google.cloud.dataflow.examples.metrics;

import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.reflect.TypeToken;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.cloud.dataflow.sdk.coders.MapCoder;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by gowrishankar.sunder on 8/28/15.
 */
public class SummaryHelper {
    private static final Logger LOG = LoggerFactory.getLogger(SummaryHelper.class.getName());

    static class ParseSummariesFn extends DoFn<String, KV<String, Map<String, Double>>> {
        private static final long serialVersionUID = 0;
        private static Gson gson = new Gson();

        @Override
        public void processElement(ProcessContext context) throws Exception {
            Map<String, Map<String, Double>> summaryMap;
            try {
                summaryMap = gson.fromJson((String) context.element(),
                        new TypeToken<Map<String, Map<String, Double>>>() {}.getType());

                if (summaryMap != null) {
                    for (String summaryName : summaryMap.keySet()) {
                        context.output(KV.of(summaryName, summaryMap.get(summaryName)));
                    }
                }
            } catch (Exception e) {
                LOG.warn("Unable to parse metric message", e);
            }
        }
    }

    static class FormatJsonFn extends DoFn<KV<String, Map<String, Double>>, KV<String, String>> {
        private static final long serialVersionUID = 0;
        private static Gson gson = new Gson();

        @Override
        public void processElement(ProcessContext context) throws Exception {
            try {
                String summaryJson = (String) gson.toJson(context.element().getValue());
                context.output(KV.of(context.element().getKey(), summaryJson));
            } catch (Exception e) {
                LOG.warn("Unable to encode JSON value", e);
            }
        }
    }

    static class SummaryProcessor extends PTransform<PCollection<String>, PCollection<KV<String, String>>> {
        private static final long serialVersionUID = 0;

        @Override
        public PCollection<KV<String, String>> apply(PCollection<String> instanceReports) {
            PCollection<KV<String, Map<String, Double>>> partitionedReports =
                    instanceReports.apply(ParDo.of(new ParseSummariesFn()));

            PCollection<KV<String, Map<String, Double>>> perMetricSummary = partitionedReports.apply(
                    Combine.<String, Map<String, Double>>perKey(new SummaryMerger()));
            PCollection<KV<String, String>> results = perMetricSummary.apply(ParDo.of(new FormatJsonFn()));
            return results;
        }
    }

    public static class SummaryMerger implements SerializableFunction<Iterable<Map<String, Double>>, Map<String, Double>> {
        private static final long serialVersionUID = 0;

        @Override
        public Map<String, Double> apply(Iterable<Map<String, Double>> summaries) {
            Map <String, Double> globalSummary = new HashMap<>();
            for (Map <String, Double> summary: summaries) {
                for (Map.Entry<String, Double> metric: summary.entrySet()) {
                    if (globalSummary.get(metric.getKey()) == null)
                        globalSummary.put(metric.getKey(), metric.getValue());
                    else
                        globalSummary.put(metric.getKey(), metric.getValue() + globalSummary.get(metric.getKey()));
                }
            }

            return globalSummary;
        }
    }
}
