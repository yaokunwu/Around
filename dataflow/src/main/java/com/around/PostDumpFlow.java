package com.around;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;

import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableScanConfiguration;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableRow;

public class PostDumpFlow {

    private static final String PROJECT_ID = "around-314619";
    private static final String BIGTABLE_ID = "around-post";
    private static final String BIGTABLE_TABLEID = "post";
    private static final String BIGQUERY_ID ="post_analysis";
    private static String BIGQUERY_TABLENAME ="daily_dump_1";
    private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline p = Pipeline.create(options);

        CloudBigtableScanConfiguration config = new CloudBigtableScanConfiguration.Builder()
                .withProjectId(PROJECT_ID)
                .withInstanceId(BIGTABLE_ID)
                .withTableId(BIGTABLE_TABLEID)
                .build();
        PCollection<Result> btRows = p.apply(Read.from(CloudBigtableIO.read(config)));
        PCollection<TableRow> bqRows = btRows.apply(ParDo.of(new DoFn<Result, TableRow>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                Result result = c.element();
                String postId = new String(result.getRow());
                String user = new String(result.getValue(Bytes.toBytes("post"), Bytes.toBytes("user")),
                        UTF8_CHARSET);
                String message = new String(result.getValue(Bytes.toBytes("post"), Bytes.toBytes("message")),
                        UTF8_CHARSET);
                String lat = new String(result.getValue(Bytes.toBytes("location"), Bytes.toBytes("lat")),
                        UTF8_CHARSET);
                String lon = new String(result.getValue(Bytes.toBytes("location"), Bytes.toBytes("lon")),
                        UTF8_CHARSET);
                TableRow row = new TableRow();
                row.set("postId", postId);
                row.set("user", user);
                row.set("message", message);
                row.set("lat", Double.parseDouble(lat));
                row.set("lon", Double.parseDouble(lon)); c.output(row);
            }
        }));

        List<TableFieldSchema> fields = new ArrayList<>();

        fields.add(new TableFieldSchema().setName("postId").setType("STRING"));
        fields.add(new TableFieldSchema().setName("user").setType("STRING"));
        fields.add(new TableFieldSchema().setName("message").setType("STRING"));
        fields.add(new TableFieldSchema().setName("lat").setType("FLOAT"));
        fields.add(new TableFieldSchema().setName("lon").setType("FLOAT"));
        TableSchema schema = new TableSchema().setFields(fields);
        bqRows.apply(BigQueryIO
                .writeTableRows()
                .to(PROJECT_ID + ":" + BIGQUERY_ID + "." + BIGQUERY_TABLENAME)
                .withSchema(schema)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
        p.run();
    }
}
