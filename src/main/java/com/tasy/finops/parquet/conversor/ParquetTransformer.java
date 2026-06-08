package com.tasy.finops.parquet.conversor;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

@ApplicationScoped
public class ParquetTransformer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetTransformer.class);

    private static final DateTimeFormatter OUTPUT_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
            .withZone(java.time.ZoneOffset.UTC);

    private final S3Client s3;

    @ConfigProperty(name = "source.prefix")
    String sourcePrefix;

    @ConfigProperty(name = "target.bucket")
    String targetBucket;

    @ConfigProperty(name = "target.prefix")
    String targetPrefix;

    @Inject
    public ParquetTransformer(final S3Client s3) {
        this.s3 = s3;
    }

    public void transform(String bucket, String key) {
        LOGGER.info("## Processing file {}", key);
        try {
            Path source = Files.createTempFile("source", ".parquet");
            Path target = Files.createTempFile("target", ".parquet");

            try {
                download(bucket, key, source);

                InputFile inputFile = HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(source.toUri()),
                        new org.apache.hadoop.conf.Configuration());

                ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(inputFile).build();
                GenericRecord first = reader.read();
                LOGGER.debug("Reading first line of {}", source);
                if (first == null) {
                    return;
                }

                Schema oldSchema = first.getSchema();
                Schema newSchema = rebuildSchema(oldSchema);
                if (newSchema.toString().contains("\"transaction_id\",\"type\":[\"null\",\"string\"],\"default\":null")) {
                    LOGGER.info("Schema was updated successfully!");
                }

                Files.delete(target);
                OutputFile outputFile = HadoopOutputFile.fromPath(new org.apache.hadoop.fs.Path(target.toUri())
                        , new org.apache.hadoop.conf.Configuration());

                ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(outputFile)
                        .withSchema(newSchema).build();

                processRecord(first, newSchema, writer);

                GenericRecord record;
                while ((record = reader.read()) != null) {
                    processRecord(record, newSchema, writer);
                }

                writer.close();
                reader.close();

                upload(targetBucket, buildTargetKey(key), target);
            } finally {
                cleanupTempFiles(source, target);
            }
        } catch (Exception e) {
            LOGGER.error("## Error processing file {}", key, e);
            throw new RuntimeException(e);
        }
    }

    private void cleanupTempFiles(Path source, Path target) {
        try {
            if (source != null && Files.exists(source)) {
                Files.delete(source);
                LOGGER.debug("Deleted temp source file: {}", source);
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to delete temp source file: {}", source, e);
        }
        try {
            if (target != null && Files.exists(target)) {
                Files.delete(target);
                LOGGER.debug("Deleted temp target file: {}", target);
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to delete temp target file: {}", target, e);
        }
    }

    private String buildTargetKey(String sourceKey) {
        if (sourceKey.startsWith(sourcePrefix)) {
            return targetPrefix + sourceKey.substring(sourcePrefix.length());
        }

        return targetPrefix + sourceKey;
    }

    private void processRecord(GenericRecord record, Schema newSchema, ParquetWriter<GenericRecord> writer) throws Exception {
        LOGGER.debug("Writing new record {}", record.toString());
        GenericRecord newRecord = new GenericData.Record(newSchema);
        for (Schema.Field field : newSchema.getFields()) {
            Object value = record.get(field.name());
            if ("transaction_id".equals(field.name())) {
                if (value != null) {
                    LOGGER.debug("Field transaction_id is not null");
                    value = value.toString();
                }
            }
            if ("metric_date".equals(field.name())) {
                if (value != null) {
                    LOGGER.debug("Normalizing metric_date field");
                    value = normalizeDate(value.toString().toUpperCase());
                }
            }
            newRecord.put(field.name(), value);
        }
        writer.write(newRecord);
    }

    private static String normalizeDate(String value) {
        Instant instant = Instant.parse(value);
        return OUTPUT_FORMAT.format(instant);
    }

    private Schema rebuildSchema(Schema oldSchema) {
        LOGGER.info("Updating file schema for transaction_id field");
        Schema.Parser parser = new Schema.Parser();
        String schemaJson = oldSchema.toString().replace(
                "\"name\":\"transaction_id\",\"type\":[\"null\",\"int\"],\"default\":null",
                "\"name\":\"transaction_id\",\"type\":[\"null\",\"string\"],\"default\":null"
        );
        return parser.parse(schemaJson);
    }

    private void download(String bucket, String key, Path destination) throws Exception {
        LOGGER.info("Downloading parquet file from {} to {}", key, destination);
        ResponseInputStream<GetObjectResponse> stream = s3.getObject(b -> b.bucket(bucket).key(key));
        Files.copy(stream, destination, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
    }

    private void upload(String bucket, String key, Path file) {
        LOGGER.info("Uploading new parquet file to {}", key);
        s3.putObject(b -> b.bucket(bucket).key(key), file);
    }

}
