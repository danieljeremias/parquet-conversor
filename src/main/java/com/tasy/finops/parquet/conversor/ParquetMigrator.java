package com.tasy.finops.parquet.conversor;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;

@ApplicationScoped
public class ParquetMigrator {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetMigrator.class);

    @ConfigProperty(name = "source.bucket")
    String sourceBucket;

    @ConfigProperty(name = "source.prefix")
    String sourcePrefix;

    @ConfigProperty(name = "quarkus.s3.aws.region")
    String awsRegion;

    private final S3Client s3;
    private final ParquetTransformer transformer;

    @Inject
    public ParquetMigrator(final S3Client s3, final ParquetTransformer transformer) {
        this.s3 = s3;
        this.transformer = transformer;
    }

    public void execute() {
        validateSourceBucket();

        final List<Future<?>> futures = new ArrayList<>();
        final Semaphore semaphore = new Semaphore(64); // Limit to 64 concurrent threads
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var request = ListObjectsV2Request.builder().bucket(sourceBucket).prefix(sourcePrefix).build();
            s3.listObjectsV2Paginator(request).stream().forEach(page -> {
                for (var object : page.contents()) {
                    if (isNotParquetFile(object.key())) {
                        continue;
                    }
                    futures.add(executor.submit(() -> {
                        semaphore.acquire();
                        try {
                            LOGGER.info("### Processing file {}", object.key());
                            transformer.transform(sourceBucket, object.key());
                        } finally {
                            semaphore.release();
                        }
                        return null;
                    }));
                }
                for (Future<?> future : futures) {
                    try {
                        future.get();
                    } catch (InterruptedException | ExecutionException e) {
                        LOGGER.error("### Error processing file", e);
                        throw new RuntimeException(e);
                    }
                }
            });
        }
    }

    private void validateSourceBucket() {
        try {
            s3.headBucket(b -> b.bucket(sourceBucket));
        } catch (NoSuchBucketException e) {
            throw new IllegalStateException(
                    "Bucket not found: '" + sourceBucket + "' in configured region '" + awsRegion +
                            "'. Verify source.bucket, AWS account/credentials, and region.",
                    e
            );
        }
    }

    private boolean isNotParquetFile(String key) {
        return !key.endsWith(".parquet");
    }

}
