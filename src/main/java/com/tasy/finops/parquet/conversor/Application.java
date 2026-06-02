package com.tasy.finops.parquet.conversor;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import jakarta.inject.Inject;

@QuarkusMain
public class Application implements QuarkusApplication {

    private final ParquetMigrator migrator;

    @Inject
    public Application(final ParquetMigrator migrator) {
        this.migrator = migrator;
    }

    @Override
    public int run(String... args) throws Exception {
        migrator.execute();
        Quarkus.asyncExit();
        return 0;
    }

    public static void main(String[] args) {
        Quarkus.run(Application.class, args);
    }

}