package ninja.egg82.sql;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

public class AsyncFileImporter {
    FileImporter importer;

    public AsyncFileImporter(FileImporter importer) {
        this.importer = importer;
    }

    public CompletableFuture<Void> readFileAsync(File file, boolean lineByLine) {
        return CompletableFuture.runAsync(() -> {
            importer.readFile(file, lineByLine);
        }, importer.sql.exec);
    }

    public CompletableFuture<Void> readResourceAsync(String resourceName, boolean lineByLine) {
        return CompletableFuture.runAsync(() -> {
            importer.readResource(resourceName, lineByLine);
        }, importer.sql.exec);
    }


    public CompletableFuture<Void> readStreamAsync(InputStream stream, boolean lineByLine) {
        return CompletableFuture.runAsync(() -> {
            importer.readStream(stream, lineByLine);
        }, importer.sql.exec);
    }


    public CompletableFuture<Void> readStringAsync(String contents, boolean lineByLine) {
        return CompletableFuture.runAsync(() -> {
            importer.readString(contents, lineByLine);
        }, importer.sql.exec);
    }
}
