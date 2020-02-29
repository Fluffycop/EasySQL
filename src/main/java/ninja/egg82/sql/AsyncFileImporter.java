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
            try {
                importer.readFile(file, lineByLine);
            } catch (SQLException | IOException ex) {
                throw new CompletionException(ex);
            }
        }, importer.sql.exec);
    }

    public CompletableFuture<Void> readResourceAsync(String resourceName, boolean lineByLine) {
        return CompletableFuture.runAsync(() -> {
            try {
                importer.readResource(resourceName, lineByLine);
            } catch (SQLException | IOException ex) {
                throw new CompletionException(ex);
            }
        }, importer.sql.exec);
    }


    public CompletableFuture<Void> readStreamAsync(InputStream stream, boolean lineByLine) {
        return CompletableFuture.runAsync(() -> {
            try {
                importer.readStream(stream, lineByLine);
            } catch (SQLException | IOException ex) {
                throw new CompletionException(ex);
            }
        }, importer.sql.exec);
    }


    public CompletableFuture<Void> readStringAsync(String contents, boolean lineByLine) {
        return CompletableFuture.runAsync(() -> {
            try {
                importer.readString(contents, lineByLine);
            } catch (SQLException | IOException ex) {
                throw new CompletionException(ex);
            }
        }, importer.sql.exec);
    }
}
