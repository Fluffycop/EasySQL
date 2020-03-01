package ninja.egg82.sql;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FileImporter {
    private final Object lineHandlerLock = new Object();
    private StringBuilder lineHandler = new StringBuilder();

    final SQL sql;
    final AsyncFileImporter async;

    private String delimiter = ";";

    private static final Pattern DELIMITER_PATTERN = Pattern.compile("^((--)|(\\/\\*[^!])|#|\\/\\/)*\\s*@?DELIMITER\\s+(.*)$", Pattern.CASE_INSENSITIVE);

    public FileImporter(SQL sql) {
        this.sql = sql;
        this.async = new AsyncFileImporter(this);
    }

    public void readFile(File file, boolean lineByLine) {
        try {
            readStream(new FileInputStream(file), lineByLine);
        }catch(FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public void readResource(String resourceName, boolean lineByLine) { readStream(getClass().getClassLoader().getResourceAsStream(resourceName), lineByLine); }

    public void readStream(InputStream stream, boolean lineByLine) {
        try {
            if (lineByLine) {
                synchronized (lineHandlerLock) {
                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8.name()))) {
                        String line;
                        while ((line = reader.readLine()) != null) {
                            readLine(line);
                        }
                    } catch (UnsupportedEncodingException ignored) {
                    }
                    if (lineHandler.length() > 0) {
                        // Double-check to make sure we didn't catch a few line terminators at the end of the file
                        String last = lineHandler.toString().trim();
                        if (last.length() > 0) {
                            throw new IOException("Missing deliminator at end of file (" + delimiter + ") at '" + lineHandler.toString() + "'.");
                        }
                    }
                }
            } else {
                StringBuilder builder = new StringBuilder();
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8.name()))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        builder.append(line);
                        builder.append('\n');
                    }
                } catch (UnsupportedEncodingException ignored) {
                }
                sql.execute(builder.toString());
            }
        }catch(SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void readString(String contents, boolean lineByLine) {
        if (lineByLine) {
            synchronized (lineHandlerLock) {
                String[] lines = contents.replaceAll("\r\n", "\n").split("\n");
                for (String line : lines) {
                    readLine(line);
                }
                if (lineHandler.length() > 0) {
                    // Double-check to make sure we didn't catch a few line terminators at the end of the file
                    String last = lineHandler.toString().trim();
                    if (last.length() > 0) {
                        throw new RuntimeException("Missing deliminator at end of string (" + delimiter + ") at '" + lineHandler.toString() + "'.");
                    }
                }
            }
        } else {
            try {sql.execute(contents);} catch(SQLException e) {throw new RuntimeException(e);}
        }
    }

    private void readLine(String line) {
        line = line.trim();
        if (line.length() == 0) {
            return;
        }

        if (line.charAt(0) == '#' || (line.startsWith("/*") && !line.startsWith("/*!")) || line.startsWith("--") || line.startsWith("//")) {
            return;
        } else {
            Matcher matcher = DELIMITER_PATTERN.matcher(line);
            if (matcher.matches()) {
                delimiter = matcher.group(4);
            } else if (line.contains(delimiter)) {
                lineHandler.append(line, 0, line.lastIndexOf(delimiter));
                try {sql.execute(lineHandler.toString());} catch(SQLException e) {throw new RuntimeException(e);}
                lineHandler.setLength(0);
                if (line.lastIndexOf(delimiter) + delimiter.length() < line.length() - 1) {
                    lineHandler.append(line.substring(line.lastIndexOf(delimiter) + delimiter.length()));
                    lineHandler.append('\n');
                }
            } else {
                lineHandler.append(line);
                lineHandler.append('\n');
            }
        }
    }
}
