package ninja.egg82.sql;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class FileImporter {
    private Map<String, String> cache;

    final SQL sql;

    public FileImporter(SQL sql) {
        this.sql = sql;
        this.cache = Collections.synchronizedMap(new HashMap<>());
    }

    public String load(String key, InputStream stream) {
        StringBuilder builder = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8.name()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                builder.append(line);
                builder.append('\n');
            }
        } catch (UnsupportedEncodingException ignored) {
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        cache.put(key, builder.toString());
        return builder.toString();
    }

    public String get(String key) {
        return cache.get(key);
    }

    public String[] getLineByLine(String key) {
        return cache.get(key).split(";");
    }
}