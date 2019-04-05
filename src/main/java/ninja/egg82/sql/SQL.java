package ninja.egg82.sql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.Properties;
import ninja.egg82.core.NamedParameterStatement;
import ninja.egg82.core.SQLExecuteResult;
import ninja.egg82.core.SQLQueryResult;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

public class SQL implements AutoCloseable {
    private HikariDataSource source;

    public SQL(HikariConfig config) { source = new HikariDataSource(config); }

    public SQL(Properties properties) { source = new HikariDataSource(new HikariConfig(properties)); }

    public SQL(String propertiesFile) { source = new HikariDataSource(new HikariConfig(propertiesFile)); }

    public SQL(String connectionString, String user, String pass) {
        source = new HikariDataSource();
        source.setJdbcUrl(connectionString);
        source.setUsername(user);
        source.setPassword(pass);
        source.setAutoCommit(true);
    }

    public void close() { source.close(); }

    public boolean isClosed() { return source.isClosed(); }

    public boolean isRunning() { return source.isRunning(); }

    public SQLQueryResult query(String q, Object... params) throws SQLException {
        try (Connection connection = source.getConnection(); PreparedStatement statement = connection.prepareStatement(q)) {
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    statement.setObject(i + 1, params[i]);
                }
            }

            SQLQueryResult result = query(statement);
            if (!source.isAutoCommit()) {
                connection.commit();
            }
            return result;
        }
    }

    public SQLQueryResult query(String q, Map<String, Object> namedParams) throws SQLException {
        try (Connection connection = source.getConnection(); NamedParameterStatement statement = new NamedParameterStatement(connection, q)) {
            if (namedParams != null) {
                for (Map.Entry<String, Object> kvp : namedParams.entrySet()) {
                    statement.setObject(kvp.getKey(), kvp.getValue());
                }
            }

            SQLQueryResult result = query(statement.getPreparedStatement());
            if (!source.isAutoCommit()) {
                connection.commit();
            }
            return result;
        }
    }

    public CompletableFuture<SQLQueryResult> queryAsync(String q, Object... params) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return query(q, params);
            } catch (SQLException ex) {
                throw new CompletionException(ex);
            }
        });
    }

    public CompletableFuture<SQLQueryResult> queryAsync(String q, Map<String, Object> namedParams) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return query(q, namedParams);
            } catch (SQLException ex) {
                throw new CompletionException(ex);
            }
        });
    }

    public SQLExecuteResult execute(String q, Object... params) throws SQLException {
        try (Connection connection = source.getConnection(); PreparedStatement statement = connection.prepareStatement(q)) {
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    statement.setObject(i + 1, params[i]);
                }
            }

            SQLExecuteResult result = execute(statement);
            if (!source.isAutoCommit()) {
                connection.commit();
            }
            return result;
        }
    }

    public SQLExecuteResult execute(String q, Map<String, Object> namedParams) throws SQLException {
        try (Connection connection = source.getConnection(); NamedParameterStatement statement = new NamedParameterStatement(connection, q)) {
            if (namedParams != null) {
                for (Map.Entry<String, Object> kvp : namedParams.entrySet()) {
                    statement.setObject(kvp.getKey(), kvp.getValue());
                }
            }

            SQLExecuteResult result = execute(statement.getPreparedStatement());
            if (!source.isAutoCommit()) {
                connection.commit();
            }
            return result;
        }
    }

    public CompletableFuture<SQLExecuteResult> executeAsync(String q, Object... params) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return execute(q, params);
            } catch (SQLException ex) {
                throw new CompletionException(ex);
            }
        });
    }

    public CompletableFuture<SQLExecuteResult> executeAsync(String q, Map<String, Object> namedParams) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return execute(q, namedParams);
            } catch (SQLException ex) {
                throw new CompletionException(ex);
            }
        });
    }

    public SQLExecuteResult[] batchExecute(String q, Object[]... params) throws SQLException {
        try (Connection connection = source.getConnection(); PreparedStatement statement = connection.prepareStatement(q)) {
            if (params != null) {
                for (Object[] p : params) {
                    if (p != null) {
                        for (int i = 0; i < p.length; i++) {
                            statement.setObject(i + 1, p[i]);
                        }
                        statement.addBatch();
                    }
                }
            }

            SQLExecuteResult[] result = executeBatch(statement);
            if (!source.isAutoCommit()) {
                connection.commit();
            }
            return result;
        }
    }

    public SQLExecuteResult[] batchExecute(String q, Map<String, Object>... namedParams) throws SQLException {
        try (Connection connection = source.getConnection(); NamedParameterStatement statement = new NamedParameterStatement(connection, q)) {
            if (namedParams != null) {
                for (Map<String, Object> p : namedParams) {
                    if (p != null) {
                        for (Map.Entry<String, Object> kvp : p.entrySet()) {
                            statement.setObject(kvp.getKey(), kvp.getValue());
                        }
                        statement.addBatch();
                    }
                }
            }

            SQLExecuteResult[] result = executeBatch(statement.getPreparedStatement());
            if (!source.isAutoCommit()) {
                connection.commit();
            }
            return result;
        }
    }

    public CompletableFuture<SQLExecuteResult[]> batchExecuteAsync(String q, Object[]... params) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return batchExecute(q, params);
            } catch (SQLException ex) {
                throw new CompletionException(ex);
            }
        });
    }

    public CompletableFuture<SQLExecuteResult[]> batchExecuteAsync(String q, Map<String, Object>... namedParams) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return batchExecute(q, namedParams);
            } catch (SQLException ex) {
                throw new CompletionException(ex);
            }
        });
    }

    private SQLQueryResult query(PreparedStatement statement) throws SQLException {
        boolean hasResults = statement.execute();

        if (!hasResults) {
            return new SQLQueryResult();
        }

        List<String> columns = new ArrayList<>();
        List<Object[]> rows = new ArrayList<>();

        try (ResultSet results = statement.getResultSet()) {
            ResultSetMetaData meta = results.getMetaData();

            for (int i = 1; i <= meta.getColumnCount(); i++) {
                columns.add(meta.getColumnName(i));
            }

            collectRows(results, rows, columns.size());
        }

        while (statement.getMoreResults()) {
            try (ResultSet results = statement.getResultSet()) {
                collectRows(results, rows, columns.size());
            }
        }

        return new SQLQueryResult(columns.toArray(new String[0]), rows.toArray(new Object[0][]));
    }

    private void collectRows(ResultSet results, List<Object[]> rows, int columnCount) throws SQLException {
        while (results.next()) {
            Object[] tVals = new Object[columnCount];
            for (int i = 0; i < columnCount; i++) {
                tVals[i] = results.getObject(i + 1);
            }
            rows.add(tVals);
        }
    }

    private SQLExecuteResult execute(PreparedStatement statement) throws SQLException {
        boolean hasResults = statement.execute();

        if (hasResults) {
            return new SQLExecuteResult(-1);
        }

        return new SQLExecuteResult(statement.getUpdateCount());
    }

    private SQLExecuteResult[] executeBatch(PreparedStatement statement) throws SQLException {
        int[] results = statement.executeBatch();
        statement.clearBatch();

        SQLExecuteResult[] retVal = new SQLExecuteResult[results.length];
        for (int i = 0; i < results.length; i++) {
            retVal[i] = new SQLExecuteResult(results[i]);
        }

        return retVal;
    }
}