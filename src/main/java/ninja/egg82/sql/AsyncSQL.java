package ninja.egg82.sql;

import ninja.egg82.core.SQLExecuteResult;
import ninja.egg82.core.SQLQueryResult;

import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

public class AsyncSQL {
    SQL sql;


    AsyncSQL(SQL sql) {
        this.sql = sql;
    }

    public CompletableFuture<Boolean> tableExists(String schema, String table) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return sql.tableExists(schema, table);
            } catch (SQLException ex) {
                throw new CompletionException(ex);
            }
        }, sql.exec);
    }

    public CompletableFuture<SQLQueryResult> query(String q, Object... params) {
        return CompletableFuture.supplyAsync(() -> {
            return sql.query(q, params);
        }, sql.exec);
    }


    public CompletableFuture<SQLQueryResult> query(String q, Map<String, Object> namedParams) {
        return CompletableFuture.supplyAsync(() -> {
            return sql.query(q, namedParams);
        }, sql.exec);
    }


    public CompletableFuture<SQLExecuteResult> execute(String q, Object... params) {
        return CompletableFuture.supplyAsync(() -> {
            return sql.execute(q, params);
        }, sql.exec);
    }

    public CompletableFuture<SQLExecuteResult> execute(String q, Map<String, Object> namedParams) {
        return CompletableFuture.supplyAsync(() -> {
            return sql.execute(q, namedParams);
        }, sql.exec);
    }

    public CompletableFuture<SQLExecuteResult[]> batchExecute(String q, Object[]... params) {
        return CompletableFuture.supplyAsync(() -> {
            return sql.batchExecute(q, params);
        }, sql.exec);
    }

    public CompletableFuture<SQLExecuteResult[]> batchExecute(String q, Map<String, Object>... namedParams) {
        return CompletableFuture.supplyAsync(() -> {
            return sql.batchExecute(q, namedParams);
        }, sql.exec);
    }

    public CompletableFuture<SQLQueryResult> call(String q, Object... params) {
        return CompletableFuture.supplyAsync(() -> {
            return sql.call(q, params);
        }, sql.exec);
    }

    public CompletableFuture<SQLQueryResult> call(String q, Map<String, Object> namedParams) {
        return CompletableFuture.supplyAsync(() -> {
            return sql.call(q, namedParams);
        }, sql.exec);
    }

    public CompletableFuture<SQLQueryResult[]> querySeparately(String[] q, Object... params) {
        return CompletableFuture.supplyAsync(() -> sql.querySeperately(q, params), sql.exec);
    }

    public CompletableFuture<SQLExecuteResult[]> executeSeparately(String[] q, Object... params) {
        return CompletableFuture.supplyAsync(() -> sql.executeSeparately(q, params), sql.exec);
    }
}
