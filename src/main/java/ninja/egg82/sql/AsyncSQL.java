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
            try {
                return sql.query(q, params);
            } catch (SQLException ex) {
                throw new CompletionException(ex);
            }
        }, sql.exec);
    }


    public CompletableFuture<SQLQueryResult> query(String q, Map<String, Object> namedParams) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return sql.query(q, namedParams);
            } catch (SQLException ex) {
                throw new CompletionException(ex);
            }
        }, sql.exec);
    }


    public CompletableFuture<SQLExecuteResult> execute(String q, Object... params) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return sql.execute(q, params);
            } catch (SQLException ex) {
                throw new CompletionException(ex);
            }
        }, sql.exec);
    }

    public CompletableFuture<SQLExecuteResult> execute(String q, Map<String, Object> namedParams) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return sql.execute(q, namedParams);
            } catch (SQLException ex) {
                throw new CompletionException(ex);
            }
        }, sql.exec);
    }

    public CompletableFuture<SQLExecuteResult[]> batchExecute(String q, Object[]... params) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return sql.batchExecute(q, params);
            } catch (SQLException ex) {
                throw new CompletionException(ex);
            }
        }, sql.exec);
    }

    public CompletableFuture<SQLExecuteResult[]> batchExecute(String q, Map<String, Object>... namedParams) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return sql.batchExecute(q, namedParams);
            } catch (SQLException ex) {
                throw new CompletionException(ex);
            }
        }, sql.exec);
    }

    public CompletableFuture<SQLQueryResult> call(String q, Object... params) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return sql.call(q, params);
            } catch (SQLException ex) {
                throw new CompletionException(ex);
            }
        }, sql.exec);
    }

    public CompletableFuture<SQLQueryResult> call(String q, Map<String, Object> namedParams) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return sql.call(q, namedParams);
            } catch (SQLException ex) {
                throw new CompletionException(ex);
            }
        }, sql.exec);
    }
}
