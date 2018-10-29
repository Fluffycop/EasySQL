package ninja.egg82.core;

public class SQLQueryResult {
    private String[] columns;
    private Object[][] data;

    public SQLQueryResult() {
        this.columns = null;
        this.data = null;
    }

    public SQLQueryResult(String[] columns, Object[][] data) {
        this.columns = columns;
        this.data = data;
    }

    public String[] getColumns() { return columns; }

    public Object[][] getData() { return data; }
}
