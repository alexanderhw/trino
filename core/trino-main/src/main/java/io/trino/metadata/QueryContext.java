package io.trino.metadata;

import io.trino.execution.QueryInfo;

public class QueryContext {

    private boolean fromGrafana;

    private QueryInfo queryInfo;

    public boolean isFromGrafana() {
        return fromGrafana;
    }

    public void setFromGrafana(boolean fromGrafana) {
        this.fromGrafana = fromGrafana;
    }

    public QueryInfo getQueryInfo() {
        return queryInfo;
    }

    public void setQueryInfo(QueryInfo queryInfo) {
        this.queryInfo = queryInfo;
    }
}
