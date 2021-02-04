package io.sqlc;

import org.json.JSONArray;

public interface SQLiteAndroidDatabaseCallback {
    public void error(String error);
    public void success(JSONArray arr);
}