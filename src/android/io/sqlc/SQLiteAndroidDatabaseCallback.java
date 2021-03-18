package io.sqlc;

import org.json.JSONArray;
import java.lang.RuntimeException;

public abstract class SQLiteAndroidDatabaseCallback {
    public abstract void error(String error);
    public abstract void success(JSONArray arr);

    public void successObj(Object obj) {
        throw new RuntimeException("Method not implemented!");
    }
}