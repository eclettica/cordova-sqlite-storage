/*
 * Copyright (c) 2012-present Christopher J. Brody (aka Chris Brody)
 * Copyright (c) 2005-2010, Nitobi Software Inc.
 * Copyright (c) 2010, IBM Corporation
 */

package io.sqlc;

import android.util.Log;

import java.io.File;

import java.lang.IllegalArgumentException;
import java.lang.Number;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.cordova.CallbackContext;
import org.apache.cordova.CordovaPlugin;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import io.sqlc.SQLiteManager;

public class SQLitePlugin extends CordovaPlugin {


    private SQLiteManager sqliteManager = SQLiteManager.instance();

    /**
     * NOTE: Using default constructor, no explicit constructor.
     */

    /**
     * Executes the request and returns PluginResult.
     *
     * @param actionAsString The action to execute.
     * @param args   JSONArry of arguments for the plugin.
     * @param cbc    Callback context from Cordova API
     * @return       Whether the action was valid.
     */
    @Override
    public boolean execute(String actionAsString, JSONArray args, CallbackContext cbc) {

        if(sqliteManager.needContext()) {
            sqliteManager.setContext(this.cordova.getActivity());
        }

        Action action;
        try {
            action = Action.valueOf(actionAsString);
        } catch (IllegalArgumentException e) {
            // shouldn't ever happen
            Log.e(SQLitePlugin.class.getSimpleName(), "unexpected error", e);
            return false;
        }

        try {
            return executeAndPossiblyThrow(action, args, cbc);
        } catch (JSONException e) {
            // TODO: signal JSON problem to JS
            Log.e(SQLitePlugin.class.getSimpleName(), "unexpected error", e);
            return false;
        }
    }

    private boolean executeAndPossiblyThrow(Action action, JSONArray args, CallbackContext cbc)
            throws JSONException {

        boolean status = true;
        JSONObject o;
        String echo_value;
        String dbname;

        switch (action) {
            case echoStringValue:
                o = args.getJSONObject(0);
                echo_value = o.getString("value");
                cbc.success(echo_value);
                break;

            case open:
                o = args.getJSONObject(0);
                dbname = o.getString("name");
                // open database and start reading its queue
                sqliteManager.startDatabase(dbname, o, cbc);
                break;

            case close:
                o = args.getJSONObject(0);
                dbname = o.getString("path");
                // put request in the q to close the db
                sqliteManager.closeDatabase(dbname, cbc);
                break;

            case delete:
                o = args.getJSONObject(0);
                dbname = o.getString("path");

                sqliteManager.deleteDatabase(dbname, cbc);

                break;

            case executeSqlBatch:
            case backgroundExecuteSqlBatch:
                sqliteManager.executeSqlBatch(args, cbc);
                break;
        }

        return status;
    }

    /**
     * Clean up and close all open databases.
     */
    @Override
    public void onDestroy() {

    }

    // --------------------------------------------------------------------------
    // LOCAL METHODS
    // --------------------------------------------------------------------------




    private static enum Action {
        echoStringValue,
        open,
        close,
        delete,
        executeSqlBatch,
        backgroundExecuteSqlBatch,
    }
}

/* vim: set expandtab : */
