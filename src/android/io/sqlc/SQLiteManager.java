package io.sqlc;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import java.io.File;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import org.apache.cordova.CallbackContext;

import android.content.Context;
import android.util.Log;



public class SQLiteManager {

    private Context context;

    private SQLiteManager() {

    }

    private static enum SQLiteManagerSingleton {
        INSTANCE;

        SQLiteManager singleton = new SQLiteManager();

        public SQLiteManager getSingleton() {
            return singleton;
        }

    }

    public static SQLiteManager instance() {
        return SQLiteManager.SQLiteManagerSingleton.INSTANCE.getSingleton();
    }

    public boolean needContext() {
        return this.context == null;
    }

    public void setContext(Context context) {
        this.context = context;
    }

    /**
     * Concurrent database runner map.
     *
     * NOTE: no public static accessor to db (runner) map since it is not
     * expected to work properly with db threading.
     *
     * FUTURE TBD put DBRunner into a public class that can provide external accessor.
     *
     * ADDITIONAL NOTE: Storing as Map<String, DBRunner> to avoid portabiity issue
     * between Java 6/7/8 as discussed in:
     * https://gist.github.com/AlainODea/1375759b8720a3f9f094
     *
     * THANKS to @NeoLSN (Jason Yang/楊朝傑) for giving the pointer in:
     * https://github.com/litehelpers/Cordova-sqlite-storage/issues/727
     */
    private Map<String, DBRunner> dbrmap = new ConcurrentHashMap<String, DBRunner>();


    public DBRunner getRunner(String dbname) {
        return dbrmap.get(dbname);
    }

    public void startDatabase(String dbname, JSONObject options, CallbackContext cbc) {
        DBRunner r = dbrmap.get(dbname);

        if (r != null) {
            // NO LONGER EXPECTED due to BUG 666 workaround solution:
            //cbc.error("INTERNAL ERROR: database already open for db name: " + dbname);
            Log.v("info", "Already open: " + dbname);
            if(cbc != null)
                cbc.success();
        } else {
            r = new DBRunner(dbname, options, cbc);
            dbrmap.put(dbname, r);
            Thread tProcess = new Thread(r);
            tProcess.start();
            //this.cordova.getThreadPool().execute(r);
        }
    }

    /**
     * Open a database.
     *
     * @param dbName   The name of the database file
     */
    private SQLiteAndroidDatabase openDatabase(String dbname, CallbackContext cbc, boolean old_impl) throws Exception {
        try {
            // ASSUMPTION: no db (connection/handle) is already stored in the map
            // [should be true according to the code in DBRunner.run()]

            File dbfile = this.context.getDatabasePath(dbname);

            if (!dbfile.exists()) {
                dbfile.getParentFile().mkdirs();
            }

            Log.v("info", "Open sqlite db: " + dbfile.getAbsolutePath());

            SQLiteAndroidDatabase mydb = old_impl ? new SQLiteAndroidDatabase() : new SQLiteConnectorDatabase();
            mydb.open(dbfile);

            if (cbc != null) // XXX Android locking/closing BUG workaround
                cbc.success();

            return mydb;
        } catch (Exception e) {
            if (cbc != null) // XXX Android locking/closing BUG workaround
                cbc.error("can't open database " + e);
            throw e;
        }
    }

    /**
     * Close a database (in another thread).
     *
     * @param dbName   The name of the database file
     */
    public void closeDatabase(String dbname, CallbackContext cbc) {
        DBRunner r = dbrmap.get(dbname);
        if (r != null) {
            try {
                r.q.put(new DBQuery(false, cbc));
            } catch(Exception e) {
                if (cbc != null) {
                    cbc.error("couldn't close database" + e);
                }
                Log.e(SQLiteManager.class.getSimpleName(), "couldn't close database", e);
            }
        } else {
            if (cbc != null) {
                cbc.success();
            }
        }
    }

    /**
     * Close a database (in the current thread).
     *
     * @param dbname   The name of the database file
     */
    private void closeDatabaseNow(String dbname) {
        DBRunner r = dbrmap.get(dbname);

        if (r != null) {
            SQLiteAndroidDatabase mydb = r.mydb;

            if (mydb != null)
                mydb.closeDatabaseNow();
        }
    }

    public void deleteDatabase(String dbname, CallbackContext cbc) {
        DBRunner r = dbrmap.get(dbname);
        if (r != null) {
            try {
                r.q.put(new DBQuery(true, cbc));
            } catch(Exception e) {
                if (cbc != null) {
                    cbc.error("couldn't close database" + e);
                }
                Log.e(SQLiteManager.class.getSimpleName(), "couldn't close database", e);
            }
        } else {
            boolean deleteResult = this.deleteDatabaseNow(dbname);
            if (deleteResult) {
                cbc.success();
            } else {
                cbc.error("couldn't delete database");
            }
        }
    }

    /**
     * Delete a database.
     *
     * @param dbName   The name of the database file
     *
     * @return true if successful or false if an exception was encountered
     */
    public boolean deleteDatabaseNow(String dbname) {
        File dbfile = this.context.getDatabasePath(dbname);

        try {
            return context.deleteDatabase(dbfile.getAbsolutePath());
        } catch (Exception e) {
            Log.e(SQLiteManager.class.getSimpleName(), "couldn't delete database", e);
            return false;
        }
    }

    public void executeSingle(String dbname, String query, JSONArray params, SQLiteAndroidDatabaseCallback cbc) {
            Log.i("CommunicationService", "executeSingle");
            String[] queries = new String[1];
            queries[0] = query;
            JSONArray[] jsonparams = new JSONArray[1];
            if(params != null)
                jsonparams[0] = params;
            else
                jsonparams[0] = new JSONArray();

            // put db query in the queue to be executed in the db thread:
            DBQuery q = new DBQuery(queries, jsonparams, cbc);
            DBRunner r = dbrmap.get(dbname);
            if (r != null) {
                try {
                    Log.i("CommunicationService", "put in runner...");
                    r.q.put(q);
                } catch(Exception e) {
                    Log.e("CommunicationService", "couldn't add to queue", e);
                    Log.e(SQLiteManager.class.getSimpleName(), "couldn't add to queue", e);
                    cbc.error("INTERNAL PLUGIN ERROR: couldn't add to queue");
                }
            } else {
                Log.i("CommunicationService", "runner not found...");
                cbc.error("INTERNAL PLUGIN ERROR: database not open");
            }
    }

    public void executeSqlBatch(JSONArray args, CallbackContext cbc) throws JSONException {
        JSONObject allargs = args.getJSONObject(0);
        JSONObject dbargs = allargs.getJSONObject("dbargs");
        String dbname = dbargs.getString("dbname");
        JSONArray txargs = allargs.getJSONArray("executes");

        if (txargs.isNull(0)) {
            cbc.error("INTERNAL PLUGIN ERROR: missing executes list");
        } else {
            int len = txargs.length();
            String[] queries = new String[len];
            JSONArray[] jsonparams = new JSONArray[len];

            for (int i = 0; i < len; i++) {
                JSONObject a = txargs.getJSONObject(i);
                queries[i] = a.getString("sql");
                jsonparams[i] = a.getJSONArray("params");
            }

            // put db query in the queue to be executed in the db thread:
            DBQuery q = new DBQuery(queries, jsonparams, cbc);
            DBRunner r = dbrmap.get(dbname);
            if (r != null) {
                try {
                    r.q.put(q);
                } catch(Exception e) {
                    Log.e(SQLiteManager.class.getSimpleName(), "couldn't add to queue", e);
                    cbc.error("INTERNAL PLUGIN ERROR: couldn't add to queue");
                }
            } else {
                cbc.error("INTERNAL PLUGIN ERROR: database not open");
            }
        }
    }

    @Override protected void finalize() throws Throwable {
        while (!dbrmap.isEmpty()) {
            String dbname = dbrmap.keySet().iterator().next();

            this.closeDatabaseNow(dbname);

            DBRunner r = dbrmap.get(dbname);
            try {
                // stop the db runner thread:
                r.q.put(new DBQuery());
            } catch(Exception e) {
                Log.e(SQLiteManager.class.getSimpleName(), "INTERNAL PLUGIN CLEANUP ERROR: could not stop db thread due to exception", e);
            }
            dbrmap.remove(dbname);
        }
        super.finalize();
    }

    private class DBRunner implements Runnable {
        final String dbname;
        private boolean oldImpl;
        private boolean bugWorkaround;

        final BlockingQueue<DBQuery> q;
        final CallbackContext openCbc;

        SQLiteAndroidDatabase mydb;

        DBRunner(final String dbname, JSONObject options, CallbackContext cbc) {
            this.dbname = dbname;
            this.oldImpl = options.has("androidOldDatabaseImplementation");
            Log.v(SQLiteManager.class.getSimpleName(), "Android db implementation: built-in android.database.sqlite package");
            this.bugWorkaround = this.oldImpl && options.has("androidBugWorkaround");
            if (this.bugWorkaround)
                Log.v(SQLiteManager.class.getSimpleName(), "Android db closing/locking workaround applied");

            this.q = new LinkedBlockingQueue<DBQuery>();
            this.openCbc = cbc;
        }

        public void run() {
            try {
                this.mydb = openDatabase(dbname, this.openCbc, this.oldImpl);
            } catch (Exception e) {
                Log.e(SQLiteManager.class.getSimpleName(), "unexpected error, stopping db thread", e);
                dbrmap.remove(dbname);
                return;
            }

            DBQuery dbq = null;

            try {
                dbq = q.take();

                while (!dbq.stop) {
                    if(dbq.cbc != null)
                        mydb.executeSqlBatch(dbq.queries, dbq.jsonparams, dbq.cbc);
                    else {
                        Log.i("CommunicationService", "runner execute");
                        mydb.executeSqlBatch(dbq.queries, dbq.jsonparams, dbq.adbc);
                    }
                    if (this.bugWorkaround && dbq.queries.length == 1 && dbq.queries[0] == "COMMIT")
                        mydb.bugWorkaround();

                    dbq = q.take();
                }
            } catch (Exception e) {
                Log.e(SQLiteManager.class.getSimpleName(), "unexpected error", e);
            }

            if (dbq != null && dbq.close) {
                try {
                    closeDatabaseNow(dbname);

                    dbrmap.remove(dbname); // (should) remove ourself

                    if (!dbq.delete) {
                        dbq.cbc.success();
                    } else {
                        try {
                            boolean deleteResult = deleteDatabaseNow(dbname);
                            if (deleteResult) {
                                dbq.cbc.success();
                            } else {
                                dbq.cbc.error("couldn't delete database");
                            }
                        } catch (Exception e) {
                            Log.e(SQLiteManager.class.getSimpleName(), "couldn't delete database", e);
                            dbq.cbc.error("couldn't delete database: " + e);
                        }
                    }
                } catch (Exception e) {
                    Log.e(SQLiteManager.class.getSimpleName(), "couldn't close database", e);
                    if (dbq.cbc != null) {
                        dbq.cbc.error("couldn't close database: " + e);
                    }
                }
            }
        }
    }

    private final class DBQuery {
        // XXX TODO replace with DBRunner action enum:
        final boolean stop;
        final boolean close;
        final boolean delete;
        final String[] queries;
        final JSONArray[] jsonparams;
        final CallbackContext cbc;
        final SQLiteAndroidDatabaseCallback adbc;

        DBQuery(String[] myqueries, JSONArray[] params, CallbackContext c) {
            this.stop = false;
            this.close = false;
            this.delete = false;
            this.queries = myqueries;
            this.jsonparams = params;
            this.cbc = c;
            this.adbc = null;
        }

        DBQuery(String[] myqueries, JSONArray[] params, SQLiteAndroidDatabaseCallback adbc) {
            this.stop = false;
            this.close = false;
            this.delete = false;
            this.queries = myqueries;
            this.jsonparams = params;
            this.cbc = null;
            this.adbc = adbc;
        }

        DBQuery(boolean delete, CallbackContext cbc) {
            this.stop = true;
            this.close = true;
            this.delete = delete;
            this.queries = null;
            this.jsonparams = null;
            this.cbc = cbc;
            this.adbc = null;
        }

        // signal the DBRunner thread to stop:
        DBQuery() {
            this.stop = true;
            this.close = false;
            this.delete = false;
            this.queries = null;
            this.jsonparams = null;
            this.cbc = null;
            this.adbc = null;
        }
    }

}