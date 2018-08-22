package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
 * Created by shivamsahu on 5/6/18.
 */

public class FailureDbHelper extends SQLiteOpenHelper {
    private static final String DATABASE_NAME = "FailureDB.db";


    private static final int VERSION = 1;

    public FailureDbHelper(Context context) {
        super(context, DATABASE_NAME, null, VERSION);

    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        final String CREATE_TABLE = "CREATE TABLE failureDyanmoTable (_ID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE, key TEXT)";
        db.execSQL(CREATE_TABLE);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        db.execSQL("DROP TABLE IF EXISTS " + "failureDyanmoTable");
        onCreate(db);
    }
}
