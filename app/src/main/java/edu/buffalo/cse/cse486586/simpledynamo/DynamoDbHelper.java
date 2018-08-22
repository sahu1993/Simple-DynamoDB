package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
 * Created by shivamsahu on 4/30/18.
 */

public class DynamoDbHelper extends SQLiteOpenHelper{
    private static final String DATABASE_NAME = "DynamoDB.db";


    private static final int VERSION = 1;

    public DynamoDbHelper(Context context) {
        super(context, DATABASE_NAME, null, VERSION);

    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        final String CREATE_TABLE = "CREATE TABLE "  + DynamoContract.DynamoEntry.TABLE_NAME + " (" +
                DynamoContract.DynamoEntry._ID  + " INTEGER PRIMARY KEY, " +
                DynamoContract.DynamoEntry.COLUMN_KEY + " TEXT NOT NULL UNIQUE, " +
                DynamoContract.DynamoEntry.COLUMN_VALUE    + " TEXT NOT NULL);";
        db.execSQL(CREATE_TABLE);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        db.execSQL("DROP TABLE IF EXISTS " + DynamoContract.DynamoEntry.TABLE_NAME);
        onCreate(db);
    }
}
