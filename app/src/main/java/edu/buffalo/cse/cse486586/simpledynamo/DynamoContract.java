package edu.buffalo.cse.cse486586.simpledynamo;

import android.provider.BaseColumns;

/**
 * Created by shivamsahu on 4/30/18.
 */

public class DynamoContract {
    public static class DynamoEntry implements BaseColumns {

        // Table and Table columns
        public static final String TABLE_NAME = "dynamo";
        public static final String COLUMN_KEY = "key";
        public static final String COLUMN_VALUE = "value";

    }
}
