package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.concurrent.Semaphore;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.nfc.Tag;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.database.sqlite.SQLiteDatabase.CONFLICT_REPLACE;

public class SimpleDynamoProvider extends ContentProvider {

	private DynamoDbHelper mDynamoDbHelper;
	private FailureDbHelper mFailureDbHelper;
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	String myPort;
	ArrayList<Nodes> nodeArrayList = null;
	static final int SERVER_PORT = 10000;
	Nodes MyNode,Node_5554,Node_5556,Node_5558,Node_5560,Node_5562,predecessorNode,predecessorNode2,successorNode,successorNode2;
	private final Semaphore secondLock = new Semaphore(1, true);
	private final Semaphore firstLock = new Semaphore(1, true);
	MatrixCursor cursorSingleRecord;
	MatrixCursor cursorALLRecord;
	Boolean inRecovery = false;
	ServerSocket serverSocket;


	@Override
	public boolean onCreate() {
		try {
			firstLock.acquire();
		} catch (InterruptedException e) {
			Log.e(TAG, "Sync Exception");
		}
		mDynamoDbHelper = new DynamoDbHelper(getContext());
		mFailureDbHelper = new FailureDbHelper(getContext());
		final SQLiteDatabase failureDb = mFailureDbHelper.getReadableDatabase();
		Log.v(TAG, "Started server");
		try {
			serverSocket = new ServerSocket(SERVER_PORT);
			serverSocket.setReuseAddress(true);
		} catch (IOException e) {
			Log.v(TAG,"onCreate serverSocekt IOException: "+e);
		}
		TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		try {
			MyNode = new Nodes(myPort, genHash(String.valueOf(Integer.parseInt(myPort)/2)));
			Node_5562 = new Nodes(String.valueOf(11124), genHash(String.valueOf(5562)));
			Node_5556 = new Nodes(String.valueOf(11112), genHash(String.valueOf(5556)));
			Node_5554 = new Nodes(String.valueOf(11108), genHash(String.valueOf(5554)));
			Node_5558 = new Nodes(String.valueOf(11116), genHash(String.valueOf(5558)));
			Node_5560 = new Nodes(String.valueOf(11120), genHash(String.valueOf(5560)));

			nodeArrayList = new ArrayList<Nodes>(10);

			nodeArrayList.add(Node_5562);
			nodeArrayList.add(Node_5556);
			nodeArrayList.add(Node_5554);
			nodeArrayList.add(Node_5558);
			nodeArrayList.add(Node_5560);

			if (Integer.parseInt(myPort) == 11124) {
				predecessorNode2 = Node_5558;
				predecessorNode = Node_5560;
				successorNode = Node_5556;
				successorNode2 = Node_5554;
			}else if(Integer.parseInt(myPort) == 11112){
				predecessorNode2 = Node_5560;
				predecessorNode = Node_5562;
				successorNode = Node_5554;
				successorNode2 = Node_5558;
			}else if(Integer.parseInt(myPort) == 11108){
				predecessorNode2 = Node_5562;
				predecessorNode = Node_5556;
				successorNode = Node_5558;
				successorNode2 = Node_5560;
			}else if(Integer.parseInt(myPort) == 11116){
				predecessorNode2 = Node_5556;
				predecessorNode = Node_5554;
				successorNode = Node_5560;
				successorNode2 = Node_5562;
			}else if(Integer.parseInt(myPort) == 11120){
				predecessorNode2 = Node_5554;
				predecessorNode = Node_5558;
				successorNode = Node_5562;
				successorNode2 = Node_5556;
			}
			Log.v(TAG, "predecessorNode" + predecessorNode.getPort());
		} catch (NoSuchAlgorithmException e) {
			Log.v(TAG, "" + e);
		}
		new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		ContentValues failureTestKeyVal = new ContentValues();
		failureTestKeyVal.put("key", "test");
		long insertResult = -1;
		try {
			insertResult = failureDb.insertOrThrow("failureDyanmoTable", null, failureTestKeyVal);
		} catch (SQLException e) {
			Log.e(TAG, "SQLException in onCreate", e);
		}
		Log.v(TAG,"insertResult: "+insertResult);
		if(insertResult == 1){
			firstLock.release();
		}else{
			startRecovery();
		}
		return true;
	}

	public void startRecovery() {

		Log.v(TAG,"inside startRecovery");
		inRecovery = true;
		try {
			secondLock.acquire();
			String msgToSend = "QueryRecoveryPre" + "##" + myPort.trim() + "##" + predecessorNode2.getPort().trim();
			//String msgToSend = "Insert"+"##"+myPort.trim()+"##"+successorNodePort+"##"+key+"##"+value;
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, predecessorNode2.getPort().trim());


			secondLock.acquire();
			msgToSend = "QueryRecoveryPre" + "##" + myPort.trim() + "##" + predecessorNode.getPort().trim();
			//String msgToSend = "Insert"+"##"+myPort.trim()+"##"+successorNodePort+"##"+key+"##"+value;
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, predecessorNode.getPort().trim());

			secondLock.acquire();
			msgToSend = "QueryRecoverySuc" + "##" + myPort.trim() + "##" + successorNode.getPort().trim();
			//String msgToSend = "Insert"+"##"+myPort.trim()+"##"+successorNodePort+"##"+key+"##"+value;
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, successorNode.getPort().trim());

			secondLock.acquire();
			secondLock.release();
			inRecovery = false;
			firstLock.release();
		}catch (InterruptedException e){
			Log.v(TAG, "In Insert  InterruptedException: ");
			e.printStackTrace();
		}
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		try {
			firstLock.acquire();
		} catch (InterruptedException e) {
			Log.e(TAG, "Sync Exception");
		}
		final SQLiteDatabase db = mDynamoDbHelper.getWritableDatabase();
		int result;
		if(selection.equals("@")) {   // AVD local
			result = db.delete(DynamoContract.DynamoEntry.TABLE_NAME, null, null);
			firstLock.release();
			return result;
		}else if(selection.equals("*")) { // AVD local(bcz only one avd)
			result = db.delete(DynamoContract.DynamoEntry.TABLE_NAME, null, null);
			firstLock.release();
			return result;
		}else{
			String key = selection;
			Nodes preNode;
			Nodes node = nodeArrayList.get(0);
			Nodes successorDelete1;
			Nodes successorDelete2;
			int i ;
			for(i = 0; i < nodeArrayList.size(); i++){
				node = nodeArrayList.get(i);
				if(i == 0){
					preNode = nodeArrayList.get(nodeArrayList.size()-1);
				}else{
					preNode = nodeArrayList.get(i-1);
				}

				if(InRange(key, preNode, node)) {
					break;
				}
			}
			if(i == 4){
				successorDelete1 = nodeArrayList.get(0);
				successorDelete2 = nodeArrayList.get(1);
			}else if(i == 3){
				successorDelete1 = nodeArrayList.get(i+1);
				successorDelete2 = nodeArrayList.get(0);
			}else{
				successorDelete1 = nodeArrayList.get(i+1);
				successorDelete2 = nodeArrayList.get(i+2);
			}
			try {
				Log.v(TAG,"i: "+i);
				secondLock.acquire();
				String msgToSend = "Delete"+"##"+myPort.trim()+"##"+node.getPort().trim()+"##"+key;
				//String msgToSend = "Insert"+"##"+myPort.trim()+"##"+successorNodePort+"##"+key+"##"+value;
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, node.getPort().trim());


				secondLock.acquire();
				msgToSend = "Delete"+"##"+myPort.trim()+"##"+successorDelete1.getPort().trim()+"##"+key;
				//String msgToSend = "Insert"+"##"+myPort.trim()+"##"+successorNodePort+"##"+key+"##"+value;
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, successorDelete1.getPort().trim());

				secondLock.acquire();
				msgToSend = "Delete"+"##"+myPort.trim()+"##"+successorDelete2.getPort().trim()+"##"+key;
				//String msgToSend = "Insert"+"##"+myPort.trim()+"##"+successorNodePort+"##"+key+"##"+value;
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, successorDelete2.getPort().trim());

				secondLock.acquire();
				secondLock.release();
				firstLock.release();
			}catch (InterruptedException e){
				Log.v(TAG, "In Insert  InterruptedException: ");
				e.printStackTrace();
			}

		}
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		try {
			firstLock.acquire();
		} catch (InterruptedException e) {
			Log.e(TAG, "Sync Exception");
		}
		String key = values.getAsString("key").trim();
		String value = values.getAsString("value").trim();
		Nodes preNode;
		Nodes node = nodeArrayList.get(0);
		Nodes successorInsert1;
		Nodes successorInsert2;
		int i ;
		for(i = 0; i < nodeArrayList.size(); i++){
			node = nodeArrayList.get(i);
			if(i == 0){
				 preNode = nodeArrayList.get(nodeArrayList.size()-1);
			}else{
				 preNode = nodeArrayList.get(i-1);
			}

			if(InRange(key, preNode, node)) {
				break;
			}
		}
		if(i == 4){
			successorInsert1 = nodeArrayList.get(0);
			successorInsert2 = nodeArrayList.get(1);
		}else if(i == 3){
			successorInsert1 = nodeArrayList.get(i+1);
			successorInsert2 = nodeArrayList.get(0);
		}else{
			successorInsert1 = nodeArrayList.get(i+1);
			successorInsert2 = nodeArrayList.get(i+2);
		}
		try {
			Log.v(TAG,"i: "+i);
			secondLock.acquire();
			String msgToSend = "Insert"+"##"+myPort.trim()+"##"+node.getPort().trim()+"##"+key+"##"+value;
			//String msgToSend = "Insert"+"##"+myPort.trim()+"##"+successorNodePort+"##"+key+"##"+value;
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, node.getPort().trim());


			secondLock.acquire();
			msgToSend = "Insert"+"##"+myPort.trim()+"##"+successorInsert1.getPort().trim()+"##"+key+"##"+value;
			//String msgToSend = "Insert"+"##"+myPort.trim()+"##"+successorNodePort+"##"+key+"##"+value;
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, successorInsert1.getPort().trim());

			secondLock.acquire();
			msgToSend = "Insert"+"##"+myPort.trim()+"##"+successorInsert2.getPort().trim()+"##"+key+"##"+value;
			//String msgToSend = "Insert"+"##"+myPort.trim()+"##"+successorNodePort+"##"+key+"##"+value;
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, successorInsert2.getPort().trim());

			secondLock.acquire();
			secondLock.release();
			firstLock.release();
			return uri;

		}catch (InterruptedException e){
			Log.v(TAG, "In Insert  InterruptedException: ");
			e.printStackTrace();
		}

		return null;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
		try {
			firstLock.acquire();
		} catch (InterruptedException e) {
			Log.e(TAG, "Sync Exception");
		}
		final SQLiteDatabase db = mDynamoDbHelper.getReadableDatabase();
		Cursor result;
		Log.v(TAG,"Selection Key == "+selection);
		Nodes preNode;
		Socket sockets;
		Nodes successorQuery2;
		Nodes successorQuery1;
		Nodes node = nodeArrayList.get(0);
		if(selection.equals("@")){   // AVD local

			result = db.query(DynamoContract.DynamoEntry.TABLE_NAME, projection, null,
					null, null, null, sortOrder);
			result.moveToNext();
			Log.v("query", selection);
			firstLock.release();
			return result;
		}else if(selection.equals("*")){
			cursorALLRecord = new MatrixCursor(new String[]{"key","value"});
			try {
				int i;
				for (i = 0; i < nodeArrayList.size(); i++) {
					secondLock.acquire();
					String msgToSend = "AllQueryFind" + "##" + myPort.trim() + "##" + nodeArrayList.get(i).getPort().trim() + "##" + selection;
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, nodeArrayList.get(i).getPort().trim());
				}
				secondLock.acquire();
				secondLock.release();
				firstLock.release();
				return cursorALLRecord;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}else{
			int i ;
			for(i = 0; i < nodeArrayList.size(); i++){
				node = nodeArrayList.get(i);
				if(i == 0){
					preNode = nodeArrayList.get(nodeArrayList.size()-1);
				}else{
					preNode = nodeArrayList.get(i-1);
				}

				if(InRange(selection, preNode, node)) {
					break;
				}
			}
			if(i == 4){
				successorQuery1  = nodeArrayList.get(0);
				successorQuery2 = nodeArrayList.get(1);
			}else if(i == 3){
				successorQuery1 = nodeArrayList.get(4);
				successorQuery2 = nodeArrayList.get(0);
			}else{
				successorQuery1 = nodeArrayList.get(i+1);
				successorQuery2 = nodeArrayList.get(i+2);
			}
			try{
				Log.v(TAG,"query i: "+i);
				cursorSingleRecord = new MatrixCursor(new String[]{"key","value"},1);
				//Log.v(TAG,"Count of cursor before: "+cursorSingleRecord.getCount());
				secondLock.acquire();
				String msgToSend = "SingleQueryFind"+"##"+myPort.trim()+"##"+node.getPort().trim()+"##"+selection;
				//String msgToSend = "Insert"+"##"+myPort.trim()+"##"+successorNodePort+"##"+key+"##"+value;
				new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, node.getPort().trim());
				secondLock.acquire();
				if(cursorSingleRecord.getCount() == 1){
					secondLock.release();
				}else{
					msgToSend = "SingleQueryFind"+"##"+myPort.trim()+"##"+successorQuery1.getPort().trim()+"##"+selection;
					//String msgToSend = "Insert"+"##"+myPort.trim()+"##"+successorNodePort+"##"+key+"##"+value;
					new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msgToSend, successorQuery1.getPort().trim());
					secondLock.acquire();
					secondLock.release();
				}
				firstLock.release();
				//Log.v(TAG,"Count of cursor after: "+cursorSingleRecord.getCount());
				return cursorSingleRecord;
			}catch (Exception e){

			}
		}
		return null;
	}

	private boolean InRange(String key, Nodes preNode, Nodes node) {
		try {
			String hashedKey = genHash(key);
			if(preNode.ID.compareTo(node.ID) > 0 &&  hashedKey.compareTo(node.ID) <= 0 && hashedKey.compareTo(preNode.ID) < 0 ||
					preNode.ID.compareTo(node.ID) > 0 && hashedKey.compareTo(preNode.ID) > 0 && hashedKey.compareTo(node.ID) > 0 ||
					hashedKey.compareTo(preNode.ID) > 0 && hashedKey.compareTo(node.ID) <= 0){
				//Log.v(TAG,"key is in range:: " + key );
				return true;
			} else {
				//Log.v(TAG,"key is not in range: " + key);
				return false;
			}
		} catch (NoSuchAlgorithmException e) {
			Log.v(TAG, "" + e);
		}
		return false;
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			//Log.v(TAG, "Inside ServerTask doInBackground");
			ServerSocket serverSocket = sockets[0];
			Socket inputSocket;
			//Log.v(TAG, "Inside ServerTask doInBackground serverSocket: "+serverSocket);

			try {
				while (true) {
					Log.v(TAG, "Inside while true");
					/* Following code is reference from https://docs.oracle.com/javase/tutorial/networking/sockets/readingWriting.html*/
					inputSocket = serverSocket.accept();
					inputSocket.setReuseAddress(true);
					InputStream inputStream = inputSocket.getInputStream();
					BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
					String recvMsg = bufferedReader.readLine();
					Log.v(TAG, "Inside ServerTask Recv msg: "+recvMsg);
					String[] msgSplit = recvMsg.split("##");
					final SQLiteDatabase db;
					if(msgSplit[0].equalsIgnoreCase("Insert") || msgSplit[0].equalsIgnoreCase("Insert_R_1") ||
							msgSplit[0].equalsIgnoreCase("Insert_R_2")){
						db = mDynamoDbHelper.getWritableDatabase();
						Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
						ContentValues mContentValues = new ContentValues();
						mContentValues.put("key", msgSplit[3]);
						mContentValues.put("value", msgSplit[4]);
						long id = db.insertWithOnConflict(DynamoContract.DynamoEntry.TABLE_NAME,null ,mContentValues, CONFLICT_REPLACE);
						Uri result;
						if ( id > 0 ) {
							result = ContentUris.withAppendedId(mUri, id);
						} else {
							throw new android.database.SQLException("Failed to insert row into " + mUri);
						}
						OutputStream outputStream = inputSocket.getOutputStream();
						PrintWriter printWriter = new PrintWriter(outputStream, true);
						printWriter.println("InsertSuccess");
					}else if(msgSplit[0].equalsIgnoreCase("SingleQueryFind") && inRecovery == false) {
						db = mDynamoDbHelper.getReadableDatabase();
						Log.v(TAG, "Inside Server: SingleQueryFind");
						String key = msgSplit[3];
						//String hashedkey = genHash(key);
						String mSelection = DynamoContract.DynamoEntry.COLUMN_KEY + "=?";
						String[] mSelectionArgs = new String[]{key};

						Cursor result = db.query(DynamoContract.DynamoEntry.TABLE_NAME, null, mSelection,
								mSelectionArgs, null, null, null);
						result.moveToNext();
						String key1 = result.getString(1);
						String value1 = result.getString(2);

						String msgToSend = "SingleQueryResponse"+"##"+myPort.trim()+"##"+msgSplit[1].trim()+"##"+key1.trim()+"##"+value1.trim();
						Log.v(TAG,"SingleQueryResponse in server: "+msgToSend);
						OutputStream outputStream = inputSocket.getOutputStream();
						PrintWriter printWriter = new PrintWriter(outputStream, true);
						printWriter.println(msgToSend);
					}else if(msgSplit[0].equalsIgnoreCase("AllQueryFind")) {
						db = mDynamoDbHelper.getReadableDatabase();
						Log.v(TAG, "Inside Server: AllQueryFind");
						Cursor result = db.query(DynamoContract.DynamoEntry.TABLE_NAME, null, null,
								null, null, null, null);
						String output = "AllQueryResponse";
						while(result.moveToNext()){
							String key1 = result.getString(1);
							String value1 = result.getString(2);
							output=  output +"##"+key1.trim()+"##"+value1.trim();
						}
						OutputStream outputStream = inputSocket.getOutputStream();
						PrintWriter printWriter = new PrintWriter(outputStream, true);
						printWriter.println(output);
					}else if(msgSplit[0].equalsIgnoreCase("QueryRecoveryPre")) {
						db = mDynamoDbHelper.getReadableDatabase();
						Cursor result = db.query(DynamoContract.DynamoEntry.TABLE_NAME, null, null,
								null, null, null, null);
						String output = "RecoveryResponse";
						while(result.moveToNext()){
							String key1 = result.getString(1);
							String value1 = result.getString(2);
							if(InRange(key1, predecessorNode, MyNode)) {
								output = output + "##" + key1.trim() + "##" + value1.trim();
							}
						}
						//Log.v(TAG,"RecoveryResponse sending from server: "+output);
						OutputStream outputStream = inputSocket.getOutputStream();
						PrintWriter printWriter = new PrintWriter(outputStream, true);
						printWriter.println(output);

					}else if(msgSplit[0].equalsIgnoreCase("QueryRecoverySuc")) {
						db = mDynamoDbHelper.getReadableDatabase();
						Cursor result = db.query(DynamoContract.DynamoEntry.TABLE_NAME, null, null,
								null, null, null, null);
						String output = "RecoveryResponse";
						while(result.moveToNext()){
							String key1 = result.getString(1);
							String value1 = result.getString(2);
							if(InRange(key1, predecessorNode2, predecessorNode)) {
								output = output + "##" + key1.trim() + "##" + value1.trim();
							}
						}
						OutputStream outputStream = inputSocket.getOutputStream();
						PrintWriter printWriter = new PrintWriter(outputStream, true);
						printWriter.println(output);
					}else if(msgSplit[0].equalsIgnoreCase("Delete")) {
						db = mDynamoDbHelper.getWritableDatabase();

						String mSelection = DynamoContract.DynamoEntry.COLUMN_KEY + "=?";
						String[] mSelectionArgs = new String[]{msgSplit[3]};
						int result = db.delete(DynamoContract.DynamoEntry.TABLE_NAME, mSelection, mSelectionArgs);

						OutputStream outputStream = inputSocket.getOutputStream();
						PrintWriter printWriter = new PrintWriter(outputStream, true);
						printWriter.println("DeleteSuccess");
					}
				}
			}catch (IOException e) {
				Log.v(TAG, "In ServerTask  IOException");
				e.printStackTrace();
			}catch (NullPointerException e){
			  Log.v(TAG, "In ServerTask  NullPointerException");
				e.printStackTrace();
			}
            return null;
		}
	}

	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... params) {
			Log.v(TAG, "Inside Client Task");
			String msgToSend = params[0];
			String destinationPort = params[1];
			final SQLiteDatabase db;
			try {
				Socket socket;
				socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(destinationPort));
				socket.setReuseAddress(true);
				socket.setSoTimeout(2000);
				Log.v(TAG, "In ClientTask Sending = " +msgToSend);
				OutputStream outputStream = socket.getOutputStream();
				PrintWriter printWriter = new PrintWriter(outputStream, true);
				printWriter.println(msgToSend);

				BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				String recvMsg = bufferedReader.readLine();
				if (recvMsg == null) {
					Log.e(TAG, "In ClientTask null true:");
					throw new SocketTimeoutException();
				}

				String[] msgSplit = recvMsg.split("##");
//				for(int j = 0; j < msgSplit.length; j++){
//					Log.v(TAG,"j : "+j+" "+msgSplit[j]);
//				}

				if(msgSplit[0].equalsIgnoreCase("InsertSuccess")){
					secondLock.release();
				}else if(msgSplit[0].equalsIgnoreCase("SingleQueryResponse")){
					Log.v(TAG,"SingleQueryResponse In client: "+recvMsg);
					cursorSingleRecord.addRow(new String[]{msgSplit[3], msgSplit[4]});
					secondLock.release();
				}else if(msgSplit[0].equalsIgnoreCase("AllQueryResponse")){
					for(int i = 1; i < msgSplit.length; i += 2){
						cursorALLRecord.addRow(new String[]{msgSplit[i], msgSplit[i+1]});
					}
					secondLock.release();
				}else if(msgSplit[0].equalsIgnoreCase("RecoveryResponse")){
					Log.v(TAG,"RecoveryResponse In client: "+recvMsg);
					db = mDynamoDbHelper.getWritableDatabase();
					Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
					for(int i = 1; i < msgSplit.length; i += 2){
						ContentValues mContentValues = new ContentValues();
						mContentValues.put("key", msgSplit[i]);
						mContentValues.put("value", msgSplit[i+1]);
						long id = db.insertWithOnConflict(DynamoContract.DynamoEntry.TABLE_NAME,null ,mContentValues, CONFLICT_REPLACE);
						Uri result;
						if ( id > 0 ) {
							result = ContentUris.withAppendedId(mUri, id);
						} else {
							throw new android.database.SQLException("Failed to insert row into " + mUri);
						}
					}
					secondLock.release();
				}else if(msgSplit[0].equalsIgnoreCase("DeleteSuccess")) {
					secondLock.release();
				}
				socket.close();
			} catch (UnknownHostException e) {
				Log.v(TAG, "ClientTask UnknownHostException");
				e.printStackTrace();
			}catch (SocketTimeoutException e) {
				Log.v(TAG, "ClientTask socket SocketTimeoutException");
				e.printStackTrace();
				secondLock.release();
			} catch (IOException e) {
				Log.v(TAG, "ClientTask socket IOException");
				e.printStackTrace();
				secondLock.release();
			}
			return null;
		}
	}


	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

}
