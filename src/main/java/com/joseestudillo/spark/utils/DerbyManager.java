package com.joseestudillo.spark.utils;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.logging.log4j.Logger;

/**
 * 
 * Class to run an instance of Derby
 * 
 * @author Jose Estudillo
 *
 */
public class DerbyManager {
	public static final String DERBY_DRIVER = "org.apache.derby.jdbc.EmbeddedDriver";
	public static final String DERBY_SERVER_CONN_STR_PATTERN = "jdbc:derby://localhost:1527/%s;create=true";;
	public static final String DERBY_CONN_STR_PATTERN = "jdbc:derby:%s;create=true";
	public static final String DERBY_SHUTDOWN_STR_PATTERN = "jdbc:derby:%s;shutdown=true";
	public static final String DEFAULT_DB_NAME = "databaseName";
	public static final String DEFAULT_DB_PATH_PTRN = "/tmp/derby/%s";

	public static void loadDriver() throws InstantiationException, IllegalAccessException, ClassNotFoundException {
		Class.forName(DERBY_DRIVER).newInstance();
	}

	public static final String getConnectionString(String databaseName) {
		return String.format(DERBY_CONN_STR_PATTERN, databaseName);
	}

	public static final String getServerConnectionString(String databaseName) {
		return String.format(DERBY_SERVER_CONN_STR_PATTERN, String.format(DEFAULT_DB_PATH_PTRN, databaseName));
	}

	public static final String getServerConnectionString(File storageDir) {
		return String.format(DERBY_SERVER_CONN_STR_PATTERN, storageDir.getPath());
	}

	public static final Connection getDerbyConnection(String databaseName) throws SQLException {
		return DriverManager.getConnection(getConnectionString(databaseName));
	}

	public static final Connection getDerbyConnection() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		return getDerbyConnection(DEFAULT_DB_NAME);
	}

	public static final Connection getServerDerbyConnection() throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {
		return getDerbyConnection(getServerConnectionString(DEFAULT_DB_NAME));
	}

	public static final void shutdownDatabase(String dbName) throws SQLException {
		DriverManager.getConnection(String.format(DERBY_SHUTDOWN_STR_PATTERN, dbName));
	}

	public static final void shutdownAllDatabases() throws SQLException {
		DriverManager.getConnection(String.format(DERBY_SHUTDOWN_STR_PATTERN, ""));
	}

	public static String DERBY_TABLE_NAME = "derby_table";
	private static String CREATE_TABLE = String.format("CREATE TABLE %s (id INT not null, name VARCHAR(255) not null, PRIMARY KEY (id))", DERBY_TABLE_NAME);
	private static String DELETE_TABLE = String.format("DROP TABLE %s", DERBY_TABLE_NAME);
	private static String INSERT_PS = String.format("INSERT INTO %s (id, name) values (?, ?)", DERBY_TABLE_NAME);
	private static String DELETE_PS = String.format("DELETE FROM %s where id = ?", DERBY_TABLE_NAME);
	private static String SELECT_ALL = String.format("SELECT * FROM %s", DERBY_TABLE_NAME);

	public static void createDummyTable(Connection conn) throws SQLException {
		Statement st = conn.createStatement();
		st.execute(DELETE_TABLE);
		st.execute(CREATE_TABLE);
		PreparedStatement pst = conn.prepareStatement(INSERT_PS);
		for (int i = 0; i < 3; i++) {
			pst.setInt(1, i);
			pst.setString(2, "name" + i);
			pst.executeUpdate();
		}
		conn.commit();
	}

	public static void logDummyTableContent(Connection conn, Logger log) throws SQLException {
		Statement st = conn.createStatement();
		ResultSet resultSet = st.executeQuery(SELECT_ALL);
		while (resultSet.next()) {
			log.info(String.format("%s, %s", resultSet.getLong(1), resultSet.getString(2)));
		}
	}
}
