package org.github.tursodatabase.core;

import static org.github.tursodatabase.utils.ByteArrayUtils.stringToUtf8ByteArray;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import org.github.tursodatabase.annotations.NativeInvocation;
import org.github.tursodatabase.utils.LimboExceptionUtils;
import org.github.tursodatabase.utils.Logger;
import org.github.tursodatabase.utils.LoggerFactory;

public class LimboConnection {
	private static final Logger logger = LoggerFactory.getLogger(LimboConnection.class);

	private final String url;
	private final long connectionPtr;
	private final LimboDB database;
	private boolean closed;

	public LimboConnection(String url, String filePath) throws SQLException {
		this(url, filePath, new Properties());
	}

	/**
	 * Creates a connection to limbo database
	 *
	 * @param url e.g. "jdbc:sqlite:fileName"
	 * @param filePath path to file
	 */
	public LimboConnection(String url, String filePath, Properties properties) throws SQLException {
		this.url = url;
		this.database = open(url, filePath, properties);
		this.connectionPtr = this.database.connect();
	}

	private static LimboDB open(String url, String filePath, Properties properties)
			throws SQLException {
		return LimboDBFactory.open(url, filePath, properties);
	}

	public void checkOpen() throws SQLException {
		if (isClosed()) throw new SQLException("database connection closed");
	}

	public String getUrl() {
		return url;
	}

	public void close() throws SQLException {
		if (isClosed()) {
			return;
		}
		this._close(this.connectionPtr);
		this.closed = true;
	}

	private native void _close(long connectionPtr);

	public boolean isClosed() throws SQLException {
		return closed;
	}

	public LimboDB getDatabase() {
		return database;
	}

	/**
	 * Compiles an SQL statement.
	 *
	 * @param sql An SQL statement.
	 * @return Pointer to statement.
	 * @throws SQLException if a database access error occurs.
	 */
	public LimboStatement prepare(String sql) throws SQLException {
		logger.trace("DriverManager [{}] [SQLite EXEC] {}", Thread.currentThread().getName(), sql);
		byte[] sqlBytes = stringToUtf8ByteArray(sql);
		if (sqlBytes == null) {
			throw new SQLException("Failed to convert " + sql + " into bytes");
		}
		return new LimboStatement(sql, prepareUtf8(connectionPtr, sqlBytes));
	}

	private native long prepareUtf8(long connectionPtr, byte[] sqlUtf8) throws SQLException;

	// TODO: check whether this is still valid for limbo
	/**
	 * Checks whether the type, concurrency, and holdability settings for a {@link ResultSet} are
	 * supported by the SQLite interface. Supported settings are:
	 *
	 * <ul>
	 *   <li>type: {@link ResultSet#TYPE_FORWARD_ONLY}
	 *   <li>concurrency: {@link ResultSet#CONCUR_READ_ONLY})
	 *   <li>holdability: {@link ResultSet#CLOSE_CURSORS_AT_COMMIT}
	 * </ul>
	 *
	 * @param resultSetType the type setting.
	 * @param resultSetConcurrency the concurrency setting.
	 * @param resultSetHoldability the holdability setting.
	 */
	public void checkCursor(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		if (resultSetType != ResultSet.TYPE_FORWARD_ONLY) {
			throw new SQLException("SQLite only supports TYPE_FORWARD_ONLY cursors");
		}
		if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
			throw new SQLException("SQLite only supports CONCUR_READ_ONLY cursors");
		}
		if (resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
			throw new SQLException("SQLite only supports closing cursors at commit");
		}
	}

	/**
	 * Throws formatted SQLException with error code and message.
	 *
	 * @param errorCode Error code.
	 * @param errorMessageBytes Error message.
	 */
	@NativeInvocation(invokedFrom = "limbo_connection.rs")
	private void throwLimboException(int errorCode, byte[] errorMessageBytes) throws SQLException {
		LimboExceptionUtils.throwLimboException(errorCode, errorMessageBytes);
	}
}
