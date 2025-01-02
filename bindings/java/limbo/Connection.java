package limbo;

import limbo.exceptions.InterfaceError;
import limbo.exceptions.OperationalError;

/**
 * Represents a connection to the database.
 */
public class Connection {

    private long connectionId;

    public Connection(long connectionId) {
        this.connectionId = connectionId;
    }

    /**
     * Creates a new cursor object using this connection.
     *
     * @return A new Cursor object.
     * @throws InterfaceError If the cursor cannot be created.
     */
    public Cursor cursor() throws InterfaceError {
        return new Cursor();
    }

    /**
     * Closes the connection to the database.
     *
     * @throws OperationalError If there is an error closing the connection.
     */
    public void close() throws OperationalError {
        // Implementation here
    }

    /**
     * Commits the current transaction.
     *
     * @throws OperationalError If there is an error during commit.
     */
    public void commit() throws OperationalError {
        // Implementation here
    }

    /**
     * Rolls back the current transaction.
     *
     * @throws OperationalError If there is an error during rollback.
     */
    public void rollback() throws OperationalError {
        // Implementation here
    }

    public long getConnectionId() {
        return connectionId;
    }

    public void setConnectionId(long connectionId) {
        this.connectionId = connectionId;
    }
}
