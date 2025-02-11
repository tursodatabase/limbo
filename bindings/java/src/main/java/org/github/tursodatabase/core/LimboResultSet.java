package org.github.tursodatabase.core;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.github.tursodatabase.annotations.Nullable;
import org.github.tursodatabase.utils.Logger;
import org.github.tursodatabase.utils.LoggerFactory;

/**
 * A table of data representing limbo database result set, which is generated by executing a
 * statement that queries the database.
 *
 * <p>A {@link LimboResultSet} object is automatically closed when the {@link LimboStatement} object
 * that generated it is closed or re-executed.
 */
public class LimboResultSet {

	private static final Logger log = LoggerFactory.getLogger(LimboResultSet.class);

	private final LimboStatement statement;

	// Name of the columns
	private String[] columnNames = new String[0];
	// Whether the result set does not have any rows.
	private boolean isEmptyResultSet = false;
	// If the result set is open. Doesn't mean it has results.
	private boolean open;
	// Maximum number of rows as set by the statement
	private long maxRows;
	// number of current row, starts at 1 (0 is used to represent loading data)
	private int row = 0;
	private boolean pastLastRow = false;

	@Nullable private LimboStepResult lastStepResult;

	public static LimboResultSet of(LimboStatement statement) {
		return new LimboResultSet(statement);
	}

	private LimboResultSet(LimboStatement statement) {
		this.open = true;
		this.statement = statement;
	}

	/**
	 * Consumes all the rows in this {@link ResultSet} until the {@link #next()} method returns
	 * `false`.
	 *
	 * @throws SQLException if the result set is not open or if an error occurs while iterating.
	 */
	public void consumeAll() throws SQLException {
		if (!open) {
			throw new SQLException("The result set is not open");
		}

		while (next()) {}
	}

	/**
	 * Moves the cursor forward one row from its current position. A {@link LimboResultSet} cursor is
	 * initially positioned before the first fow; the first call to the method <code>next</code> makes
	 * the first row the current row; the second call makes the second row the current row, and so on.
	 * When a call to the <code>next</code> method returns <code>false</code>, the cursor is
	 * positioned after the last row.
	 *
	 * <p>Note that limbo only supports <code>ResultSet.TYPE_FORWARD_ONLY</code>, which means that the
	 * cursor can only move forward.
	 */
	public boolean next() throws SQLException {
		if (!open) {
			throw new SQLException("The resultSet is not open");
		}

		if (isEmptyResultSet || pastLastRow) {
			return false; // completed ResultSet
		}

		if (maxRows != 0 && row == maxRows) {
			return false;
		}

		lastStepResult = this.statement.step();
		log.debug("lastStepResult: {}", lastStepResult);
		if (lastStepResult.isRow()) {
			row++;
		}

		if (lastStepResult.isInInvalidState()) {
			open = false;
			throw new SQLException("step() returned invalid result: " + lastStepResult);
		}

		pastLastRow = lastStepResult.isDone();
		return !pastLastRow;
	}

	/** Checks whether the last step result has returned row result. */
	public boolean hasLastStepReturnedRow() {
		return lastStepResult != null && lastStepResult.isRow();
	}

	/**
	 * Checks the status of the result set.
	 *
	 * @return true if it's ready to iterate over the result set; false otherwise.
	 */
	public boolean isOpen() {
		return open;
	}

	/** @throws SQLException if not {@link #open} */
	public void checkOpen() throws SQLException {
		if (!open) {
			throw new SQLException("ResultSet closed");
		}
	}

	public void close() throws SQLException {
		this.open = false;
	}

	// Note that columnIndex starts from 1
	@Nullable
	public Object get(int columnIndex) throws SQLException {
		if (!this.isOpen()) {
			throw new SQLException("ResultSet is not open");
		}

		if (this.lastStepResult == null || this.lastStepResult.getResult() == null) {
			throw new SQLException("ResultSet is null");
		}

		final Object[] resultSet = this.lastStepResult.getResult();
		if (columnIndex > resultSet.length || columnIndex < 0) {
			throw new SQLException("columnIndex out of bound");
		}

		return resultSet[columnIndex - 1];
	}

	public String[] getColumnNames() {
		return this.columnNames;
	}

	public void setColumnNames(String[] columnNames) {
		this.columnNames = columnNames;
	}

	@Override
	public String toString() {
		return "LimboResultSet{"
				+ "statement="
				+ statement
				+ ", isEmptyResultSet="
				+ isEmptyResultSet
				+ ", open="
				+ open
				+ ", maxRows="
				+ maxRows
				+ ", row="
				+ row
				+ ", pastLastRow="
				+ pastLastRow
				+ ", lastResult="
				+ lastStepResult
				+ '}';
	}
}
