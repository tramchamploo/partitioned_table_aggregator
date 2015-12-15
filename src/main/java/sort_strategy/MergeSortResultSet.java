package sort_strategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.profiler.Profiler;
import org.springframework.jdbc.support.rowset.ResultSetWrappingSqlRowSet;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import util.ProfilerFactory;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Map;

/**
 * Created by guohang.bao on 15-8-27.
 */
public class MergeSortResultSet implements ResultSet {

    private Logger logger = LoggerFactory.getLogger(MergeSortResultSet.class);

    private ResultSet left;

    private SqlRowSet leftWrapped;

    private ResultSet right;

    private SqlRowSet rightWrapped;

    private ResultSet current;

    private SqlRowSet currentWrapped;

    private int lastCompareResult = 0;

    private Comparator<SqlRowSet> comparator;

    private boolean noSort = false;

    private boolean first = false;

    private boolean beforeFirst = true;

    private boolean last = false;

    private boolean afterLast = false;

    private int rowNum = 0;

    private boolean leftEnd;

    private boolean rightEnd;


    public MergeSortResultSet(ResultSet left, ResultSet right, Comparator<SqlRowSet> comparator) {
        this.left = left;
        if (left != null) this.leftWrapped = new ResultSetWrappingSqlRowSet(left);

        this.right = right;
        if (right != null) this.rightWrapped = new ResultSetWrappingSqlRowSet(right);

        this.comparator = comparator;

        noSort = comparator == null;

        init();
    }


    private void init() {
        Profiler profiler = ProfilerFactory.createProfiler("MergeSortResultSet.init");
        profiler.setLogger(logger);

        first = false;
        beforeFirst = true;
        last = false;
        afterLast = false;

        if (!noSort) {
            try {
                profiler.start("move to first");
                boolean leftFirst = left.first();
                boolean rightFirst = right.first();

                if (leftFirst && rightFirst) {
                    profiler.start("compare");
                    lastCompareResult = comparator.compare(leftWrapped, rightWrapped);
                    if (lastCompareResult <= 0) {
                        current = left;
                    } else {
                        current = right;
                    }
                } else if (leftFirst) {
                    current = left;
                    lastCompareResult = -1;
                    rightEnd = true;
                } else if (rightFirst) {
                    current = right;
                    lastCompareResult = 1;
                    leftEnd = true;
                }

                changeWrapped();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        profiler.stop().log();
    }


    private void changeWrapped() {
        if (current == null) {
            currentWrapped = null;
        } else if (current == left) {
            currentWrapped = leftWrapped;
        } else if (current == right) {
            currentWrapped = rightWrapped;
        }
    }


    public boolean next() throws SQLException {
        Profiler profiler = ProfilerFactory.createProfiler("MergeSortResultSet.next");
        profiler.setLogger(logger);

        if (current == null && !noSort || leftEnd && rightEnd) {
            return false;
        }

        if (noSort) {
            profiler.start("no sort");
            if (!leftEnd && left.next()) {
                rowNum++;
                current = left;

                changeWrapped();

                profiler.stop().log();
                return true;
            } else if (right.next()) {
                if (!leftEnd) leftEnd = true;

                rowNum++;
                current = right;

                changeWrapped();

                profiler.stop().log();
                return true;
            } else {
                return false;
            }
        } else {
            if (beforeFirst) {
                first = true;
                beforeFirst = false;
                rowNum = 1;
                return current != null;
            }

            if (lastCompareResult <= 0 && !leftEnd) {
                profiler.start("left next");

                if (left.next()) {
                    if (!rightEnd) {
                        profiler.start("compare");
                        lastCompareResult = comparator.compare(leftWrapped, rightWrapped);
                    }
                } else {
                    leftEnd = true;
                    lastCompareResult = 1;
                }
            } else if (!rightEnd) {
                profiler.start("left next");

                if (right.next()) {
                    if (!leftEnd) {
                        profiler.start("compare");
                        lastCompareResult = comparator.compare(leftWrapped, rightWrapped);
                    }
                } else {
                    rightEnd = true;
                    lastCompareResult = -1;
                }
            }

            if (first) {
                first = false;
            }

            rowNum++;

            profiler.start("see if is last");
            last = left.isAfterLast() && right.isLast() || left.isLast() && right.isAfterLast();

            if (!last) {
                profiler.start("see if is after last");
                afterLast = left.isAfterLast() && right.isAfterLast();
            }

            if (lastCompareResult <= 0 && !leftEnd) {
                current = left;

                changeWrapped();
            } else if (!rightEnd) {
                current = right;

                changeWrapped();
            } else {
                current = null;

                changeWrapped();
                profiler.stop().log();
                return false;
            }

            profiler.stop().log();
            return true;
        }
    }

    public void close() throws SQLException {
        left.close();
        right.close();
    }

    public boolean wasNull() throws SQLException {
        return current.wasNull();
    }

    public String getString(int columnIndex) throws SQLException {
        return currentWrapped.getString(columnIndex);
    }

    public boolean getBoolean(int columnIndex) throws SQLException {
        return currentWrapped.getBoolean(columnIndex);
    }

    public byte getByte(int columnIndex) throws SQLException {
        return currentWrapped.getByte(columnIndex);
    }

    public short getShort(int columnIndex) throws SQLException {
        return currentWrapped.getShort(columnIndex);
    }

    public int getInt(int columnIndex) throws SQLException {
        return currentWrapped.getInt(columnIndex);
    }

    public long getLong(int columnIndex) throws SQLException {
        return currentWrapped.getLong(columnIndex);
    }

    public float getFloat(int columnIndex) throws SQLException {
        return currentWrapped.getFloat(columnIndex);
    }

    public double getDouble(int columnIndex) throws SQLException {
        return currentWrapped.getDouble(columnIndex);
    }

    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public byte[] getBytes(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Date getDate(int columnIndex) throws SQLException {
        return currentWrapped.getDate(columnIndex);
    }

    public Time getTime(int columnIndex) throws SQLException {
        return currentWrapped.getTime(columnIndex);
    }

    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return currentWrapped.getTimestamp(columnIndex);
    }

    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getString(String columnLabel) throws SQLException {
        return currentWrapped.getString(columnLabel);
    }

    public boolean getBoolean(String columnLabel) throws SQLException {
        return currentWrapped.getBoolean(columnLabel);
    }

    public byte getByte(String columnLabel) throws SQLException {
        return currentWrapped.getByte(columnLabel);
    }

    public short getShort(String columnLabel) throws SQLException {
        return currentWrapped.getShort(columnLabel);
    }

    public int getInt(String columnLabel) throws SQLException {
        return currentWrapped.getInt(columnLabel);
    }

    public long getLong(String columnLabel) throws SQLException {
        return currentWrapped.getLong(columnLabel);
    }

    public float getFloat(String columnLabel) throws SQLException {
        return currentWrapped.getFloat(columnLabel);
    }

    public double getDouble(String columnLabel) throws SQLException {
        return currentWrapped.getDouble(columnLabel);
    }

    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public byte[] getBytes(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Date getDate(String columnLabel) throws SQLException {
        return currentWrapped.getDate(columnLabel);
    }

    public Time getTime(String columnLabel) throws SQLException {
        return currentWrapped.getTime(columnLabel);
    }

    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return currentWrapped.getTimestamp(columnLabel);
    }

    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public SQLWarning getWarnings() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void clearWarnings() throws SQLException {
        current.clearWarnings();
    }

    public String getCursorName() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public ResultSetMetaData getMetaData() throws SQLException {
        return left.getMetaData();
    }

    public Object getObject(int columnIndex) throws SQLException {
        return currentWrapped.getObject(columnIndex);
    }

    public Object getObject(String columnLabel) throws SQLException {
        return currentWrapped.getObject(columnLabel);
    }

    public int findColumn(String columnLabel) throws SQLException {
        return current.findColumn(columnLabel);
    }

    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return currentWrapped.getBigDecimal(columnIndex);
    }

    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return currentWrapped.getBigDecimal(columnLabel);
    }

    public boolean isBeforeFirst() throws SQLException {
        return beforeFirst;
    }

    public boolean isAfterLast() throws SQLException {
        return afterLast;
    }

    public boolean isFirst() throws SQLException {
        return first;
    }

    public boolean isLast() throws SQLException {
        return last;
    }

    public void beforeFirst() throws SQLException {
        init();
    }

    public void afterLast() throws SQLException {
        while (next()) {
        }
    }

    public boolean first() throws SQLException {
        init();
        return next();
    }

    public boolean last() throws SQLException {
        boolean ret = false;
        while (!isLast()) ret = next();
        return ret;
    }

    public int getRow() throws SQLException {
        return rowNum;
    }

    public boolean absolute(int row) throws SQLException {
        init();

        boolean ret = false;
        while (getRow() != row) ret = next();

        return ret;
    }

    public boolean relative(int rows) throws SQLException {
        return absolute(getRow() + rows);
    }

    public boolean previous() throws SQLException {
        return relative(-1);
    }

    public void setFetchDirection(int direction) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getFetchDirection() throws SQLException {
        return current.getFetchDirection();
    }

    public void setFetchSize(int rows) throws SQLException {
        left.setFetchSize(rows);
        right.setFetchSize(rows);
    }

    public int getFetchSize() throws SQLException {
        return current.getFetchSize();
    }

    public int getType() throws SQLException {
        return current.getType();
    }

    public int getConcurrency() throws SQLException {
        return current.getConcurrency();
    }

    public boolean rowUpdated() throws SQLException {
        return current.rowUpdated();
    }

    public boolean rowInserted() throws SQLException {
        return current.rowInserted();
    }

    public boolean rowDeleted() throws SQLException {
        return current.rowInserted();
    }

    public void updateNull(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateString(int columnIndex, String x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateNull(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateString(String columnLabel, String x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void insertRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void deleteRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void refreshRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void cancelRowUpdates() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void moveToInsertRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void moveToCurrentRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Statement getStatement() throws SQLException {
        return current.getStatement();
    }

    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return currentWrapped.getObject(columnIndex, map);
    }

    public Ref getRef(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Blob getBlob(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Clob getClob(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Array getArray(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return currentWrapped.getObject(columnLabel, map);
    }

    public Ref getRef(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Blob getBlob(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Clob getClob(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Array getArray(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return currentWrapped.getDate(columnIndex, cal);
    }

    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return currentWrapped.getDate(columnLabel, cal);
    }

    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return currentWrapped.getTime(columnIndex, cal);
    }

    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return currentWrapped.getTime(columnLabel, cal);
    }

    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return currentWrapped.getTimestamp(columnIndex, cal);
    }

    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return currentWrapped.getTimestamp(columnLabel, cal);
    }

    public URL getURL(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public URL getURL(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public RowId getRowId(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public RowId getRowId(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public int getHoldability() throws SQLException {
        throw new UnsupportedOperationException();
    }

    public boolean isClosed() throws SQLException {
        return left.isClosed() && right.isClosed();
    }

    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public NClob getNClob(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public NClob getNClob(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public String getNString(int columnIndex) throws SQLException {
        return currentWrapped.getNString(columnIndex);
    }

    public String getNString(String columnLabel) throws SQLException {
        return currentWrapped.getNString(columnLabel);
    }

    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return currentWrapped.getObject(columnIndex, type);
    }

    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return currentWrapped.getObject(columnLabel, type);
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return current.unwrap(iface);
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return current.isWrapperFor(iface);
    }
}
