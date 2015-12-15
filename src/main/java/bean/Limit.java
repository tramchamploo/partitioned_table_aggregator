package bean;

import com.google.common.base.Strings;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by guohang.bao on 15-6-25.
 * Tools for user to specify limit, so sql writer need not to write limit in sql.
 */
public class Limit {

    private int offset;
    private int rowCount;
    private int nQMarks;

    private static None noneInstance = new None();

    private static Pattern pattern = Pattern.compile("limit\\s+((\\d+|\\?)\\s*,)?\\s*(\\d+|\\?)",
            Pattern.CASE_INSENSITIVE);

    public Limit(int offset, int rowCount, int nQMarks) {
        this.offset = offset;
        this.rowCount = rowCount;
        this.nQMarks = nQMarks;
    }

    public int getOffset() {
        return offset;
    }

    public int getRowCount() {
        return rowCount;
    }

    public int count() {
        return getOffset() + getRowCount();
    }

    public int nQMarks() {
        return nQMarks;
    }

    public static Limit fromSql(String sql) {
        return fromSql(sql, null);
    }

    public static Limit fromSql(String sql, Object[] args) {
        if (Strings.isNullOrEmpty(sql)) return none();
        Matcher matcher = pattern.matcher(sql);
        if (matcher.find()) {
            String offsetStr = matcher.group(2);
            String rowCountStr = matcher.group(3);
            int offset = 0;
            int rowCount = 0;
            int nQMarks = 0;

            if (rowCountStr.equals("?")) {
                if (args == null || args.length < 1) {
                    throw new IllegalArgumentException();
                }

                rowCount = (Integer) args[args.length - 1];
                nQMarks += 1;
            } else {
                rowCount = Integer.parseInt(rowCountStr);
            }

            if (!Strings.isNullOrEmpty(offsetStr)) {
                if (offsetStr.equals("?")) {
                    if (args == null || args.length < 2) {
                        throw new IllegalArgumentException();
                    }

                    offset = (Integer) args[args.length - 2];
                    nQMarks += 1;
                } else {
                    offset = Integer.parseInt(offsetStr);
                }
            }

            return new Limit(offset, rowCount, nQMarks);
        }
        return none();
    }

    public static Limit none() {
        return noneInstance;
    }

    public boolean isNone() {
        return this == none();
    }

    public static class None extends Limit {
        public None() {
            super(0, 0, 0);
        }
    }

    @Override
    public String toString() {
        return "LIMIT " + getOffset() + ", " + getRowCount();
    }
}
