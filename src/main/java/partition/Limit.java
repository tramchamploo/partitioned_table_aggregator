package partition;

import com.google.common.base.Strings;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by guohang.bao on 15-6-25.
 */
public class Limit {

    private int offset;
    private int rowCount;

    private static None noneInstance = new None();

    private static Pattern pattern = Pattern.compile("limit\\s+((\\d+)\\s*,)?\\s*(\\d+)", Pattern.CASE_INSENSITIVE);

    public Limit(int offset, int rowCount) {
        this.offset = offset;
        this.rowCount = rowCount;
    }

    protected int getOffset() {
        return offset;
    }

    protected int getRowCount() {
        return rowCount;
    }

    public int count() {
        return getOffset() + getRowCount();
    }

    public static Limit fromSql(String sql) {
        if (Strings.isNullOrEmpty(sql)) return none();
        Matcher matcher = pattern.matcher(sql);
        if (matcher.find()) {
            String offsetStr = matcher.group(2);
            String rowCountStr = matcher.group(3);
            if (!Strings.isNullOrEmpty(offsetStr)) {
                return new Limit(Integer.parseInt(offsetStr), Integer.parseInt(rowCountStr));
            } else {
                return new Limit(0, Integer.parseInt(rowCountStr));
            }
        }
        throw new IllegalArgumentException(sql);
    }

    public static Limit none() {
        return noneInstance;
    }

    public static class None extends Limit {
        public None() {
            super(0, 0);
        }

        @Override
        public int count() {
            return Integer.MAX_VALUE;
        }

        @Override
        protected int getOffset() {
            return Integer.MAX_VALUE;
        }

        @Override
        protected int getRowCount() {
            return Integer.MAX_VALUE;
        }
    }

    @Override
    public String toString() {
        return "LIMIT " + getOffset() + ", " + getRowCount();
    }
}
