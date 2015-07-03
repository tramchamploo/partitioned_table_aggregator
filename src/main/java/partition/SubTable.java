package partition;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * Created by guohang.bao on 15-6-25.
 */
public class SubTable {

    private String prefix;

    private String[] tableNames;

    private int from;

    private int to;

    public SubTable(String prefix, int from, int to) {
        Preconditions.checkArgument(from < to);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(prefix));
        this.prefix = prefix;
        this.from = from;
        this.to = to;
        tableNames = new String[to - from + 1];
        int idx = 0;
        for (int i = from; i <= to; i++, idx++) {
            tableNames[idx] = prefix + i;
        }
    }

    public SubTable(String prefix, int index) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(prefix));
        this.prefix = prefix;
        this.from = index;
        this.to = index;
        tableNames = new String[1];
        tableNames[0] = prefix + index;
    }

    public String[] getTableNames() {
        return tableNames;
    }

    public int numOfTables() {
        return tableNames.length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SubTable subTable = (SubTable) o;

        if (from != subTable.from) return false;
        if (to != subTable.to) return false;
        return !(prefix != null ? !prefix.equals(subTable.prefix) : subTable.prefix != null);

    }

    @Override
    public int hashCode() {
        int result = prefix != null ? prefix.hashCode() : 0;
        result = 31 * result + from;
        result = 31 * result + to;
        return result;
    }
}
