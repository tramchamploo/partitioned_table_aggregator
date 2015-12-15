package bean;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.Arrays;

/**
 * Created by guohang.bao on 15-6-25.
 * A partitioned table, such as forum_post_p0...p19
 */
public class PartitionedTable {

    private String prefix;

    private String[] tableNames;

    private int from;

    private int to;

    public PartitionedTable(String prefix, int from, int to) {
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

    public PartitionedTable(String prefix, int index) {
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

    public String getTheOnlyTableName() {
        if (tableNames.length != 1) throw new RuntimeException(tableNames.length + " tables in all!");
        return tableNames[0];
    }

    public int numOfTables() {
        return tableNames.length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PartitionedTable partitionedTable = (PartitionedTable) o;

        if (from != partitionedTable.from) return false;
        if (to != partitionedTable.to) return false;
        return !(prefix != null ? !prefix.equals(partitionedTable.prefix) : partitionedTable.prefix != null);

    }

    @Override
    public int hashCode() {
        int result = prefix != null ? prefix.hashCode() : 0;
        result = 31 * result + from;
        result = 31 * result + to;
        return result;
    }

    @Override
    public String toString() {
        if (tableNames.length == 1) return getTheOnlyTableName();
        else return Arrays.toString(getTableNames());
    }
}
