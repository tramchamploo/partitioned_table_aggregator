package bean;

import com.google.common.base.*;
import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.*;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.update.Update;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import util.tuple.Tuple;
import util.tuple.Tuples;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by guohang.bao on 15-6-25.
 * sql to specify when querying from partitions
 */
public class Sql {

    /**
     * partitioned table need to be replaced with PLACEHOLDER + pos
     */
    private static final String PLACEHOLDER = "$";

    /**
     * max num of partitioned tables in a query
     */
    private static final int N_MAX_SUB_TABLES = 8;


    private int nPartitionedTables = 0;


    protected String sql;


    protected String sqlWithPlaceholder;


    protected Object[] args;


    protected Limit limit;


    protected PartitionedTable[] partitionedTables;


    private Select select;


    private Multimap<String, Object[]> tables;


    private LinkedHashMap<String, SelectExpressionItem> fields;


    private Tuple<String, Boolean> orderBy;


    // function ==> alias
    private Map<Function, String> functions;


    private List<Column> groupBys;


    public List<Column> groupBys() {
        return groupBys;
    }


    protected Logger logger = LoggerFactory.getLogger(Sql.class);


    private enum Type {
        SELECT, UPDATE
    }

    private Type type = Type.SELECT;


    public Sql(String sql, Object[] args) {
        this(sql, args, null, null, null, null, null);
    }


    Sql(String sql,
        Object[] args,
        LinkedHashMap<String, SelectExpressionItem> fields,
        Map<Function, String> functions,
        List<Column> groupBys,
        Tuple<String, Boolean> orderBy,
        Limit limit) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(sql));

        this.sql = sql;
        this.args = args;

        Multimap<String, Object[]> tables = Sql.tables(sql);

        ImmutableSet<String> partitioned = Sets.intersection(tables.keySet(),
                SubTables.PARTITIONED_TABLES.keySet()).immutableCopy();

        if (partitioned.size() > 0) {
            List<PartitionedTable> partitionedTableList = Lists.newLinkedList();

            int subTableIdx = 1;
            int pos = 0;
            StringBuilder result = new StringBuilder();

            for (String table : partitioned) {
                Collection<Object[]> positions = tables.get(table);

                for (Object[] p : positions) {
                    int tablePos = (Integer) p[0];

                    result.append(sql.substring(pos, tablePos)).append(Sql.withPlaceHolder(subTableIdx++));
                    partitionedTableList.add(SubTables.PARTITIONED_TABLES.get(table));

                    pos = tablePos + table.length();
                }
            }
            result.append(sql.substring(pos, sql.length()));

            this.sqlWithPlaceholder = result.toString();

            if (partitionedTableList.size() > 0) {
                PartitionedTable[] sts = new PartitionedTable[partitionedTableList.size()];
                this.partitionedTables = partitionedTableList.toArray(sts);

                this.tables = tables;

                try {
                    Statement stmt = CCJSqlParserUtil.parse(sql);

                    if (stmt instanceof Select) {
                        this.select = (Select) stmt;
                    } else if (stmt instanceof Update) {
                        this.type = Type.UPDATE;
                    }
                } catch (JSQLParserException e) {
                    logger.error("select parse failed", e);
                }

                if (fields == null && type == Type.SELECT) {
                    this.initFields();
                } else {
                    this.fields = fields;
                    this.functions = functions;
                }

                if (groupBys == null && type == Type.SELECT) {
                    this.initGroupBy();
                } else {
                    this.groupBys = groupBys;
                }

                if (orderBy == null && type == Type.SELECT) {
                    this.initOrderBy();
                } else {
                    this.orderBy = orderBy;
                }

                if (limit == null && type == Type.SELECT) {
                    this.limit = Limit.fromSql(sql, args);
                } else {
                    this.limit = limit;
                }

                this.initNPartitionedTables();

                validate();
            }
        }

    }


    public PartitionedTable[] partitionedTables() {
        return partitionedTables;
    }


    public static String withPlaceHolder(int idx) {
        return PLACEHOLDER + idx;
    }

    /**
     * see how many partitioned tables in this query
     */
    private void initNPartitionedTables() {
        int idx = 0;
        for (int i = 1; i <= N_MAX_SUB_TABLES; i++) {
            if ((idx = sqlWithPlaceholder.indexOf(PLACEHOLDER + i, idx)) > -1) nPartitionedTables++;
        }
        Preconditions.checkArgument(nPartitionedTables == partitionedTables.length);
    }

    /**
     * replace PLACEHOLDER with real table names
     */
    public String[] subSqls() {
        int numOfResult = partitionedTables[0].numOfTables();
        String[] ret = new String[numOfResult];

        Sql s = withoutLimit();
        if (s.groupBys != null) {
            s = s.withoutGroupBy().withoutOrderBy();
        }

        String baseSql = s.sqlWithPlaceholder;

        for (int i = 0; i < numOfResult; i++) {
            String sqlCopy = baseSql;

            for (int j = 1; j <= nPartitionedTables; j++) {
                sqlCopy = sqlCopy.replace(PLACEHOLDER + j, partitionedTables[j - 1].getTableNames()[i]);
            }

            sqlCopy = replaceFunctionsWithFields(sqlCopy);

            ret[i] = appendLimit(sqlCopy);
        }
        return ret;
    }

    public Map<Function, String> functions() {
        return functions;
    }

    private String replaceFunctionsWithFields(String sql) {
        if (type == Type.SELECT &&
                ((!canAddResult() && groupBys == null) || (groupBys != null))) {

            for (Map.Entry<String, SelectExpressionItem> itemEntry : fields.entrySet()) {
                // if field is function
                if (itemEntry.getValue().getExpression() instanceof Function) {
                    Function funcField = (Function) itemEntry.getValue().getExpression();
                    String paramsWithParens = funcField.getParameters().toString();

                    sql = sql.replace(itemEntry.getKey(), paramsWithParens.substring(1, paramsWithParens.length() - 1));
                }
            }
        }
        return sql;
    }


    public Limit limit() {
        return limit;
    }


    private int beforeAnyOf(String src, Pattern ptn) {
        Matcher matcher = ptn.matcher(src);
        if (matcher.find()) {
            return matcher.start();
        }
        return src.length();
    }

    private String excludedFrom(String src, Pattern ptn, Pattern notAfterPattern) {
        Matcher matcher = ptn.matcher(src);
        if (matcher.find()) {
            return src.substring(0, matcher.start()) + src.substring(beforeAnyOf(src, notAfterPattern));
        }
        return src;
    }


    private static final Pattern GROUP_BY_PATTERN = Pattern.compile("\\s+group\\s+by\\s+", Pattern.CASE_INSENSITIVE);

    private static final Pattern GROUP_BY_NOT_AFTER_PATTERN = Pattern.compile("\\s+(order|limit|;)\\s+", Pattern.CASE_INSENSITIVE);


    /**
     * sql with no group by
     */
    public Sql withoutGroupBy() {
        return new Sql(excludedFrom(sql, GROUP_BY_PATTERN, GROUP_BY_NOT_AFTER_PATTERN), args, fields, functions, groupBys, orderBy, limit);
    }


    private static final Pattern LIMIT_PATTERN = Pattern.compile("\\s+limit\\s+", Pattern.CASE_INSENSITIVE);

    private static final Pattern LIMIT_NOT_AFTER_PATTERN = Pattern.compile("\\s+;", Pattern.CASE_INSENSITIVE);

    /**
     * sql with no limit
     */
    public Sql withoutLimit() {
        return new Sql(excludedFrom(sql, LIMIT_PATTERN, LIMIT_NOT_AFTER_PATTERN), args, fields, functions, groupBys, orderBy, limit);
    }

    /**
     * append limit when no limit in sql
     */
    private String appendLimit(String src) {
        if (type == Type.SELECT && groupBys == null && !LIMIT_PATTERN.matcher(src).find()) {
            if (limit() instanceof Limit.None) {
                return src;
            } else {
                String cnt = partitionedTables().length > 0 && partitionedTables()[0].numOfTables() > 1 ?
                        String.valueOf(limit().count()) :
                        (limit().getOffset() + ", " + limit().getRowCount());

                if (src.endsWith(";")) {
                    return src.substring(0, src.length() - 1) + " LIMIT " + cnt + ";";
                } else {
                    return src + " LIMIT " + cnt;
                }
            }
        }
        return src;
    }

    /**
     * if limit has question marks, remove args from original arg list
     */
    public Object[] trimArgs(Object[] args) {
        if (limit().nQMarks() > 0) {
            return Arrays.copyOfRange(args, 0, args.length - limit().nQMarks());
        }
        return args;
    }

    private static final Set<String> TABLE_ENDS = ImmutableSet.of("where", "group", "order", "limit", "set", ";");

    private static final Set<String> JOIN_PREFIX = ImmutableSet.of("inner", "outer", "left", "right");

    private static final Pattern TABLE_START = Pattern.compile("(\\s+from|update)\\s+");

    /**
     * get table name from sql
     *
     * @return tableName -> (pos, alias)
     */
    static Multimap<String, Object[]> tables(String sql) {
        String lowerCased = sql.toLowerCase();

        Matcher matcher = TABLE_START.matcher(lowerCased);
        int tailStart = -1;
        if (matcher.find()) tailStart = matcher.end();

        if (tailStart > -1) {
            Multimap<String, Object[]> ret = LinkedListMultimap.create(N_MAX_SUB_TABLES);

            while (CharMatcher.WHITESPACE.matches(lowerCased.charAt(tailStart))) {
                tailStart++;
            }

            String tail = lowerCased.substring(tailStart).trim();

            String currTable = null;
            int currTablePos = -1;
            String currAlias = null;

            StringBuilder buffer = new StringBuilder();
            for (int i = 0; i < tail.length(); i++) {
                char ch = tail.charAt(i);

                if (CharMatcher.WHITESPACE.matches(ch)) {
                    String part = buffer.toString();

                    if (part.isEmpty()) continue;

                    if (TABLE_ENDS.contains(part)) {
                        ret.put(currTable, new Object[]{currTablePos, currAlias});
                        currTable = null;
                        currTablePos = -1;
                        currAlias = null;
                        buffer.delete(0, buffer.length());
                        break;
                    }

                    if (JOIN_PREFIX.contains(part)) {
                        ret.put(currTable, new Object[]{currTablePos, currAlias});
                        currTable = null;
                        currTablePos = -1;
                        currAlias = null;
                        buffer.delete(0, buffer.length());
                        continue;
                    }

                    if (part.equals("as") || part.equals("join")) {
                        buffer.delete(0, buffer.length());
                        continue;
                    }

                    if (currTable == null) {
                        currTable = part;
                        currTablePos = tailStart + i - part.length();
                    } else if (currAlias == null) {
                        currAlias = part;
                    }
                    buffer.delete(0, buffer.length());
                } else if (ch == ',') {
                    String part = buffer.toString();

                    if (currTable == null) {
                        currTable = part;
                        currTablePos = tailStart + i - part.length();
                    } else if (currAlias == null) {
                        currAlias = part;
                    }

                    ret.put(currTable, new Object[]{currTablePos, currAlias});

                    currTable = null;
                    currTablePos = -1;
                    currAlias = null;
                    buffer.delete(0, buffer.length());
                } else {
                    buffer.append(ch);
                }
            }

            String part = buffer.toString();
            if (!part.isEmpty()) {
                if (TABLE_ENDS.contains(part)) {
                    ret.put(currTable, new Object[]{currTablePos, currAlias});
                }

                if (currTable == null) {
                    currTable = part;
                    currTablePos = sql.length() - part.length();
                } else if (currAlias == null) {
                    currAlias = part;
                }

                if (currTable != null) {
                    ret.put(currTable, new Object[]{currTablePos, currAlias});
                }
            }

            return ret;
        }

        return LinkedListMultimap.create();
    }

    public String aliasByField(String fieldName) {
        if (fields != null) {
            SelectExpressionItem item = fields.get(fieldName);
            if (item != null) {
                Alias alias = item.getAlias();
                return alias == null ? null : alias.getName();
            }
        }
        return null;
    }

    /**
     * get select fields from sql
     *
     * @return (field -> alias)
     */
    private void initFields() {
        final LinkedHashMap<String, SelectExpressionItem> fields = new LinkedHashMap<String, SelectExpressionItem>();
        select.getSelectBody().accept(new SelectVisitorAdapter() {
            @Override
            public void visit(PlainSelect plainSelect) {
                for (SelectItem i : plainSelect.getSelectItems()) {
                    i.accept(new SelectItemVisitorAdapter() {

                        public void visit(SelectExpressionItem selectExpressionItem) {
                            Expression expression = selectExpressionItem.getExpression();
                            fields.put(removeHeadAfterDot(expression.toString()),
                                    selectExpressionItem);

                            // init functions
                            if (expression instanceof Function) {
                                Function funcField = (Function) expression;

                                if (functions == null) functions = Maps.newHashMapWithExpectedSize(4);

                                functions.put(funcField, selectExpressionItem.getAlias() == null ?
                                        null : selectExpressionItem.getAlias().getName());
                            }
                        }
                    });
                }
            }
        });

        this.fields = fields;
    }


    private void initGroupBy() {
        select.getSelectBody().accept(new SelectVisitorAdapter() {
            @Override
            public void visit(PlainSelect plainSelect) {
                if (plainSelect.getGroupByColumnReferences() != null) {
                    groupBys = Lists.newLinkedList();

                    for (Expression expr : plainSelect.getGroupByColumnReferences()) {
                        expr.accept(new ExpressionVisitorAdapter() {

                            @Override
                            public void visit(Column column) {
                                groupBys.add(column);
                            }
                        });
                    }
                }
            }
        });
    }

    // validate if sql is supported
    private void validate() {
        // if have group by, check select fields
        if (this.groupBys != null) {
            for (SelectExpressionItem expressionItem : this.fields.values()) {
                if (expressionItem.getExpression() instanceof Column) {
                    Column selCol = (Column) expressionItem.getExpression();

                    String field = selCol.getColumnName();
                    String alias = expressionItem.getAlias() == null ? null : expressionItem.getAlias().getName();
                    boolean exists = false;

                    for (Column groupByCol : this.groupBys) {
                        if (groupByCol.getColumnName().equals(field) || groupByCol.getColumnName().equals(alias)) {
                            exists = true;
                            break;
                        }
                    }

                    if (!exists) {
                        throw new IllegalStateException("select cols not exist in group by cols, sql: " + sql);
                    }
                }
            }
        }
    }


    private static LoadingCache<Tuple<String, Object[]>, Optional<Sql>> sqlCache = CacheBuilder.newBuilder()
            .maximumSize(1000)
            .refreshAfterWrite(1, TimeUnit.DAYS)
            .build(new CacheLoader<Tuple<String, Object[]>, Optional<Sql>>() {
                @Override
                public Optional<Sql> load(Tuple<String, Object[]> reprAndArgs) throws Exception {

                    Sql sql = new Sql(reprAndArgs._1(), reprAndArgs._2());
                    if (sql.partitionedTables != null) {
                        return Optional.of(sql);
                    }
                    return Optional.absent();
                }
            });


    /**
     * create Sql from string
     */
    public static Optional<Sql> fromString(String repr, Object... args) {
        return sqlCache.getUnchecked(Tuples.of(repr, args));
    }

    private static String removeHeadAfterDot(String repr) {
        int dotIdx = -1;
        if ((dotIdx = repr.indexOf(".")) > -1 && !repr.contains("(")) {
            return repr.substring(dotIdx + 1, repr.length());
        }
        return repr;
    }

    /**
     * pattern that matches order by
     */
    private static final Pattern ORDER_BY_PATTERN = Pattern.compile("\\s+ORDER\\s+BY\\s+(\\S+)(\\s+(ASC|DESC))?", Pattern.CASE_INSENSITIVE);

    /**
     * pattern that matches order by
     */
    private static final Pattern ORDER_BY_NOT_AFTER_PATTERN = Pattern.compile("\\s+(LIMIT|;)", Pattern.CASE_INSENSITIVE);


    private void initOrderBy() {
        this.orderBy = this.orderBy().orNull();
    }

    /**
     * extract order by field
     *
     * @return (field, desc)
     */
    private Optional<Tuple<String, Boolean>> orderBy() {
        Matcher matcher = ORDER_BY_PATTERN.matcher(sql);
        if (matcher.find()) {
            String field = matcher.group(1);
            field = removeHeadAfterDot(field);

            boolean desc = matcher.group(3) != null && matcher.group(3).toLowerCase().equals("desc");

            return Optional.of(Tuples.of(field, desc));
        }
        return Optional.absent();
    }

    /**
     * sql with no order by
     */
    public Sql withoutOrderBy() {
        return new Sql(excludedFrom(sql, ORDER_BY_PATTERN, ORDER_BY_NOT_AFTER_PATTERN), args, fields, functions, groupBys, orderBy, limit);
    }

    /**
     * extract a Map comparator from sql
     * result for each query must have sort field otherwise no sort is applied
     */
    public <T> Comparator<T> extractComparator(Class<T> cls) {
        if (orderBy != null) {
            final String field = orderBy._1();
            final boolean desc = orderBy._2();

            if (cls == Map.class) {

                return new Comparator<T>() {
                    public int compare(T o1, T o2) {
                        Object f1 = ((Map<String, Object>) o1).get(field);
                        Object f2 = ((Map<String, Object>) o2).get(field);

                        int abs = 0;

                        if (f1 == null && f2 == null) {
                            abs = 0;
                        } else if (f1 == null) {
                            abs = -1;
                        } else if (f2 == null) {
                            abs = 1;
                        } else if (f1 instanceof Number) {
                            abs = (int) (((Number) f1).doubleValue() - ((Number) f2).doubleValue());
                        } else if (f1 instanceof Comparable)
                            abs = ((Comparable) f1).compareTo(f2);

                        return desc ? -abs : abs;
                    }
                };
            } else if (cls == SqlRowSet.class) {

                return new Comparator<T>() {
                    public int compare(T o1, T o2) {
                        SqlRowSet r1 = (SqlRowSet) o1;
                        SqlRowSet r2 = (SqlRowSet) o2;
                        Object l = r1.getObject(field);
                        Object r = r2.getObject(field);

                        int abs = 0;
                        if (l instanceof Number) {
                            abs = (int) (((Number) l).doubleValue() - ((Number) r).doubleValue());
                        } else if (l instanceof Comparable) {
                            abs = ((Comparable) l).compareTo(r);
                        }
                        return desc ? -abs : abs;
                    }
                };
            } else if (Comparable.class.isAssignableFrom(cls)) {

                return new Comparator<T>() {
                    public int compare(T o1, T o2) {
                        Comparable d1 = (Comparable) o1;
                        Comparable d2 = (Comparable) o2;
                        return desc ? d1.compareTo(d2) : d2.compareTo(d1);
                    }
                };
            }
        }
        return null;
    }

    /**
     * see if sql contains sum(), count() whose results can be added
     */
    public boolean canAddResult() {
        return fields != null &&
                fields.size() == 1 &&
                functions != null &&
                Sets.filter(functions.keySet(), new Predicate<Function>() {
                    public boolean apply(Function function) {
                        return function.getName().equalsIgnoreCase("sum") ||
                                function.getName().equalsIgnoreCase("count");
                    }
                }).size() > 0;
    }


    /**
     * see if there is function in sql
     */
    public boolean haveFunctions() {
        return functions != null && !functions.isEmpty();
    }

    /**
     * return a sql using local cache
     */
    public Sql cached() {
        return new CachedSql(sql, args, fields, functions, groupBys, orderBy, limit);
    }

    public String toString() {
        return Joiner.on("\n").join(subSqls()) + "\nLimit:" + limit() + "\nGroup by:" + groupBys + "\nFields:" + fields;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Sql sql1 = (Sql) o;

        if (sql != null ? !sql.equals(sql1.sql) : sql1.sql != null) return false;
        if (sqlWithPlaceholder != null ? !sqlWithPlaceholder.equals(sql1.sqlWithPlaceholder) : sql1.sqlWithPlaceholder != null)
            return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if (!Arrays.equals(args, sql1.args)) return false;
        if (limit != null ? !limit.equals(sql1.limit) : sql1.limit != null) return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        if (!Arrays.equals(partitionedTables, sql1.partitionedTables)) return false;
        if (tables != null ? !tables.equals(sql1.tables) : sql1.tables != null) return false;
        if (fields != null ? !fields.equals(sql1.fields) : sql1.fields != null) return false;
        if (orderBy != null ? !orderBy.equals(sql1.orderBy) : sql1.orderBy != null) return false;
        if (functions != null ? !functions.equals(sql1.functions) : sql1.functions != null) return false;
        if (groupBys != null ? !groupBys.equals(sql1.groupBys) : sql1.groupBys != null) return false;
        return type == sql1.type;

    }

    @Override
    public int hashCode() {
        int result = sql != null ? sql.hashCode() : 0;
        result = 31 * result + (sqlWithPlaceholder != null ? sqlWithPlaceholder.hashCode() : 0);
        result = 31 * result + Arrays.hashCode(args);
        result = 31 * result + (limit != null ? limit.hashCode() : 0);
        result = 31 * result + Arrays.hashCode(partitionedTables);
        result = 31 * result + (tables != null ? tables.hashCode() : 0);
        result = 31 * result + (fields != null ? fields.hashCode() : 0);
        result = 31 * result + (orderBy != null ? orderBy.hashCode() : 0);
        result = 31 * result + (functions != null ? functions.hashCode() : 0);
        result = 31 * result + (groupBys != null ? groupBys.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        return result;
    }
}
