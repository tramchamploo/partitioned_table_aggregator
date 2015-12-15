package bean;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Created by guohang.bao on 15-6-29.
 * Currently used sub tables
 */
public class SubTables {

    public static final PartitionedTable FORUM_POST = new PartitionedTable("forum_post_p", 0, 19);

    public static final PartitionedTable FORUM_POST_CONTENT = new PartitionedTable("forum_post_content_p", 0, 19);

    public static final Map<String, PartitionedTable> PARTITIONED_TABLES = ImmutableMap.
            of("forum_post", FORUM_POST,
                    "forum_post_content", FORUM_POST_CONTENT);


    public static PartitionedTable forumPostPart(int index) {
        return new PartitionedTable("forum_post_p", index);
    }

    /**
     * used when data is located in only one table of subtables
     */
    public static PartitionedTable forumPostPartByRootId(long rootId) {
        return forumPostPart((int) (rootId % 20));
    }

    public static PartitionedTable forumPostContentPart(int index) {
        return new PartitionedTable("forum_post_content_p", index);
    }

    public static PartitionedTable forumPostContentPartByRootId(long rootId) {
        return forumPostContentPart((int) (rootId % 20));
    }

}
