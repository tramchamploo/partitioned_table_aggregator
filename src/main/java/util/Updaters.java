package util;

/**
 * Created by guohang.bao on 15-7-21.
 */
public class Updaters {

    public static class ForumPostUpdater extends UpdaterProxy {

        public ForumPostUpdater(DoubleWriteJdbcTemplate doubleWriteJdbcTemplate) {
            super(doubleWriteJdbcTemplate, "forum_post");
        }
    }

    public static class ForumPostContentUpdater extends UpdaterProxy {

        public ForumPostContentUpdater(DoubleWriteJdbcTemplate doubleWriteJdbcTemplate) {
            super(doubleWriteJdbcTemplate, "forum_post_content");
        }
    }
}
