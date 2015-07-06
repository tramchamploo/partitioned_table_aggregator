package id_gen;

/**
 * Created by guohang.bao on 15-6-10.
 */
public final class ZookeeperIdGeneratorConfig extends IdGeneratorConfig {

    /**
     * 乐观锁提升为悲观锁的锁路径
     */
    private String promotedLockPath;

    /**
     * 表示zookeeper状态在redis中的key
     */
    private String zookeeperStatusRedisKey;

    private ZookeeperIdGeneratorConfig(Builder builder) {
        super(builder);
        this.promotedLockPath = builder.promotedLockPath;
        this.zookeeperStatusRedisKey = builder.zookeeperStatusRedisKey;
    }

    public String getPromotedLockPath() {
        return promotedLockPath;
    }

    public String getZookeeperStatusRedisKey() {
        return zookeeperStatusRedisKey;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends BaseBuilder<Builder> {

        private String promotedLockPath;

        private String zookeeperStatusRedisKey;

        public Builder promotedLockPath(String promotedLockPath) {
            this.promotedLockPath = promotedLockPath;
            return this;
        }

        public Builder zookeeperStatusRedisKey(String zookeeperStatusRedisKey) {
            this.zookeeperStatusRedisKey = zookeeperStatusRedisKey;
            return this;
        }

        public ZookeeperIdGeneratorConfig build() {
            return new ZookeeperIdGeneratorConfig(this);
        }

        @Override
        public Builder self() {
            return this;
        }
    }

    @Override
    public String toString() {
        return "ZookeeperIdGeneratorConfig{" + super.toString() +
                ", promotedLockPath='" + promotedLockPath + '\'' +
                ", zookeeperStatusRedisKey='" + zookeeperStatusRedisKey + '\'' +
                '}';
    }
}
