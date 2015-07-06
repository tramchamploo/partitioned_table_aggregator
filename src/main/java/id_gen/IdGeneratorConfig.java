package id_gen;


import common.Self;

/**
 * Created by guohang.bao on 15-6-10.
 */
public class IdGeneratorConfig {

    private String keyPath;

    private String paperboyLogCategory;

    private int nRetries;

    protected IdGeneratorConfig(BaseBuilder<?> builder) {
        this.keyPath = builder.keyPath;
        this.paperboyLogCategory = builder.paperboyLogCategory;
        this.nRetries = builder.nRetries;
    }

    public String getKeyPath() {
        return keyPath;
    }

    public String getPaperboyLogCategory() {
        return paperboyLogCategory;
    }

    public int getnRetries() {
        return nRetries;
    }

    public static BaseBuilder<?> builder() {
        return new Builder();
    }

    public static abstract class BaseBuilder<T extends BaseBuilder<T>> implements Self<T> {

        private String keyPath;

        private String paperboyLogCategory;

        private int nRetries;

        public T keyPath(String keyPath) {
            this.keyPath = keyPath;
            return self();
        }

        public T paperboyLogCategory(String paperboyLogCategory) {
            this.paperboyLogCategory = paperboyLogCategory;
            return self();
        }

        public T nRetries(int nRetries) {
            this.nRetries = nRetries;
            return self();
        }

        public IdGeneratorConfig build() {
            return new IdGeneratorConfig(this);
        }
    }

    private static class Builder extends BaseBuilder<Builder> {
        @Override
        public Builder self() {
            return this;
        }
    }

    @Override
    public String toString() {
        return "IdGeneratorConfig{" +
                "keyPath='" + keyPath + '\'' +
                ", paperboyLogCategory='" + paperboyLogCategory + '\'' +
                ", nRetries=" + nRetries +
                '}';
    }
}
