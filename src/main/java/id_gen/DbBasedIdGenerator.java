package id_gen;

/**
 * Created by guohang.bao on 15-4-28.
 */
public interface DbBasedIdGenerator extends IdGenerator {

    int SYNC_OFFSET = 10000;

    Long syncFromDB();

    void recover();

    Long maxIdFromDB();
}
