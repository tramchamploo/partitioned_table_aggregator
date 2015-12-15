package util;

import com.google.common.util.concurrent.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.util.concurrent.Callable;

/**
 * Created by tramchamploo on 15-4-28.
 */
public class DoubleWriteJdbcTemplate {

    private JdbcTemplate jdbcTemplateLeft;
    private JdbcTemplate jdbcTemplateRight;

    private boolean useLeft;
    private boolean useRight;
    private boolean async;

    private Logger logger = LoggerFactory.getLogger(DoubleWriteJdbcTemplate.class);

    @Resource
    private PublicExecutorService publicExecutorService;

    public DoubleWriteJdbcTemplate(DataSource dataSourceLeft, DataSource dataSourceRight, boolean useLeft, boolean useRight, boolean async) {
        this.jdbcTemplateLeft = new JdbcTemplate(dataSourceLeft);
        this.jdbcTemplateRight = new JdbcTemplate(dataSourceRight);

        this.useLeft = useLeft;
        this.useRight = useRight;
        this.async = async;
    }

    public JdbcTemplate getJdbcTemplateLeft() {
        return jdbcTemplateLeft;
    }

    public JdbcTemplate getJdbcTemplateRight() {
        return jdbcTemplateRight;
    }

    public int doubleUpdate(Functions.Function1<JdbcTemplate, Integer> leftProcess, Functions.Function1<JdbcTemplate, Integer> rightProcess) {
        return async ? doubleUpdateAsync(leftProcess, rightProcess) : doubleUpdateSync(leftProcess, rightProcess);
    }

    private int doubleUpdateSync(Functions.Function1<JdbcTemplate, Integer> leftProcess, Functions.Function1<JdbcTemplate, Integer> rightProcess) {
        int ret = 0;
        if (useLeft) {
            ret += leftProcess.apply(jdbcTemplateLeft);
        }
        if (useRight) {
            ret += rightProcess.apply(jdbcTemplateRight);
        }
        return ret;
    }


    private int doubleUpdateAsync(Functions.Function1<JdbcTemplate, Integer> leftProcess, final Functions.Function1<JdbcTemplate, Integer> rightProcess) {
        int ret = 0;
        if (useLeft) {
            ret = leftProcess.apply(jdbcTemplateLeft);
        }
        if (useRight) {
            publicExecutorService.submit(new Callable<Integer>() {

                public Integer call() throws Exception {
                    return rightProcess.apply(jdbcTemplateRight);
                }
            }, new FutureCallback<Integer>() {

                public void onSuccess(Integer rows) {
                    logger.debug("{} rows affected.", rows);
                }


                public void onFailure(Throwable throwable) {
                    logger.error("doubleUpdateAsync error", throwable);
                }
            });
        }
        return ret;
    }
}
