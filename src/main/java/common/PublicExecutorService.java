package common;

import com.google.common.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

/**
 * Created by guohang.bao on 14-8-1.
 */
public class PublicExecutorService {

    private Logger logger = LoggerFactory.getLogger(PublicExecutorService.class);

    private ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());

    public <T> ListenableFuture<T> submit(Callable<T> callable, FutureCallback<T> callback) {
        ListenableFuture<T> future = executorService.submit(callable);
        logger.debug("new task submitted: {}", callable);
        Futures.addCallback(future, callback);
        return future;
    }

}
