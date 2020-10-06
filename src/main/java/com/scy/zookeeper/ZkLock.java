package com.scy.zookeeper;

import com.scy.core.ObjectUtil;
import com.scy.core.StringUtil;
import com.scy.core.format.DateUtil;
import com.scy.core.format.MessageUtil;
import com.scy.core.thread.ThreadLocalUtil;
import com.scy.zookeeper.listener.CuratorListener;
import com.scy.zookeeper.listener.DataListener;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * ZkLock
 *
 * @author shichunyang
 * Created by shichunyang on 2020/10/5.
 */
@Slf4j
public class ZkLock {

    public static final String LOCK_BASE_PATH = "/distributedlocks/";

    public static final String ZK_LOCK_PATH = "zk_lock_path";

    private final ZkClient zkClient;

    private final Executor executor;

    private CuratorListener curatorListener;

    private volatile CountDownLatch countDownLatch;

    public ZkLock(ZkClient zkClient, Executor executor) {
        this.zkClient = zkClient;
        this.executor = executor;
    }

    public void lock(String key) {
        String path = LOCK_BASE_PATH + key;

        ThreadLocalUtil.put(ZK_LOCK_PATH, path);

        String lockNode = zkClient.createNode(path, DateUtil.getCurrentDateStr(), CreateMode.EPHEMERAL);
        if (!StringUtil.isEmpty(lockNode)) {
            log.info(MessageUtil.format("zk lock success", "path", path));
            return;
        }

        this.countDownLatch = new CountDownLatch(1);
        this.curatorListener = new CuratorListener(zkClient, new DataListener() {

            @Override
            public void delete(String deletePath, String data) {
                if (!ObjectUtil.equals(path, deletePath)) {
                    return;
                }
                if (countDownLatch != null) {
                    countDownLatch.countDown();
                }
            }
        });
        zkClient.addListener(path, curatorListener, executor);

        int count = 2;

        while (true) {
            try {
                lockNode = zkClient.createNode(path, DateUtil.getCurrentDateStr(), CreateMode.EPHEMERAL);
                if (!StringUtil.isEmpty(lockNode)) {
                    log.info(MessageUtil.format("zk lock success", "path", path, "count", count));
                    return;
                }
                this.countDownLatch.await(3_000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                log.warn(MessageUtil.format("zk lock interrupted", "path", path, "count", count));
            } finally {
                this.countDownLatch = new CountDownLatch(1);
            }
            count++;
        }
    }

    public void unlock() {
        this.countDownLatch = null;

        String lockPath = (String) ThreadLocalUtil.get(ZK_LOCK_PATH);
        zkClient.delete(lockPath);

        if (!ObjectUtil.isNull(this.curatorListener)) {
            zkClient.removeListener(this.curatorListener);
        }
    }
}
