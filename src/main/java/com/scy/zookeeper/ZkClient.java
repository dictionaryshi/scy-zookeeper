package com.scy.zookeeper;

import com.scy.core.format.MessageUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;

import java.util.concurrent.CountDownLatch;

/**
 * ZkClient
 *
 * @author shichunyang
 * Created by shichunyang on 2020/10/3.
 */
@Slf4j
public class ZkClient {

    private volatile static ZkClient zkClient;

    private final CuratorFramework curatorFramework;

    private static final CountDownLatch CONNECTED_SEMAPHORE = new CountDownLatch(1);

    public ZkClient(String namespace) {
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder()
                .connectString("127.0.0.1:2181")
                .retryPolicy(new RetryNTimes(1, 5_000))
                .namespace(namespace);
        curatorFramework = builder.build();
        curatorFramework.getConnectionStateListenable().addListener((curatorFramework, newState) -> {
            log.info(MessageUtil.format("zkClient state listener", "newState", newState.name()));
            if (newState.isConnected()) {
                CONNECTED_SEMAPHORE.countDown();
            }
        });
        curatorFramework.start();

        try {
            CONNECTED_SEMAPHORE.await();
        } catch (InterruptedException e) {
            log.warn("zkClient init interrupted");
        }
    }
}
