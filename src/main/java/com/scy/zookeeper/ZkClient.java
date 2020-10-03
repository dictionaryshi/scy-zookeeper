package com.scy.zookeeper;

import com.scy.core.format.MessageUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;

/**
 * ZkClient
 *
 * @author shichunyang
 * Created by shichunyang on 2020/10/3.
 */
@Slf4j
public class ZkClient {

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

    /**
     * 创建节点(若节点存在则创建失败, 不可用于更新节点)
     */
    public String createNode(String path, String data, CreateMode createMode) {
        try {
            byte[] dataBytes = data.getBytes(StandardCharsets.UTF_8);
            String createPath = curatorFramework.create().creatingParentContainersIfNeeded().withMode(createMode).forPath(path, dataBytes);
            log.info(MessageUtil.format("createNode success", "createPath", createPath));
            return createPath;
        } catch (KeeperException.NodeExistsException e) {
            log.warn(MessageUtil.format("createNode fail, path已存在", "path", path));
            return null;
        } catch (Exception e) {
            log.error(MessageUtil.format("createNode error", e, "path", path, "data", data));
            return null;
        }
    }

    /**
     * 创建节点并赋值(可用于更新节点)
     */
    public String createNodeWithData(String path, String data, CreateMode createMode) {
        try {
            byte[] dataBytes = data.getBytes(StandardCharsets.UTF_8);
            String createPath = curatorFramework.create().orSetData().creatingParentContainersIfNeeded().withMode(createMode).forPath(path, dataBytes);
            log.info(MessageUtil.format("createNodeWithData success", "createPath", createPath));
            return createPath;
        } catch (Exception e) {
            log.error(MessageUtil.format("createNodeWithData error", e, "path", path, "data", data));
            return null;
        }
    }

    /**
     * 判断path是否已创建
     *
     * @return true 已创建
     */
    public boolean checkExists(String path) {
        try {
            if (curatorFramework.checkExists().forPath(path) != null) {
                return Boolean.TRUE;
            }
        } catch (Exception e) {
            log.error(MessageUtil.format("checkExists error", e, "path", path));
        }
        return Boolean.FALSE;
    }
}
