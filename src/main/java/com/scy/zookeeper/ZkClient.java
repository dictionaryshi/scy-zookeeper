package com.scy.zookeeper;

import com.scy.core.CollectionUtil;
import com.scy.core.StringUtil;
import com.scy.core.format.MessageUtil;
import com.scy.zookeeper.listener.CuratorListener;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;

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

    private final Map<CuratorListener, CuratorCache> curatorCacheMap = new ConcurrentHashMap<>();

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

    /**
     * 查询节点数据
     */
    public String doGetContent(String path) {
        try {
            byte[] dataBytes = curatorFramework.getData().forPath(path);
            return dataBytes == null ? StringUtil.EMPTY : new String(dataBytes, StandardCharsets.UTF_8);
        } catch (KeeperException.NoNodeException e) {
            log.warn(MessageUtil.format("doGetContent 节点不存在", "path", path));
            return StringUtil.EMPTY;
        } catch (Exception e) {
            log.error(MessageUtil.format("doGetContent error", e, "path", path));
            return StringUtil.EMPTY;
        }
    }

    /**
     * 查询子节点
     */
    public List<String> getChildren(String path) {
        try {
            return curatorFramework.getChildren().forPath(path);
        } catch (KeeperException.NoNodeException e) {
            log.warn(MessageUtil.format("getChildren path不存在", "path", path));
            return CollectionUtil.emptyList();
        } catch (Exception e) {
            log.error(MessageUtil.format("getChildren error", e, "path", path));
            return CollectionUtil.emptyList();
        }
    }

    /**
     * 删除节点(同时删除子节点)
     *
     * @return true 删除成功
     */
    public boolean delete(String path) {
        try {
            curatorFramework.delete().guaranteed().deletingChildrenIfNeeded().forPath(path);
            return Boolean.TRUE;
        } catch (KeeperException.NoNodeException e) {
            log.warn(MessageUtil.format("delete path不存在", "path", path));
            return Boolean.FALSE;
        } catch (Exception e) {
            log.error(MessageUtil.format("delete error", e, "path", path));
            return Boolean.FALSE;
        }
    }

    /**
     * 添加监听
     */
    public void addListener(String path, CuratorListener listener, Executor executor) {
        CuratorCache curatorCache = CuratorCache.build(curatorFramework, path);
        curatorCacheMap.putIfAbsent(listener, curatorCache);
        curatorCache.listenable().addListener(listener, executor);
        curatorCache.start();
    }

    /**
     * 删除监听
     */
    public void removeListener(CuratorListener listener) {
        CuratorCache curatorCache = curatorCacheMap.get(listener);
        if (Objects.isNull(curatorCache)) {
            return;
        }
        curatorCache.listenable().removeListener(listener);
        curatorCache.close();
        curatorCacheMap.remove(listener);
    }
}
