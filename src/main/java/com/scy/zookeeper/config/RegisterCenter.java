package com.scy.zookeeper.config;

import com.scy.core.thread.ThreadPoolUtil;
import com.scy.zookeeper.ZkClient;
import com.scy.zookeeper.listener.CuratorListener;
import com.scy.zookeeper.listener.DataListener;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author : shichunyang
 * Date    : 2022/3/9
 * Time    : 7:28 下午
 * ---------------------------------------
 * Desc    : RegisterCenter
 */
@Getter
@Setter
@ToString
public class RegisterCenter {

    private static final String BASE_PATH = "/scy-rpc";

    private volatile ConcurrentMap<String, TreeSet<String>> registryData = new ConcurrentHashMap<>();

    private volatile ConcurrentMap<String, TreeSet<String>> discoveryData = new ConcurrentHashMap<>();

    private final ZkClient zkClient;

    private String env;

    private String envPath;

    public RegisterCenter(ZkClient zkClient, String env) {
        this.zkClient = zkClient;
        this.env = env;

        envPath = BASE_PATH.concat("/").concat(env);
    }

    public String serviceKeyToPath(String serviceKey) {
        return envPath + "/" + serviceKey;
    }

    public void init() {
        CuratorListener curatorListener = new CuratorListener(zkClient, new DataListener() {

            @Override
            public void add(String path, String data) {
            }

            @Override
            public void update(String path, String oldData, String newData) {
            }

            @Override
            public void delete(String path, String data) {
            }
        });
        zkClient.addListener(envPath, curatorListener, ThreadPoolUtil.getThreadPool("registerCenter-pool", 10, 10, 1024));
    }
}
