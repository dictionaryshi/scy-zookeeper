package com.scy.zookeeper.config;

import com.scy.core.StringUtil;
import com.scy.core.exception.Try;
import com.scy.core.thread.ThreadPoolUtil;
import com.scy.zookeeper.ZkClient;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author : shichunyang
 * Date    : 2022/7/20
 * Time    : 8:27 下午
 * ---------------------------------------
 * Desc    : MqRegister
 */
@Slf4j
@Getter
@Setter
@ToString
public class MqRegister {

    private static final String BASE_PATH = "/scy-mq";

    private volatile ConcurrentMap<String, String> registryData = new ConcurrentHashMap<>();

    private final ZkClient zkClient;

    private String env;

    private String envPath;

    public MqRegister(ZkClient zkClient, String env) {
        this.zkClient = zkClient;
        this.env = env;

        envPath = BASE_PATH.concat("/").concat(env);
    }

    public String topicToPath(String topic) {
        return envPath.concat("/").concat(topic);
    }

    public void init() {
        ScheduledThreadPoolExecutor scheduledPool = ThreadPoolUtil.getScheduledPool("mqRegisterLoop", 5);
        scheduledPool.scheduleWithFixedDelay(() -> {
            Try.run(this::refreshRegistryData);
        }, 0, 60, TimeUnit.SECONDS);
    }

    public void refreshRegistryData() {
        registryData.forEach(this::registry);
    }

    private void registry(String topic, String group) {
        if (StringUtil.isEmpty(topic) || StringUtil.isEmpty(group)) {
            return;
        }

        String path = topicToPath(topic).concat("/").concat(group);

        zkClient.createNode(path, StringUtil.EMPTY, CreateMode.EPHEMERAL);

        registryData.putIfAbsent(topic, group);
    }

    public boolean remove(String topic, String group) {
        String path = topicToPath(topic).concat("/").concat(group);
        zkClient.delete(path);

        return Boolean.TRUE;
    }

    public List<String> discovery(String topic) {
        String topicPath = topicToPath(topic);
        return zkClient.getChildren(topicPath);
    }
}
