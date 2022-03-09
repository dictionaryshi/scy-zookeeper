package com.scy.zookeeper.config;

import com.scy.zookeeper.ZkClient;
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

    private String envPath;

    private final ZkClient zkClient;

    public RegisterCenter(ZkClient zkClient) {
        this.zkClient = zkClient;
    }

    public String serviceKeyToPath(String serviceKey) {
        return envPath + "/" + serviceKey;
    }
}
