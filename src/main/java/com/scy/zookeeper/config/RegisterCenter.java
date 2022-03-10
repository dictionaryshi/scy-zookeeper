package com.scy.zookeeper.config;

import com.scy.core.ArrayUtil;
import com.scy.core.CollectionUtil;
import com.scy.core.ObjectUtil;
import com.scy.core.StringUtil;
import com.scy.core.thread.ThreadPoolUtil;
import com.scy.zookeeper.ZkClient;
import com.scy.zookeeper.listener.CuratorListener;
import com.scy.zookeeper.listener.DataListener;
import com.scy.zookeeper.model.RegisterCenterData;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;
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

    private RegisterCenterData getRegisterCenterData(String path) {
        if (StringUtil.isEmpty(path)) {
            return null;
        }

        if (path.length() <= envPath.length()) {
            return null;
        }

        String[] dataArr = path.substring(envPath.length() + 1).split("/");
        if (ArrayUtil.isEmpty(dataArr) || ArrayUtil.getLength(dataArr) < 2) {
            return null;
        }

        RegisterCenterData registerCenterData = new RegisterCenterData();
        registerCenterData.setServiceKey(dataArr[0]);
        registerCenterData.setAddress(dataArr[1]);
        return registerCenterData;
    }

    public void init() {
        CuratorListener curatorListener = new CuratorListener(zkClient, new DataListener() {

            @Override
            public void add(String path, String data) {
                listener(path);
            }

            @Override
            public void update(String path, String oldData, String newData) {
                listener(path);
            }

            @Override
            public void delete(String path, String data) {
                listener(path);
            }
        });
        zkClient.addListener(envPath, curatorListener, ThreadPoolUtil.getThreadPool("registerCenter-pool", 10, 10, 1024));
    }

    private void listener(String path) {
        RegisterCenterData registerCenterData = getRegisterCenterData(path);
        if (ObjectUtil.isNull(registerCenterData)) {
            return;
        }

        String servicePath = serviceKeyToPath(registerCenterData.getServiceKey());
        List<String> addresses = zkClient.getChildren(servicePath);
        if (CollectionUtil.isEmpty(addresses)) {
            return;
        }

        addresses.forEach(address -> {
            String addressPath = servicePath + "/" + address;
            String addressData = zkClient.doGetContent(addressPath);
        });
    }
}
