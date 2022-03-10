package com.scy.zookeeper.config;

import com.fasterxml.jackson.core.type.TypeReference;
import com.scy.core.ArrayUtil;
import com.scy.core.CollectionUtil;
import com.scy.core.ObjectUtil;
import com.scy.core.StringUtil;
import com.scy.core.json.JsonUtil;
import com.scy.core.thread.ThreadPoolUtil;
import com.scy.zookeeper.ZkClient;
import com.scy.zookeeper.listener.CuratorListener;
import com.scy.zookeeper.listener.DataListener;
import com.scy.zookeeper.model.AddressDataBO;
import com.scy.zookeeper.model.RegisterCenterData;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.*;
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

    public static final TypeReference<AddressDataBO> ADDRESS_DATA_TYPE_REFERENCE = new TypeReference<AddressDataBO>() {
    };

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

        refreshDiscoveryData(registerCenterData.getServiceKey());
    }

    private void refreshDiscoveryData(String key) {
        Set<String> keys = new HashSet<>();
        if (!StringUtil.isEmpty(key)) {
            keys.add(key);
        } else {
            if (!CollectionUtil.isEmpty(discoveryData)) {
                keys.addAll(discoveryData.keySet());
            }
        }

        keys.forEach(serviceKey -> {
            String servicePath = serviceKeyToPath(serviceKey);
            List<String> addresses = zkClient.getChildren(servicePath);
            if (CollectionUtil.isEmpty(addresses)) {
                return;
            }

            TreeSet<String> addressSet = addresses.stream().filter(address -> {
                String addressPath = servicePath + "/" + address;
                String addressData = zkClient.doGetContent(addressPath);
                if (StringUtil.isEmpty(addressData)) {
                    return Boolean.FALSE;
                }

                AddressDataBO addressDataBO = JsonUtil.json2Object(addressData, ADDRESS_DATA_TYPE_REFERENCE);
                if (ObjectUtil.isNull(addressDataBO) || ObjectUtil.isNull(addressDataBO.getEnable())) {
                    return Boolean.FALSE;
                }

                return addressDataBO.getEnable();
            }).collect(TreeSet::new, TreeSet::add, TreeSet::addAll);

            if (CollectionUtil.isEmpty(addressSet)) {
                return;
            }

            discoveryData.put(serviceKey, addressSet);
        });
    }
}
