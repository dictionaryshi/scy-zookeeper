package com.scy.zookeeper.config;

import com.scy.core.format.MessageUtil;
import com.scy.core.net.NetworkInterfaceUtil;
import com.scy.zookeeper.ZkClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * SystemTimeMonitor
 *
 * @author shichunyang
 * Created by shichunyang on 2020/10/6.
 */
@Slf4j
public class SystemTimeMonitor {

    private final ZkClient zkClient;

    private final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

    private final String ip;

    private String zkAddressNode;

    private Long lastUpdateTime = 0L;

    public static final String PATH_FOREVER = "/snowflake/forever";

    public SystemTimeMonitor(ZkClient zkClient, ScheduledThreadPoolExecutor scheduledThreadPoolExecutor) {
        this.ip = NetworkInterfaceUtil.getIp();
        this.zkClient = zkClient;
        this.scheduledThreadPoolExecutor = scheduledThreadPoolExecutor;
    }

    public void init() {
        String listenPath = PATH_FOREVER + "/" + ip;
        boolean isExists = zkClient.checkExists(listenPath);
        if (!isExists) {
            // 第一次启动检测服务
            String data = String.valueOf(System.currentTimeMillis());
            this.zkAddressNode = zkClient.createNodeWithData(listenPath, data, CreateMode.PERSISTENT);

            log.info(MessageUtil.format("systemTimeMonitor first start", "zkAddressNode", zkAddressNode));

            this.scheduledUploadData();
            return;
        }

        this.zkAddressNode = listenPath;

        // 检查时间
        if (!checkTimeStamp()) {
            log.error(MessageUtil.format("systemTimeMonitor 服务时间回调", "zkAddressNode", zkAddressNode));
            return;
        }

        this.scheduledUploadData();
    }

    private boolean checkTimeStamp() {
        long zkAddressNodeTime = Long.parseLong(zkClient.doGetContent(this.zkAddressNode));
        return System.currentTimeMillis() >= zkAddressNodeTime;
    }

    private void scheduledUploadData() {
        scheduledThreadPoolExecutor.scheduleWithFixedDelay(this::updateNewData, 30, 300, TimeUnit.SECONDS);
    }

    private void updateNewData() {
        long timestamp = System.currentTimeMillis();
        if (timestamp < lastUpdateTime) {
            log.error(MessageUtil.format("systemTimeMonitor 服务时间回调", "zkAddressNode", zkAddressNode));
            return;
        }

        zkClient.createNodeWithData(this.zkAddressNode, String.valueOf(timestamp), CreateMode.PERSISTENT);

        lastUpdateTime = timestamp;
    }
}
