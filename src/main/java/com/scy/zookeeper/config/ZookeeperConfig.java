package com.scy.zookeeper.config;

import com.scy.core.spring.ApplicationContextUtil;
import com.scy.core.thread.ThreadPoolUtil;
import com.scy.zookeeper.ZkClient;
import org.springframework.context.annotation.Bean;

import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * ZookeeperConfig
 *
 * @author shichunyang
 * Created by shichunyang on 2020/10/5.
 */
public class ZookeeperConfig {

    @Bean(destroyMethod = "close")
    public ZkClient zkClient() {
        String applicationName = ApplicationContextUtil.getApplicationName();
        return new ZkClient(applicationName);
    }

    @Bean
    public ScheduledThreadPoolExecutor zookeeperScheduledThreadPoolExecutor() {
        return ThreadPoolUtil.getScheduledPool("zookeeper-time-monitor", 1);
    }

    @Bean(initMethod = "init")
    public SystemTimeMonitor systemTimeMonitor(ZkClient zkClient, ScheduledThreadPoolExecutor zookeeperScheduledThreadPoolExecutor) {
        return new SystemTimeMonitor(zkClient, zookeeperScheduledThreadPoolExecutor);
    }
}
