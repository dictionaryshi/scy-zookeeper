package com.scy.zookeeper.config;

import com.scy.core.spring.ApplicationContextUtil;
import com.scy.core.thread.ThreadPoolUtil;
import com.scy.zookeeper.ZkClient;
import org.springframework.context.annotation.Bean;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;

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

    @Bean
    public ThreadPoolExecutor dynamicConfigThreadPoolExecutor() {
        return ThreadPoolUtil.getThreadPool("dynamicConfig", 1, 1, 10);
    }

    @Bean(initMethod = "init")
    public DynamicConfiguration dynamicConfiguration(ZkClient zkClient, ThreadPoolExecutor dynamicConfigThreadPoolExecutor) {
        return new DynamicConfiguration(zkClient, dynamicConfigThreadPoolExecutor);
    }

    @Bean(initMethod = "init")
    public RegisterCenter registerCenter(ZkClient zkClient) {
        return new RegisterCenter(zkClient, ApplicationContextUtil.getProperty(ApplicationContextUtil.ACTIVE));
    }
}
