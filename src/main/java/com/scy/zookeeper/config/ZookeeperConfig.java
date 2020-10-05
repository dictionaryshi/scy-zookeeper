package com.scy.zookeeper.config;

import com.scy.core.spring.ApplicationContextUtil;
import com.scy.zookeeper.ZkClient;
import org.springframework.context.annotation.Bean;

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
}
