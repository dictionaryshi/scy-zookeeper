package com.scy.zookeeper.annotation;

import java.lang.annotation.*;

/**
 * @author : shichunyang
 * Date    : 2021/7/7
 * Time    : 4:44 下午
 * ---------------------------------------
 * Desc    : 配置中心
 */
@Inherited
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface ConfigCenter {

    String key();
}
