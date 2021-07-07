package com.scy.zookeeper.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @author : shichunyang
 * Date    : 2021/7/7
 * Time    : 4:44 下午
 * ---------------------------------------
 * Desc    : 配置中心
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface ConfigCenter {

    String key();
}
