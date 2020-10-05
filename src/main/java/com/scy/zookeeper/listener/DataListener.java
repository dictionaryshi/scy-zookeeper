package com.scy.zookeeper.listener;

import java.util.List;

/**
 * DataListener
 *
 * @author shichunyang
 * Created by shichunyang on 2020/10/4.
 */
public interface DataListener {

    /**
     * 添加节点
     *
     * @param path 节点
     * @param data 节点数据
     */
    default void add(String path, String data) {
    }

    /**
     * 修改节点
     *
     * @param path    节点
     * @param oldData 修改前数据
     * @param newData 修改后数据
     */
    default void update(String path, String oldData, String newData) {
    }

    /**
     * 删除节点
     *
     * @param path 节点
     * @param data 节点数据
     */
    default void delete(String path, String data) {
    }

    /**
     * 子节点发生改变
     *
     * @param children 子节点
     */
    default void childrenChange(List<String> children) {
    }
}
