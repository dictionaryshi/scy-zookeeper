package com.scy.zookeeper.listener;

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
}
