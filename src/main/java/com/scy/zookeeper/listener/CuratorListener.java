package com.scy.zookeeper.listener;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;

/**
 * CuratorListener
 *
 * @author shichunyang
 * Created by shichunyang on 2020/10/4.
 */
@EqualsAndHashCode
@AllArgsConstructor
public class CuratorListener implements CuratorCacheListener {

    private final DataListener dataListener;

    @Override
    public void event(Type type, ChildData oldData, ChildData data) {
        switch (type) {
            case NODE_CREATED: {
                dataListener.add(data.getPath(), new String(data.getData()));
                return;
            }
            case NODE_CHANGED: {
                dataListener.update(oldData.getPath(), new String(oldData.getData()), new String(data.getData()));
                return;
            }
            case NODE_DELETED: {
                dataListener.delete(oldData.getPath(), new String(oldData.getData()));
                return;
            }
            default: {
                break;
            }
        }
    }
}
