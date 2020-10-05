package com.scy.zookeeper.listener;

import com.scy.core.CollectionUtil;
import com.scy.core.StringUtil;
import com.scy.zookeeper.ZkClient;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.zookeeper.WatchedEvent;

import java.util.List;

/**
 * CuratorListener
 *
 * @author shichunyang
 * Created by shichunyang on 2020/10/4.
 */
@EqualsAndHashCode
@AllArgsConstructor
public class CuratorListener implements CuratorCacheListener, CuratorWatcher {

    private final ZkClient zkClient;

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

    @Override
    public void process(WatchedEvent event) throws Exception {
        String path = event.getPath();
        if (StringUtil.isEmpty(path)) {
            return;
        }
        List<String> children = zkClient.getChildrenAndAddListener(path, this);
        if (CollectionUtil.isEmpty(children)) {
            return;
        }
        dataListener.childrenChange(children);
    }
}
