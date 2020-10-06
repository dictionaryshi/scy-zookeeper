package com.scy.zookeeper.config;

import com.scy.core.CollectionUtil;
import com.scy.core.ObjectUtil;
import com.scy.core.StringUtil;
import com.scy.core.enums.ResponseCodeEnum;
import com.scy.core.exception.BusinessException;
import com.scy.core.format.MessageUtil;
import com.scy.core.reflect.AnnotationUtil;
import com.scy.core.reflect.ReflectionsUtil;
import com.scy.core.spring.ApplicationContextUtil;
import com.scy.zookeeper.ZkClient;
import com.scy.zookeeper.listener.CuratorListener;
import com.scy.zookeeper.listener.DataListener;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.lang.NonNull;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * DynamicConfiguration
 *
 * @author shichunyang
 * Created by shichunyang on 2020/10/6.
 */
@Slf4j
public class DynamicConfiguration implements BeanPostProcessor {

    public static final String APPLICATION_CONFIG_PATH = "/application/config/dynamic_configuration";

    public static final Map<Field, Object> VALUE_BEAN_MAP = CollectionUtil.newHashMap();

    private final ZkClient zkClient;

    private final Executor executor;

    public DynamicConfiguration(ZkClient zkClient, Executor executor) {
        this.zkClient = zkClient;
        this.executor = executor;
    }

    @Override
    public Object postProcessBeforeInitialization(@NonNull Object bean, @NonNull String beanName) throws BeansException {
        ReflectionsUtil.doWithFields(bean.getClass(), field -> VALUE_BEAN_MAP.put(field, bean), field -> !Objects.isNull(AnnotationUtil.findAnnotation(field, Value.class)));
        return bean;
    }

    public void init() {
        String configName = "dynamic_configuration";

        // 配置服务数据
        Map<String, Object> dataMap = this.getData();
        if (CollectionUtil.isEmpty(dataMap)) {
            return;
        }

        check(dataMap);

        ApplicationContextUtil.addLastMapPropertySource(configName, dataMap);

        addListener(configName);
    }

    private void addListener(String configName) {
        String path = APPLICATION_CONFIG_PATH;
        CuratorListener curatorListener = new CuratorListener(zkClient, new DataListener() {
            @Override
            public void update(String updatePath, String oldData, String newData) {
                if (!ObjectUtil.equals(updatePath, path) && updatePath.startsWith(path)) {
                    this.update(configName);
                }
            }

            private void update(String configName) {
                Map<String, Object> dataMap = getData();
                if (CollectionUtil.isEmpty(dataMap)) {
                    return;
                }
                ApplicationContextUtil.replaceMapPropertySource(configName, dataMap);

                VALUE_BEAN_MAP.forEach((field, bean) -> {
                    Value valueAnnotation = AnnotationUtil.findAnnotation(field, Value.class);
                    if (ObjectUtil.isNull(valueAnnotation)) {
                        return;
                    }

                    String key = valueAnnotation.value();
                    String valueKey = StringUtil.parseSpringValue(key);
                    if (StringUtil.isEmpty(valueKey)) {
                        return;
                    }

                    Object value = dataMap.get(valueKey);
                    if (ObjectUtil.isNull(value)) {
                        return;
                    }

                    try {
                        field.setAccessible(Boolean.TRUE);
                        field.set(bean, value);
                    } catch (Exception e) {
                        log.error(MessageUtil.format("动态更新配置error", e, "bean", bean.getClass().getName(), "field", field.getName()));
                    }
                });
            }
        });
        zkClient.addListener(path, curatorListener, executor);
    }

    private void check(Map<String, Object> dataMap) {
        MutablePropertySources propertySources = ApplicationContextUtil.getMutablePropertySources();
        propertySources.stream().forEach(propertySource -> dataMap.keySet().forEach(key -> {
            boolean isExist = propertySource.containsProperty(key);
            if (isExist) {
                throw new BusinessException(ResponseCodeEnum.SYSTEM_EXCEPTION.getCode(), MessageUtil.format("禁止覆盖配置", "key", key));
            }
        }));
    }

    private Map<String, Object> getData() {
        String path = APPLICATION_CONFIG_PATH;
        List<String> children = zkClient.getChildren(path);
        if (CollectionUtil.isEmpty(children)) {
            return CollectionUtil.emptyMap();
        }
        return children.stream().collect(Collectors.toMap(child -> child, child -> zkClient.doGetContent(path + "/" + child)));
    }
}
