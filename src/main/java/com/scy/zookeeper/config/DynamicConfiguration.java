package com.scy.zookeeper.config;

import com.scy.core.CollectionUtil;
import com.scy.core.IOUtil;
import com.scy.core.ObjectUtil;
import com.scy.core.StringUtil;
import com.scy.core.enums.ResponseCodeEnum;
import com.scy.core.exception.BusinessException;
import com.scy.core.format.MessageUtil;
import com.scy.core.json.JsonUtil;
import com.scy.core.reflect.AnnotationUtil;
import com.scy.core.reflect.ReflectionsUtil;
import com.scy.core.spring.ApplicationContextUtil;
import com.scy.zookeeper.ZkClient;
import com.scy.zookeeper.annotation.ConfigCenter;
import com.scy.zookeeper.listener.CuratorListener;
import com.scy.zookeeper.listener.DataListener;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.lang.NonNull;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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

    public static final Set<Field> FIELDS;

    static {
        FIELDS = ReflectionsUtil.getFieldsAnnotatedWith(ConfigCenter.class);
    }

    public DynamicConfiguration(ZkClient zkClient, Executor executor) {
        this.zkClient = zkClient;
        this.executor = executor;
    }

    @Override
    public Object postProcessBeforeInitialization(@NonNull Object bean, @NonNull String beanName) throws BeansException {
        // 将有@Value注解的field缓存起来
        ReflectionsUtil.doWithFields(bean.getClass(), field -> VALUE_BEAN_MAP.put(field, bean), field -> !Objects.isNull(AnnotationUtil.findAnnotation(field, Value.class)));
        return bean;
    }

    public void init() {
        writeData();

        // 配置服务数据
        Map<String, Object> dataMap = this.getData();
        if (CollectionUtil.isEmpty(dataMap)) {
            return;
        }

        String configName = "dynamic_configuration";

        // 校验数据
        check(dataMap);
        ApplicationContextUtil.addLastMapPropertySource(configName, dataMap);
        updateData(dataMap);

        addListener(configName);
    }

    private void writeData() {
        if (CollectionUtil.isEmpty(DynamicConfiguration.FIELDS)) {
            return;
        }

        List<String> keys = DynamicConfiguration.FIELDS.stream().map(field -> {
            ConfigCenter annotation = AnnotationUtil.findAnnotation(field, ConfigCenter.class);
            if (Objects.isNull(annotation)) {
                throw new BusinessException(MessageUtil.format("ConfigCenter不存在", "class", field.getDeclaringClass(), "field", field.getName()));
            }

            String key = annotation.key();
            if (StringUtil.isEmpty(key)) {
                throw new BusinessException(MessageUtil.format("配置中心key不能为空", "class", field.getDeclaringClass(), "field", field.getName()));
            }

            try {
                Object value = field.get(null);
                if (Objects.isNull(value)) {
                    throw new BusinessException(MessageUtil.format("配置中心value不能为null", "class", field.getDeclaringClass(), "field", field.getName()));
                }
            } catch (Exception e) {
                log.error(MessageUtil.format("writeData get field error", e, "class", field.getDeclaringClass(), "field", field.getName()));
                throw new BusinessException(MessageUtil.format("writeData get field error", "class", field.getDeclaringClass(), "field", field.getName()), e);
            }
            return key;
        }).collect(Collectors.toList());

        List<String> duplicateElements = CollectionUtil.getDuplicateElements(keys);
        if (!CollectionUtil.isEmpty(duplicateElements)) {
            throw new BusinessException(MessageUtil.format("配置中心key不能重复", "duplicateElements", JsonUtil.object2Json(duplicateElements)));
        }

        Map<String, Object> writeMap = DynamicConfiguration.FIELDS.stream().collect(Collectors.toMap(field -> {
            ConfigCenter configCenter = AnnotationUtil.findAnnotation(field, ConfigCenter.class);
            if (ObjectUtil.isNull(configCenter)) {
                throw new BusinessException(MessageUtil.format("ConfigCenter不存在", "class", field.getDeclaringClass(), "field", field.getName()));
            }
            return configCenter.key();
        }, field -> {
            try {
                return field.get(null);
            } catch (Exception e) {
                log.error(MessageUtil.format("writeData get field error", e, "class", field.getDeclaringClass(), "field", field.getName()));
                throw new BusinessException(MessageUtil.format("writeData get field error", "class", field.getDeclaringClass(), "field", field.getName()), e);
            }
        }));

        writeMap.forEach((key, value) -> {
            String result = zkClient.createNode(APPLICATION_CONFIG_PATH + IOUtil.DIR_SEPARATOR_UNIX + key, ObjectUtil.obj2Str(value), CreateMode.PERSISTENT);
            log.info("writeData key=>{}, value=>{}, result=>{}", key, ObjectUtil.obj2Str(value), result);
        });
    }

    private void addListener(String configName) {
        String path = APPLICATION_CONFIG_PATH;
        CuratorListener curatorListener = new CuratorListener(zkClient, new DataListener() {
            @Override
            public void update(String updatePath, String oldData, String newData) {
                if (!ObjectUtil.equals(updatePath, path) && updatePath.startsWith(path)) {
                    Map<String, Object> dataMap = getData();
                    if (CollectionUtil.isEmpty(dataMap)) {
                        return;
                    }

                    // 校验数据
                    check(dataMap);
                    ApplicationContextUtil.replaceMapPropertySource(configName, dataMap);
                    updateData(dataMap);
                }
            }
        });
        zkClient.addListener(path, curatorListener, executor);
    }

    public static void updateData(Map<String, Object> dataMap) {
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

        CollectionUtil.emptyIfNull(DynamicConfiguration.FIELDS).forEach(field -> {
            ConfigCenter configCenter = AnnotationUtil.findAnnotation(field, ConfigCenter.class);
            if (ObjectUtil.isNull(configCenter)) {
                return;
            }

            String valueKey = configCenter.key();
            Object value = dataMap.get(valueKey);
            if (ObjectUtil.isNull(value)) {
                return;
            }

            try {
                field.setAccessible(Boolean.TRUE);
                field.set(null, JsonUtil.json2Object((String) value, field.getType()));
            } catch (Exception e) {
                log.error(MessageUtil.format("动态更新配置error", e, "class", field.getDeclaringClass(), "field", field.getName()));
            }
        });
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

    /**
     * 获取配置服务数据
     */
    private Map<String, Object> getData() {
        String path = APPLICATION_CONFIG_PATH;
        List<String> children = zkClient.getChildren(path);
        if (CollectionUtil.isEmpty(children)) {
            return CollectionUtil.emptyMap();
        }
        return children.stream().collect(Collectors.toMap(child -> child, child -> zkClient.doGetContent(path + "/" + child)));
    }
}
