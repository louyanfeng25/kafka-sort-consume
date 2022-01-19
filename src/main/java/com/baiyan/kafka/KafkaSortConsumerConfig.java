package com.baiyan.kafka;

import lombok.Data;

import java.util.function.Consumer;

/**
 * @author baiyan
 * @date 2022-01-19
 */
@Data
public class KafkaSortConsumerConfig<E> {

    /**
     * 业务名称
     */
    String bizName;

    /**
     * 并发级别，多少的队列与线程处理任务
     */
    Integer concurrentSize;

    /**
     * 业务处理服务
     */
    Consumer<E> bizService;

}
