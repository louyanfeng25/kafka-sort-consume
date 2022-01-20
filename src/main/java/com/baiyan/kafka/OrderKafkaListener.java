package com.baiyan.kafka;


import com.baiyan.model.OrderDTO;
import com.baiyan.service.OrderService;
import com.baiyan.util.GsonUtil;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;

/**
 * 订单消费者
 *
 * @author baiyan
 * @date 2022/01/19
 */
@Component
@Slf4j
@ConfigurationProperties(prefix = "kafka.order")
@Data
@EqualsAndHashCode(callSuper = false)
public class OrderKafkaListener extends AbstractConsumerSeekAware {

    @Autowired
    private OrderService orderService;

    /**
     * 顺序消费并发级别
     */
    private Integer concurrent;

    /**
     * order业务顺序消费池
     */
    private KafkaConsumerPool<OrderDTO> kafkaConsumerPool;

    /**
     * 初始化顺序消费池
     */
    @PostConstruct
    public void init(){
        KafkaSortConsumerConfig<OrderDTO> config = new KafkaSortConsumerConfig<>();
        config.setBizName("order");
        config.setBizService(orderService::solveRetry);
        config.setConcurrentSize(concurrent);
        kafkaConsumerPool = new KafkaConsumerPool<>(config);
    }

    @KafkaListener(topics = {"${kafka.order.topic}"}, containerFactory = "baiyanCommonFactory")
    public void consumerMsg(List<ConsumerRecord<?, ?>> records, Acknowledgment ack){
        if(records.isEmpty()){
            return;
        }

        records.forEach(consumerRecord->{
            OrderDTO order = GsonUtil.gsonToBean(consumerRecord.value().toString(), OrderDTO.class);
            kafkaConsumerPool.submitTask(order.getId(),order);
        });

        // 当线程池中任务处理完成的计数达到拉取到的记录数时提交
        // 注意这里如果存在部分业务阻塞时间很长，会导致位移提交不上去，务必做好一些熔断措施
        while (true){
           if(records.size() == kafkaConsumerPool.getPendingOffsets().get()){
               ack.acknowledge();
               log.info("offset提交：{}",records.get(records.size()-1).offset());
               kafkaConsumerPool.getPendingOffsets().set(0L);
               break;
           }
        }
    }

}
