package com.baiyan.kafka;


import com.baiyan.model.OrderDTO;
import com.baiyan.service.OrderService;
import com.baiyan.util.GsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
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
public class OrderKafkaListener extends AbstractConsumerSeekAware {

    @Autowired
    OrderService orderService;

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
        config.setConcurrentSize(3);
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

        /**
         *
         */
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
