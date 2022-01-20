package com.baiyan.service.impl;

import com.baiyan.model.OrderDTO;
import com.baiyan.service.OrderService;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Recover;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

/**
 * @author baiyan
 * @date 2022/01/19
 */
@Service
@Slf4j
public class OrderServiceImpl implements OrderService {

    /**
     * 处理订单数据
     *
     * @param order
     */
    @Override
    @SneakyThrows
    @Retryable(value = Exception.class,backoff = @Backoff(delay = 1000L, multiplier = 1.5))
    public void solveRetry(OrderDTO order){

        //注意这里添加幂等处理，防止消息重复消费

        //模拟逻辑处理需要时长
        Thread.sleep(50);
        //模拟调用异常
        if(order.getId()==1){
            throw new RuntimeException("模拟订单id为1的数据报错");
        }
    }


    /**
     * 容错处理类
     *
     * @param e 异常
     * @param order 订单入参
     * @throws ArithmeticException
     */
    @Recover
    public void solveRecover(Throwable e, OrderDTO order) throws ArithmeticException {

        //模拟重试失败保护机制

        log.info("全部重试失败，执行doRecover");

        // 发送至死信队列
    }
}
