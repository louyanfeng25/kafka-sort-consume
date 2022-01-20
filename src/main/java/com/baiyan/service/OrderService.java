package com.baiyan.service;

import com.baiyan.model.OrderDTO;

/**
 * 测试订单服务，模拟业务处理成功与失败场景
 *
 * @author baiyan
 * @date 2022/01/19
 */
public interface OrderService {

    /**
     * 处理订单数据
     *
     * @param order
     */
    void solveRetry(OrderDTO order);
}
