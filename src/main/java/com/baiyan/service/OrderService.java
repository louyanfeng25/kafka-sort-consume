package com.baiyan.service;

import com.baiyan.model.OrderDTO;

/**
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
