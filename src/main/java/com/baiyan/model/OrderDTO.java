package com.baiyan.model;

import lombok.Data;

/**
 * @author baiyan
 * @date 2022/01/19
 */
@Data
public class OrderDTO {

    /**
     * 订单id
     */
    private Long id;

    /**
     * 订单状态
     *
     * 0：生成订单
     * 1：支付订单
     * 2：归档订单
     *
     **/
    private Long status;

    /**
     * 订单名称
     */
    private String orderName;
}
