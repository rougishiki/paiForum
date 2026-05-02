package com.github.paicoding.forum.service.notify.service;

public interface RabbitmqService {

    boolean enabled();

    /**
     * 发布消息到指定交换机
     *
     * @param exchange   交换机名
     * @param routingKey 路由键
     * @param message    消息体，由 MessageConverter 自动序列化
     */
    void publishMsg(String exchange, String routingKey, Object message);
}
