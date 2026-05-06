package com.github.paicoding.forum.service.notify.service.impl;

import com.github.paicoding.forum.core.util.SpringUtil;
import com.github.paicoding.forum.service.notify.service.RabbitmqService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class RabbitmqServiceImpl implements RabbitmqService {

    @Autowired(required = false)
    private RabbitTemplate rabbitTemplate;

    @Override
    public boolean enabled() {
        return "true".equalsIgnoreCase(SpringUtil.getConfig("rabbitmq.switchFlag"));
    }

    @Override
    public boolean publishMsg(String exchange, String routingKey, Object message) {
        if (!enabled()) {
            return false;
        }
        try {
            rabbitTemplate.convertAndSend(exchange, routingKey, message);
            log.info("消息发送成功 exchange:{} routingKey:{} message:{}", exchange, routingKey, message);
            return true;
        } catch (Exception e) {
            log.error("消息发送失败，异常信息：", e);
            return false;
        }
    }
}
