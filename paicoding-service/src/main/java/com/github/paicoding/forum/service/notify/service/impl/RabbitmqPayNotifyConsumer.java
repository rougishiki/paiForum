package com.github.paicoding.forum.service.notify.service.impl;

import com.github.paicoding.forum.api.model.enums.NotifyStatEnum;
import com.github.paicoding.forum.api.model.enums.NotifyTypeEnum;
import com.github.paicoding.forum.api.model.enums.pay.PayStatusEnum;
import com.github.paicoding.forum.api.model.enums.pay.ThirdPayWayEnum;
import com.github.paicoding.forum.api.model.vo.notify.NotifyMessage;
import com.github.paicoding.forum.api.model.vo.user.dto.BaseUserInfoDTO;
import com.github.paicoding.forum.service.article.repository.entity.ArticleDO;
import com.github.paicoding.forum.service.article.repository.entity.ArticlePayRecordDO;
import com.github.paicoding.forum.service.article.service.ArticleReadService;
import com.github.paicoding.forum.service.notify.config.RabbitMqConfig;
import com.github.paicoding.forum.service.notify.repository.dao.NotifyMsgDao;
import com.github.paicoding.forum.service.notify.repository.entity.NotifyMsgDO;
import com.github.paicoding.forum.service.notify.service.NotifyService;
import com.github.paicoding.forum.service.user.service.UserService;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Objects;

/**
 * 支付通知消费者
 */
@Component
@Slf4j
public class RabbitmqPayNotifyConsumer {
    private static final int MAX_RETRY_COUNT = 3;

    @Autowired
    private NotifyMsgDao notifyMsgDao;

    @Autowired
    private NotifyService notifyService;

    @Autowired
    private ArticleReadService articleReadService;

    @Autowired
    private UserService userService;

    @Autowired(required = false)
    private RabbitTemplate rabbitTemplate;

    @RabbitListener(queues = RabbitMqConfig.QUEUE_NAME_PAY)
    public void handlePayMessage(NotifyMessage<ArticlePayRecordDO> notifyMessage, Message message, Channel channel) {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        ArticlePayRecordDO record = notifyMessage.getContent();
        log.info("消费支付消息: payUserId={}, articleId={}", record.getPayUserId(), record.getArticleId());

        Integer retryCount = (Integer) message.getMessageProperties().getHeaders().getOrDefault("retry_count", 0);

        try {
            savePayNotify(record);
            channel.basicAck(deliveryTag, false);
            log.info("消费支付消息成功, retryCount={}", retryCount);
        } catch (Exception e) {
            log.error("处理支付消息失败, retryCount={}", retryCount, e);
            try {
                if (retryCount < MAX_RETRY_COUNT) {
                    retryMessageWithDelay(message, retryCount + 1);
                    channel.basicAck(deliveryTag, false);
                } else {
                    channel.basicReject(deliveryTag, false);
                    log.warn("支付消息重试超过限制, 发送至死信队列");
                }
            } catch (Exception ex) {
                log.error("重试/拒绝支付消息失败", ex);
            }
        }
    }

    private void savePayNotify(ArticlePayRecordDO record) {
        ArticleDO article = articleReadService.queryBasicArticle(record.getArticleId());
        PayStatusEnum payStatus = PayStatusEnum.statusOf(record.getPayStatus());

        NotifyMsgDO msg;
        if (PayStatusEnum.PAYING == payStatus) {
            BaseUserInfoDTO payUser = userService.queryBasicUserInfo(record.getPayUserId());
            msg = new NotifyMsgDO().setRelatedId(record.getArticleId())
                    .setNotifyUserId(record.getReceiveUserId())
                    .setOperateUserId(record.getPayUserId())
                    .setType(NotifyTypeEnum.PAY.getType())
                    .setState(NotifyStatEnum.UNREAD.getStat())
                    .setMsg(String.format("您的文章 <a href=\"/article/detail/%d\">%s</a> 收到一份来自 <a href=\"/user/home?userId=%d\">%s</a> 的 [%s] 打赏，点击 <a href=\"/article/payConfirm?payId=%d\">去确认~</a>",
                            record.getArticleId(), article.getTitle(),
                            payUser.getUserId(), payUser.getUserName(),
                            StringUtils.isBlank(record.getPayWay()) || Objects.equals(record.getPayWay(), ThirdPayWayEnum.EMAIL.getPay()) ? "个人收款码" : "微信支付",
                            record.getId()));
        } else {
            msg = new NotifyMsgDO().setRelatedId(record.getArticleId())
                    .setNotifyUserId(record.getPayUserId())
                    .setOperateUserId(record.getReceiveUserId())
                    .setType(NotifyTypeEnum.PAY.getType())
                    .setState(NotifyStatEnum.UNREAD.getStat())
                    .setMsg(
                            PayStatusEnum.SUCCEED == payStatus
                                    ? String.format("您对 <a href=\"/article/detail/%d\">%s</a> 的支付已完成~", record.getArticleId(), article.getTitle())
                                    : String.format("您对 <a href=\"/article/detail/%d\">%s</a> 的支付未完成哦~", record.getArticleId(), article.getTitle())
                    );
        }

        NotifyMsgDO dbMsg = notifyMsgDao.getByUserIdRelatedIdAndType(msg);
        if (dbMsg == null) {
            notifyMsgDao.save(msg);
        } else if (!Objects.equals(dbMsg.getMsg(), msg.getMsg())) {
            notifyMsgDao.save(msg);
        } else if (payStatus == PayStatusEnum.PAYING && Objects.equals(dbMsg.getState(), NotifyStatEnum.UNREAD.getStat())) {
            notifyMsgDao.save(msg);
        }

        if (payStatus == PayStatusEnum.PAYING) {
            notifyService.notifyToUser(msg.getNotifyUserId(), NotifyTypeEnum.SYSTEM,
                    String.format("您的文章《%s》收到一份打赏，请及时确认~", article.getTitle()));
        } else if (payStatus == PayStatusEnum.SUCCEED) {
            notifyService.notifyToUser(msg.getNotifyUserId(), NotifyTypeEnum.SYSTEM,
                    String.format("您对文章《%s》的支付已完成，刷新即可阅读全文哦~", article.getTitle()));
        } else if (payStatus == PayStatusEnum.FAIL) {
            notifyService.notifyToUser(msg.getNotifyUserId(), NotifyTypeEnum.SYSTEM,
                    String.format("您对文章《%s》的支付未成功，请重试一下吧~", article.getTitle()));
        }
    }

    private void retryMessageWithDelay(Message originalMsg, int newRetryCount) {
        try {
            MessageProperties props = originalMsg.getMessageProperties();
            props.setHeader("retry_count", newRetryCount);
            Thread.sleep(newRetryCount * 1000L);
            rabbitTemplate.send(
                    RabbitMqConfig.EXCHANGE_NAME,
                    RabbitMqConfig.ROUTING_KEY_PAY,
                    originalMsg);
            log.info("支付消息重试成功, 新重试次数:{}", newRetryCount);
        } catch (Exception e) {
            log.error("支付消息重试失败", e);
        }
    }
}
