# RabbitMQ 全通知类型改造方案

## 核心设计：三态分流 + 三队列

```
                     ┌── MQ 发送成功 ──→ RabbitMQ Consumer → DB + WebSocket（通知）
                     │                       │
MQ 开关=开 ──────────┤                       └── Spring Event（统计 only）
                     │
                     └── MQ 发送失败 ──→ 死信队列（不统计，等人工处理）


MQ 开关=关 ──────────→ Spring Event ──→ NotifyMsgListener（通知 + 统计全量处理）
                                      → UserActivityListener（统计）
                                      → UserStatisticEventListener（统计）
```

## RabbitMQ 队列设计（3 队列，按优先级）

| 队列 | 通知类型 | 优先级 | 原因 |
|------|---------|--------|------|
| `queue.notify.pay` | PAY / PAYING | 高 | 钱相关，独立隔离 |
| `queue.interact` | PRAISE / COLLECT | 中 | 用户互动操作（原 `queue.praise`，重构更名） |
| `queue.notify.social` | COMMENT / REPLY / FOLLOW | 中 | 社交互动 |

每个队列独立死信队列，独立 Consumer，互不干扰。

## 哪些迁 MQ、哪些不迁

| 通知类型 | 迁 MQ | 目标队列 |
|---------|-------|---------|
| COMMENT / REPLY | ✅ | `queue.notify.social` |
| FOLLOW | ✅ | `queue.notify.social` |
| PRAISE / COLLECT | ✅ | `queue.interact`（原 `queue.praise`） |
| PAY / PAYING | ✅ | `queue.notify.pay` |
| CANCEL_PRAISE / CANCEL_COLLECT / CANCEL_FOLLOW | ❌ | 留 Spring Event |
| REGISTER | ❌ | 留 Spring Event |
| DELETE_COMMENT / DELETE_REPLY | ❌ | 留 Spring Event |

## 详细步骤

### Step 1：新建 `NotifyMessage<T>`

位置：`paicoding-api` 模块

```java
public class NotifyMessage<T> {
    private NotifyTypeEnum notifyType;
    private T content;
}
```

### Step 2：`RabbitmqServiceImpl.publishMsg()` 返回 boolean

```java
public boolean publishMsg(String exchange, String routingKey, Object message) {
    if (!enabled()) return false;
    try {
        rabbitTemplate.convertAndSend(exchange, routingKey, message);
        return true;
    } catch (Exception e) {
        log.error("MQ 发送失败，消息将进入死信队列", e);
        return false;
    }
}
```

### Step 3：RabbitMQ 配置重构

praise 重命名为 interact，新增 pay 和 social 队列。

### Step 4：新建 Consumer

- `RabbitmqInteractConsumer`（替代 `RabbitmqPraiseConsumer`）— 监听 `queue.interact`
- `RabbitmqPayNotifyConsumer` — 监听 `queue.notify.pay`
- `RabbitmqSocialNotifyConsumer` — 监听 `queue.notify.social`

### Step 5：改造 4 个业务入口

| 文件 | 通知类型 | 队列路由 |
|------|---------|---------|
| `UserFootServiceImpl` | PRAISE / COLLECT | `queue.interact` |
| `CommentWriteServiceImpl` | COMMENT / REPLY | `queue.notify.social` |
| `UserRelationServiceImpl` | FOLLOW | `queue.notify.social` |
| `ArticlePayServiceImpl` | PAY / PAYING | `queue.notify.pay` |

### Step 6：精简 `NotifyMsgListener`

MQ 开启时只处理 CANCEL/REGISTER 轻量类型，MQ 关闭时全量处理。

## 涉及改动汇总

| 文件 | 改动 |
|------|------|
| `NotifyMessage.java` | **新建** |
| `RabbitMqConfig.java` | praise→interact 重命名 + 新增 pay/social 队列 |
| `CommonConstants.java` | 常量同步 |
| `RabbitmqServiceImpl.java` | `void` → `boolean` |
| `RabbitmqInteractConsumer.java` | **新建**（替代 `RabbitmqPraiseConsumer`） |
| `RabbitmqPayNotifyConsumer.java` | **新建** |
| `RabbitmqSocialNotifyConsumer.java` | **新建** |
| `CommentWriteServiceImpl.java` | COMMENT/REPLY 迁 MQ |
| `UserFootServiceImpl.java` | PRAISE/COLLECT 迁 MQ，删除 if-else |
| `UserRelationServiceImpl.java` | FOLLOW 迁 MQ |
| `ArticlePayServiceImpl.java` | PAY/PAYING 迁 MQ |
| `NotifyMsgListener.java` | 增加 MQ 开关判断 |
| `RabbitmqPraiseConsumer.java` | **删除** |
| `RabbitmqPraiseDlxConsumer.java` | 重命名为 `RabbitmqInteractDlxConsumer` |
