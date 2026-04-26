# AGENTS.md

本文件为 Qoder (qoder.com) 在处理此代码库中的代码时提供指导。

## 项目概述

PaiCoding 是一个基于 Spring Boot 构建的技术内容分享社区平台。这是一个多模块 Maven 项目，采用分层架构，将 API 定义、业务逻辑、核心工具、前端资源和 Web 端点分离。

## 构建与测试命令

### 构建
```bash
# 清理并安装所有模块
mvn clean install -DskipTests=true

# 为特定环境构建（dev/test/pre/prod）
mvn clean install -DskipTests=true -P<env>

# 仅构建 web 模块
cd paicoding-web && mvn clean package spring-boot:repackage -DskipTests=true -Pprod
```

### 测试
```bash
# 运行所有测试
mvn test

# 运行特定模块的测试
cd paicoding-web && mvn test

# 运行单个测试类
mvn test -Dtest=ClassName

# 运行单个测试方法
mvn test -Dtest=ClassName#methodName
```

### 运行应用程序
```bash
# 本地开发（需要 MySQL 和 Redis 正在运行）
# 入口点：paicoding-web 模块中的 QuickForumApplication
# 默认端口：8080
# 数据库配置位置：paicoding-web/src/main/resources-env/dev/application-dal.yml

# 生产部署
./launch.sh start        # 构建并部署
./launch.sh restart      # 重启现有部署
```

## 架构

### 模块依赖关系
```
paicoding-web
├── 依赖于：paicoding-ui, paicoding-service
│
paicoding-service  
├── 依赖于：paicoding-core, paicoding-api
│
paicoding-core
├── 依赖于：paicoding-api
│
paicoding-api
└── （基础模块：实体、DTO、VO、枚举）
```

### 关键模块
- **paicoding-api**：实体定义、DTO、VO、通用枚举
- **paicoding-core**：工具类、搜索、缓存、推荐、通用组件
- **paicoding-service**：业务逻辑、MyBatis-Plus 数据库操作
- **paicoding-ui**：Thymeleaf 模板、JavaScript、CSS、静态资源
- **paicoding-web**：控制器、REST 端点、`QuickForumApplication` 入口点、全局异常处理、认证

### 配置结构
- 环境配置位于 `paicoding-web/src/main/resources-env/<env>/`
  - `application-dal.yml`：数据库配置
  - `application-image.yml`：图片上传配置
  - `application-web.yml`：Web 配置
- 主配置位于 `paicoding-web/src/main/resources/`
  - `application.yml`：主入口
  - `application-config.yml`：站点配置
  - `logback-spring.xml`：日志配置

### 技术栈
- Spring Boot 2.7.1，Java 8+
- MyBatis-Plus 用于 ORM
- Thymeleaf 用于服务端渲染（SSR）
- Redis 用于缓存/会话管理
- ElasticSearch 用于搜索
- RabbitMQ 用于消息传递
- MongoDB 用于 NoSQL
- Liquibase 用于模式迁移（位于 `paicoding-web/src/main/resources/liquibase`）

## 开发指南

### 数据库变更
- 将 Liquibase changeset 添加到 `paicoding-web/src/main/resources/liquibase`
- 首次启动时自动创建数据库（默认：`paicoding`）
- 使用 MyBatis-Plus 进行数据库操作
- 实体类位于 paicoding-api 模块中

### 添加新功能
- 遵循分层架构：API → Service → Core
- 查看相邻文件中的现有代码模式
- 使用项目中已有的库（检查根目录的 `pom.xml`）
- 前端更改放在 paicoding-ui 模块中

### API 文档
- Swagger UI 可通过 `/doc.html` 访问
- 使用 Knife4j（knife4j-openapi2-spring-boot-starter）

### 测试
- 支持 JUnit 和 Spock（基于 Groovy 的 BDD 测试）
- 测试文件位于 `paicoding-web/src/test/java` 和 `paicoding-web/src/test/groovy`
