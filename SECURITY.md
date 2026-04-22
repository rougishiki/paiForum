# 安全配置指南

## ⚠️ 重要提示

**永远不要将敏感信息提交到 Git 仓库！** 包括但不限于：
- GitHub OAuth Client Secret
- 微信公众号 AppSecret
- 知识星球 Secret
- 数据库密码
- Redis 密码
- API 密钥

## 🔧 配置方法

### 方法 1：使用环境变量（推荐）

#### Windows (PowerShell)
```powershell
# 临时设置（当前会话有效）
$env:GITHUB_CLIENT_SECRET="your_secret_here"
$env:WX_APP_SECRET="your_wechat_secret_here"

# 永久设置（系统环境变量）
[Environment]::SetEnvironmentVariable("GITHUB_CLIENT_SECRET", "your_secret_here", "User")
```

#### Linux/Mac
```bash
# 临时设置（当前会话有效）
export GITHUB_CLIENT_SECRET="your_secret_here"
export WX_APP_SECRET="your_wechat_secret_here"

# 永久设置（添加到 ~/.bashrc 或 ~/.zshrc）
echo 'export GITHUB_CLIENT_SECRET="your_secret_here"' >> ~/.bashrc
source ~/.bashrc
```

#### IDE 配置（IntelliJ IDEA）
1. 打开 Run/Debug Configurations
2. 找到 Environment variables
3. 添加环境变量：
   ```
   GITHUB_CLIENT_SECRET=your_secret_here
   WX_APP_SECRET=your_wechat_secret_here
   ```

### 方法 2：使用 .env 文件

1. 复制 `.env.example` 为 `.env`：
   ```bash
   cp .env.example .env
   ```

2. 编辑 `.env` 文件，填入真实的密钥：
   ```env
   GITHUB_CLIENT_SECRET=your_real_secret_here
   WX_APP_SECRET=your_real_wechat_secret_here
   ```

3. **重要**：`.env` 文件已被添加到 `.gitignore`，不会被提交到 Git

### 方法 3：使用 Spring Boot 配置覆盖

创建 `application-local.yml` 或 `application-local.properties` 文件（已被 `.gitignore` 忽略）：

```yaml
paicoding:
  login:
    github:
      client:
        secret: your_real_secret_here
    wx:
      appSecret: your_real_wechat_secret_here
```

启动时指定 profile：
```bash
java -jar app.jar --spring.profiles.active=dev,local
```

## 📋 环境变量列表

### GitHub OAuth
| 变量名 | 说明 | 示例 |
|--------|------|------|
| `GITHUB_CLIENT_ID` | GitHub OAuth App ID | `Ov23liLs3ZUtwFyoRuJ3` |
| `GITHUB_CLIENT_SECRET` | GitHub OAuth App Secret | `83a14f7cea...` |
| `GITHUB_REDIRECT_URI` | 回调地址 | `http://localhost:8080/oauth/github/callback` |

### 微信公众号
| 变量名 | 说明 | 示例 |
|--------|------|------|
| `WX_APP_ID` | 微信公众号 AppID | `wx59xffb8` |
| `WX_APP_SECRET` | 微信公众号 AppSecret | `a7c4x68c84` |

### 知识星球
| 变量名 | 说明 | 示例 |
|--------|------|------|
| `ZSXQ_APP_ID` | 知识星球 AppID | `666` |
| `ZSXQ_GROUP_NUMBER` | 知识星球群号 | `666` |
| `ZSXQ_SECRET` | 知识星球 Secret | `666` |

## 🔍 验证配置

启动应用后，检查日志中是否有以下信息：
```
GitHub OAuth 使用代理: /127.0.0.1:7897
```

如果看到空值或默认值，说明环境变量未正确设置。

## 🚨 紧急处理

### 如果已经提交了敏感信息到 Git

1. **立即撤销密钥**：
   - GitHub: 访问 https://github.com/settings/developers 重新生成 Client Secret
   - 微信: 在微信公众平台重新设置 AppSecret

2. **从 Git 历史中移除敏感信息**：
   ```bash
   # 使用 BFG Repo-Cleaner（推荐）
   bfg --replace-text passwords.txt
   
   # 或使用 git filter-branch
   git filter-branch --force --index-filter \
     'git ls-files -s | sed "s|your_secret||g" | git update-index --index-info' \
     --prune-empty --tag-name-filter cat -- --all
   ```

3. **强制推送到远程仓库**：
   ```bash
   git push origin --force --all
   ```

## 📚 参考资料

- [Spring Boot Externalized Configuration](https://docs.spring.io/spring-boot/docs/current/reference/html/features.html#features.external-config)
- [GitHub OAuth Apps](https://docs.github.com/en/developers/apps/building-oauth-apps)
- [微信公众号开发文档](https://developers.weixin.qq.com/doc/offiaccount/Getting_Started/Overview.html)

## ✅ 安全检查清单

- [ ] 所有敏感信息已从配置文件中移除
- [ ] `.env` 文件已添加到 `.gitignore`
- [ ] 环境变量已正确设置
- [ ] 应用启动正常，功能测试通过
- [ ] Git 历史中无敏感信息
- [ ] 团队成员已知晓安全配置方法
