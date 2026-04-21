# GitHub OAuth 登录接入文档

> 官方文档：https://docs.github.com/en/apps/oauth-apps/building-oauth-apps/authorizing-oauth-apps

## 一、创建 OAuth App

### 1.1 进入设置页面

1. 登录 GitHub，点击右上角头像 → **Settings**
2. 左侧菜单最下方 → **Developer settings**
3. 左侧菜单 → **OAuth Apps**
4. 点击 **New OAuth App**

### 1.2 填写应用信息

| 字段                       | 说明             | 示例                                            |
| -------------------------- | ---------------- | ----------------------------------------------- |
| Application name           | 应用名称         | PaiForum                                        |
| Homepage URL               | 应用主页地址     | `http://localhost:8080`                       |
| Application description    | 应用描述（可选） | 技术论坛                                        |
| Authorization callback URL | 授权回调地址     | `http://localhost:8080/oauth/github/callback` |

### 1.3 获取凭证

创建完成后，你将获得：

* **Client ID**：公开标识符
* **Client Secret**：密钥（点击 "Generate a new client secret" 生成）

---

## 二、OAuth 授权流程

GitHub OAuth 使用标准的 **Authorization Code Grant** 流程：

```
┌──────────┐     1. 请求授权      ┌──────────┐
│   用户   │ ──────────────────► │  GitHub  │
└──────────┘                     └──────────┘
     │                                │
     │  2. 用户授权                   │
     │ ◄──────────────────────────────┘
     │                             
     ▼                             
┌──────────┐     3. 回调带code      ┌──────────┐
│   应用   │ ◄──────────────────── │  GitHub  │
└──────────┘                        └──────────┘
     │
     │  4. 用 code 换 token
     ▼
┌──────────┐     POST请求          ┌──────────┐
│   应用   │ ──────────────────► │  GitHub  │
└──────────┘                        └──────────┘
     │
     │  5. 返回 access_token
     │ ◄──────────────────────────
     ▼
┌──────────┐     6. 获取用户信息    ┌──────────┐
│   应用   │ ──────────────────► │ GitHub API│
└──────────┘                        └──────────┘
```

---

## 三、详细步骤

### Step 1: 请求用户授权

**GET** `https://github.com/login/oauth/authorize`

**参数：**

| 参数         | 必填 | 说明                       |
| ------------ | ---- | -------------------------- |
| client_id    | ✅   | OAuth App 的 Client ID     |
| redirect_uri | 推荐 | 授权后跳转的回调地址       |
| scope        | 可选 | 请求的权限范围             |
| state        | 推荐 | 随机字符串，防止 CSRF 攻击 |
| allow_signup | 可选 | 是否允许注册，默认 true    |

**示例：**

```
https://github.com/login/oauth/authorize?client_id=YOUR_CLIENT_ID&redirect_uri=http://localhost:8080/oauth/github/callback&scope=user:email&state=abc123
```

**常用 scope：**

| Scope           | 说明                   |
| --------------- | ---------------------- |
| (无)            | 只读公开信息           |
| `user`        | 读/写用户资料          |
| `user:email`  | 读取用户邮箱           |
| `repo`        | 完全访问私有和公开仓库 |
| `public_repo` | 访问公开仓库           |

---

### Step 2: 用户授权后回调

用户授权后，GitHub 会重定向到你指定的 `redirect_uri`，并带上参数：

```
http://localhost:8080/oauth/github/callback?code=abc123def456&state=abc123
```

**参数说明：**

* `code`：临时授权码，10分钟内有效
* `state`：你在 Step 1 中传递的 state 值（用于校验）

---

### Step 3: 用 Code 换取 Access Token

**POST** `https://github.com/login/oauth/access_token`

**请求头：**

```
Accept: application/json
```

**请求体：**

| 参数          | 必填 | 说明                       |
| ------------- | ---- | -------------------------- |
| client_id     | ✅   | OAuth App 的 Client ID     |
| client_secret | ✅   | OAuth App 的 Client Secret |
| code          | ✅   | Step 2 获取的授权码        |
| redirect_uri  | 推荐 | 回调地址                   |

**响应示例：**

```
{
  "access_token": "gho_16C7e42F292c6912E7710c838347Ae178B4a",
  "scope": "user:email",
  "token_type": "bearer"
}
```

---

### Step 4: 使用 Token 获取用户信息

**GET** `https://api.github.com/user`

**请求头：**

```
Authorization: Bearer gho_16C7e42F292c6912E7710c838347Ae178B4a
```

**响应示例：**

```
{
  "login": "octocat",
  "id": 1,
  "avatar_url": "https://github.com/images/error/octocat_happy.gif",
  "html_url": "https://github.com/octocat",
  "name": "monalisa octocat",
  "email": "octocat@github.com",
  "bio": "There once was...",
  "public_repos": 2,
  "followers": 20,
  "following": 0
}
```

---

## 四、安全注意事项

### 4.1 State 参数防 CSRF

```
// 生成随机 state
String state = UUID.randomUUID().toString();
session.setAttribute("oauth_state", state);

// 回调时验证
String savedState = session.getAttribute("oauth_state");
if (!savedState.equals(callbackState)) {
    throw new SecurityException("Invalid state parameter");
}
```

### 4.2 Client Secret 保护

* ❌ 不要提交到 Git 仓库
* ❌ 不要暴露在前端代码中
* ✅ 使用环境变量或配置文件管理
* ✅ 生产环境使用加密配置

### 4.3 Token 存储

* 不要将 Token 存储在 URL 或日志中
* 建议使用 HttpOnly Cookie 或 Session 存储
* Token 有有效期，需要考虑刷新机制

---

## 五、本项目配置

### 5.1 配置文件

`paicoding-web/src/main/resources-env/dev/application-login.yml`:

```
paicoding:
  login:
    github:
      client:
        id: YOUR_GITHUB_CLIENT_ID
        secret: YOUR_GITHUB_CLIENT_SECRET
        redirect-uri: http://localhost:8080/oauth/github/callback
```

### 5.2 接口说明

| 接口                        | 方法 | 说明                   |
| --------------------------- | ---- | ---------------------- |
| `/oauth/github/authorize` | GET  | 跳转到 GitHub 授权页面 |
| `/oauth/github/callback`  | GET  | GitHub 授权回调处理    |

---

## 六、测试流程

1. 启动项目：`mvn spring-boot:run`
2. 访问：`http://localhost:8080`
3. 点击登录 → 点击 GitHub 图标
4. 跳转 GitHub 授权页面
5. 授权后自动跳转回首页，完成登录

---

## 七、常见问题

### Q1: redirect_uri_mismatch 错误

**原因**：回调地址与 OAuth App 中配置的不一致

**解决**：确保 `redirect_uri` 与 GitHub OAuth App 中配置的完全一致（包括末尾斜杠）

### Q2: bad_verification_code 错误

**原因**：授权码已过期或已使用

**解决**：授权码只能使用一次，10分钟内有效

### Q3: 获取不到用户邮箱

**原因**：用户邮箱可能设置为私有

**解决**：

* 请求 `user:email` scope
* 调用 `GET https://api.github.com/user/emails` 获取邮箱列表

---

## 八、参考链接

* [Creating an OAuth App](https://docs.github.com/en/apps/oauth-apps/building-oauth-apps/creating-an-oauth-app)
* [Authorizing OAuth Apps](https://docs.github.com/en/apps/oauth-apps/building-oauth-apps/authorizing-oauth-apps)
* [Scopes for OAuth Apps](https://docs.github.com/en/apps/oauth-apps/building-oauth-apps/scopes-for-oauth-apps)
* [GitHub REST API - Users](https://docs.github.com/en/rest/reference/users)
