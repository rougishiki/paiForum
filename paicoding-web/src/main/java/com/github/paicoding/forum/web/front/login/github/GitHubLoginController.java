package com.github.paicoding.forum.web.front.login.github;

import com.github.paicoding.forum.api.model.vo.ResVo;
import com.github.paicoding.forum.core.net.ProxyCenter;
import com.github.paicoding.forum.core.util.SessionUtil;
import com.github.paicoding.forum.service.user.service.LoginService;
import com.xkcoding.http.config.HttpConfig;
import lombok.extern.slf4j.Slf4j;
import me.zhyd.oauth.config.AuthConfig;
import me.zhyd.oauth.model.AuthCallback;
import me.zhyd.oauth.model.AuthResponse;
import me.zhyd.oauth.model.AuthUser;
import me.zhyd.oauth.request.AuthGithubRequest;
import me.zhyd.oauth.request.AuthRequest;
import me.zhyd.oauth.utils.AuthStateUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.Proxy;
import java.util.List;

@Controller
@RequestMapping("/oauth/github")
@Slf4j
public class GitHubLoginController {

    @Autowired
    private LoginService loginService;

    @Value("${paicoding.login.github.client.id:}")
    private String clientId;

    @Value("${paicoding.login.github.client.secret:}")
    private String clientSecret;

    @Value("${paicoding.login.github.client.redirect-uri:}")
    private String redirectUri;

    @Value("${paicoding.login.github.proxy:true}")
    private boolean useProxy;

    @Value("${paicoding.login.github.retry.max-times:3}")
    private int maxRetryTimes;

    @Value("${paicoding.login.github.retry.interval-ms:1000}")
    private int retryIntervalMs;


    @GetMapping("/authorize")
    public void authorize(HttpServletResponse response) throws IOException {
        // 第1步：创建 AuthRequest 对象（配置好 client_id、secret 等）
        AuthRequest authRequest = getAuthRequest();

        // 第2步：生成 GitHub 授权 URL，并附带一个随机 state 参数
        String authorizeUrl = authRequest.authorize(AuthStateUtils.createState());

        // 第3步：记录日志，方便调试
        log.info("GitHub 授权地址: {}", authorizeUrl);

        // 第4步：重定向浏览器到 GitHub 授权页面
        response.sendRedirect(authorizeUrl);
    }

    /**
     * GitHub OAuth 回调处理
     *
     * @param callback GitHub 回调参数（包含 code、state）
     * @param response HTTP 响应对象
     * @return 重定向地址
     */
    @GetMapping("/callback")
    public String callback(AuthCallback callback, HttpServletResponse response)  {
        try {
            // 步骤1: 验证配置参数
            validateConfig();
            
            // 步骤2: 创建 GitHub 认证请求对象
            AuthRequest authRequest = getAuthRequest();
                
            // 步骤3: 执行 GitHub OAuth 登录（带重试机制）
            AuthResponse authResponse = executeGitHubLoginWithRetry(authRequest, callback);
    
            // 步骤4: 处理认证响应
            return processAuthResponse(authResponse, response);
        } catch (ConfigValidationException e) {
            log.error("GitHub 登录配置验证失败: {}", e.getMessage());
            return "redirect:/login?error=config_error";
        } catch (GitHubLoginException e) {
            log.error("GitHub 登录失败: {}", e.getMessage(), e);
            return "redirect:/login?error=" + e.getErrorCode();
        } catch (Exception e) {
            // 步骤10: 捕获其他异常，记录错误日志并重定向到登录页
            log.error("GitHub 登录未知异常", e);
            return "redirect:/login?error=unknown";
        }
    }


    /**
     * 验证配置参数
     */
    private void validateConfig() throws ConfigValidationException {
        if (StringUtils.isBlank(clientId)) {
            throw new ConfigValidationException("GitHub Client ID 未配置");
        }
        if (StringUtils.isBlank(clientSecret)) {
            throw new ConfigValidationException("GitHub Client Secret 未配置");
        }
        if (StringUtils.isBlank(redirectUri)) {
            throw new ConfigValidationException("GitHub Redirect URI 未配置");
        }
        log.debug("GitHub OAuth 配置验证通过");
    }

    /**
     * 执行 GitHub 登录（带重试机制）
     */
    private AuthResponse executeGitHubLoginWithRetry(AuthRequest authRequest, AuthCallback callback) 
            throws GitHubLoginException {
        int attempt = 0;
        Exception lastException = null;
        
        while (attempt <= maxRetryTimes) {
            try {
                attempt++;
                log.debug("尝试第 {} 次 GitHub 登录", attempt);
                
                AuthResponse authResponse = authRequest.login(callback);
                
                if (authResponse.getCode() == 2000) {
                    log.debug("GitHub 登录成功，第 {} 次尝试", attempt);
                    return authResponse;
                }
                
                // 如果是认证相关的错误，不重试
                if (isAuthError(authResponse.getCode())) {
                    log.warn("GitHub 认证错误，不进行重试: code={}, msg={}", 
                            authResponse.getCode(), authResponse.getMsg());
                    throw new GitHubLoginException("auth_error", 
                            "GitHub 认证失败: " + authResponse.getMsg());
                }
                
                log.warn("GitHub 登录失败，第 {} 次尝试: code={}, msg={}", 
                        attempt, authResponse.getCode(), authResponse.getMsg());
                
            } catch (Exception e) {
                lastException = e;
                log.warn("GitHub 登录异常，第 {} 次尝试: {}", attempt, e.getMessage());
            }
            
            // 如果不是最后一次尝试，等待重试
            if (attempt <= maxRetryTimes) {
                try {
                    Thread.sleep(retryIntervalMs * attempt); // 指数退避
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new GitHubLoginException("retry_interrupted", "重试被中断");
                }
            }
        }
        
        throw new GitHubLoginException("max_retries_exceeded", 
                "GitHub 登录重试次数已达上限", lastException);
    }
    
    /**
     * 判断是否为认证错误（不需要重试）
     */
    private boolean isAuthError(int code) {
        // 4000-4999 范围内的错误码通常表示认证相关错误
        return code >= 4000 && code < 5000;
    }
    
    /**
     * 处理认证响应
     */
    private String processAuthResponse(AuthResponse authResponse, HttpServletResponse response) 
            throws GitHubLoginException {
        // 提取响应数据并验证数据有效性
        Object data = authResponse.getData();
        if (data == null || !(data instanceof AuthUser)) {
            throw new GitHubLoginException("invalid_response", "GitHub 登录响应数据为空或格式错误");
        }

        // 从认证用户对象中提取用户信息
        AuthUser authUser = (AuthUser) data;
        String githubId = authUser.getUuid();
        String username = authUser.getUsername();
        String avatar = authUser.getAvatar();
        String email = authUser.getEmail();

        // 验证 GitHub 用户ID是否为空
        if (StringUtils.isBlank(githubId)) {
            throw new GitHubLoginException("invalid_user_data", "GitHub 用户ID为空");
        }

        // 记录登录成功日志
        log.info("GitHub 登录成功: githubId={}, username={}, email={}", githubId, username, email);

        // 调用第三方登录服务，处理用户关联和 Session 生成
        String session = loginService.loginByThirdParty(githubId, "github", username, avatar, email);

        // 根据 Session 生成结果进行相应处理
        if (StringUtils.isNotBlank(session)) {
            // Session 生成成功，将 Session ID 写入 Cookie
            response.addCookie(SessionUtil.newCookie(LoginService.SESSION_KEY, session));
            return "redirect:/";
        } else {
            throw new GitHubLoginException("session_error", "生成 Session 失败");
        }
    }

    /**
     * 创建 GitHub OAuth 请求对象
     * 
     * 注意：这里不只是本地封装，返回的 AuthRequest 对象在调用 login() 方法时，
     * 会实际发起 HTTP 请求调用 GitHub API：
     * 1. POST https://github.com/login/oauth/access_token （用 code 换取 access_token）
     * 2. GET  https://api.github.com/user （用 token 获取用户信息）
     * 
     * @return AuthRequest GitHub 认证请求对象
     */
    private AuthRequest getAuthRequest() {
        // 第1步：配置 HTTP 客户端参数
        HttpConfig.HttpConfigBuilder httpConfigBuilder = HttpConfig.builder()
                .timeout(15000);  // 总超时时间：15秒（包括连接+读取）
        
        // 第2步：如果启用了代理，配置代理服务器（国内访问 GitHub API 可能需要）
        if (useProxy) {
            // 从代理中心加载 GitHub 专用的代理配置
            Proxy proxy = ProxyCenter.loadProxy("github");
            if (proxy != null) {
                httpConfigBuilder.proxy(proxy);
                log.info("GitHub OAuth 使用代理: {}", proxy.address());
            }
        }
        
        // 第3步：创建 JustAuth 的 GitHub 认证请求对象
        // 这个对象封装了完整的 OAuth 2.0 流程，调用 login() 时会：
        // - 向 GitHub 发送 POST 请求换取 access_token
        // - 向 GitHub 发送 GET 请求获取用户信息
        return new AuthGithubRequest(AuthConfig.builder()
                .clientId(clientId)              // GitHub OAuth App 的 Client ID
                .clientSecret(clientSecret)      // GitHub OAuth App 的 Client Secret
                .redirectUri(redirectUri)        // 授权回调地址（必须与 GitHub 配置一致）
                .httpConfig(httpConfigBuilder.build())  // HTTP 客户端配置（超时、代理等）
                .build());
    }
    
    /**
     * 配置验证异常
     */
    private static class ConfigValidationException extends Exception {
        public ConfigValidationException(String message) {
            super(message);
        }
    }
    
    /**
     * GitHub 登录异常
     */
    private static class GitHubLoginException extends Exception {
        private final String errorCode;
        
        public GitHubLoginException(String errorCode, String message) {
            super(message);
            this.errorCode = errorCode;
        }
        
        public GitHubLoginException(String errorCode, String message, Throwable cause) {
            super(message, cause);
            this.errorCode = errorCode;
        }
        
        public String getErrorCode() {
            return errorCode;
        }
    }
}
