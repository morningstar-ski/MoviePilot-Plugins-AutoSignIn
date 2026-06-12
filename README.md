# MoviePilot 插件自用改进版

本仓库基于 [jxxghp/MoviePilot-Plugins](https://github.com/jxxghp/MoviePilot-Plugins) 进行自用调整和增强。

## 主要改进

### AutoSignIn 站点自动签到插件 v2.9.4

- **随机调度**：每天签到和模拟登录时间在 12:00-18:59 随机，避免固定时间特征
- **失败重试**：失败后自动在 15-20 分钟内随机重试，直至成功
- **启动补偿**：MoviePilot 重启后 15 秒内自动巡检并补偿当天遗漏任务
- **最终检查**：每天指定时间（默认 22:00，可配置）检查所有站点状态，失败时发送通知
- **验证码支持**：支持 MoviePilot 内置、YesCaptcha、2Captcha 等多种验证码识别服务
- **状态持久化**：基于 `schedule_state_v2` 的详细调度状态跟踪

## 安装方式

在 MoviePilot 插件市场中，使用以下仓库地址安装：

```
https://github.com/morningstar-ski/MoviePilot-Plugins-Custom
```

或使用本地路径：

```
local://AutoSignIn?path=本地路径&version=v2
```

## 配置保留

更新插件时会自动保留原有配置，新增字段使用默认值。

## 致谢

感谢 [jxxghp](https://github.com/jxxghp) 和所有 MoviePilot 插件贡献者。

## 许可

遵循原仓库许可协议。
