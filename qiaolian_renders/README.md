# 侨联 UserBot 上线部署包（对齐现网）

## 包含文件

```text
qiaolian_renders/
├── 频道置顶SEO推广文案.md
├── Bot固定索引消息.md
├── 快速上线部署指南.md
├── deploy_index.py
└── README.md
```

## 3 步上线

1. **频道置顶（2 分钟）**
   - 打开 `频道置顶SEO推广文案.md`
   - 复制推荐版本发到频道并置顶

2. **Bot 固定索引（3 分钟）**
   - 推荐自动方式：运行 `python3 qiaolian_renders/deploy_index.py`
   - 或手动方式：复制 `Bot固定索引消息.md` 的主索引消息到 Bot 对话并固定

3. **回归验证（3 分钟）**
   - `/start` 回首页，显示主导航按钮
   - `/find`、`/favorites`、`/appointments`、`/contact` 可正常进入
   - 从频道帖内深链进入可承接（`a__` / `q__` / `ch__` / `t_bind_`）

## 注意

- 本包已对齐当前 `qiaolian_dual/user_bot.py` 的真实命令和按钮语义。
- 不再使用示例里的 `/menu`、`/sukhumvit`、`/hot` 等无关命令。
- 若在服务器执行，请在项目根目录（例如 `/opt/qiaolian_dual_bots`）运行脚本。
