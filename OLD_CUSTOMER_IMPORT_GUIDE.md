# 老客户批量建档与绑定指南

## 目的

这套流程用于把历史成交租户接入“入住与生活服务”。每条租约会生成一个**专属 Telegram 绑定链接**。老客户点击链接后，系统才会将其 Telegram 账号与对应租约档案绑定；在此之前，档案处于 `pending` 状态，不能用于群发或租后提醒。

> 建议采用“先建档、后定向邀请、客户主动绑定”的方式。这样既避免把姓名、手机号、微信等身份信息批量写入机器人数据库，也避免误绑或误触达。

## 准备 CSV

以 [`templates/old_customer_import_template.csv`](templates/old_customer_import_template.csv) 为模板。必填列只有 `property_name`；其余字段可按掌握情况填写。

| 列名 | 是否必填 | 说明 |
|---|---:|---|
| `binding_code` | 否 | 专属编号。建议使用 `OLD-年份-流水号`，例如 `OLD-2026-0001`。留空时系统自动生成。 |
| `property_name` | 是 | 小区、楼栋、房号等可用于服务识别的信息。 |
| `lease_end_date` | 否 | 租约到期日，格式 `YYYY-MM-DD`。 |
| `rent_day` | 否 | 每月交租日，填写 `1`–`31`。 |
| `monthly_rent` | 否 | 月租金额，仅填写数字。 |
| `contract_start_date` | 否 | 合同起始日，格式 `YYYY-MM-DD`。 |
| `contract_end_date` | 否 | 合同到期日，格式 `YYYY-MM-DD`。 |
| `deposit_months` | 否 | 押金月数，仅填写数字。 |
| `contract_notes` | 否 | 服务备注，例如“需提前 30 天沟通续租”。 |

请**不要**在此文件中放入姓名、手机号、微信、身份证件、护照或聊天记录。

## 安全执行流程

先执行预览校验。以下命令不会写入数据库：

```bash
cd /opt/qiaolian_dual_bots
.venv/bin/python tools/import_legacy_tenants.py \
  --input /opt/qiaolian_dual_bots/templates/old_customer_import_template.csv
```

校验通过后，再显式执行导入，并生成专属链接清单：

```bash
cd /opt/qiaolian_dual_bots
.venv/bin/python tools/import_legacy_tenants.py \
  --input /path/to/old_customers.csv \
  --apply \
  --output /opt/qiaolian_dual_bots/reports/old_customer_binding_links.csv
```

导入完成后，由顾问通过已知的客户沟通渠道**一对一或分组定向**发送其专属链接。建议先从近期租户或富力城单个小区开始小规模试运行，确认绑定率与服务承接节奏后再扩大范围。

## 推荐邀请话术

> 你好，我是侨联。我们已上线“入住与生活服务”，以后报修、物业沟通、续租换房、周边生活信息都可以在这里查看。点击下方专属入口即可找回你的服务档案；如资料有变化，直接在机器人里联系顾问即可。

## 导入后的检查

导入工具会拒绝重复的 `binding_code`、无效日期、无效交租日或错误数值。成功后会生成只含房源标识、租约服务信息和绑定链接的结果文件。建议先随机核对 3–5 条链接，再发送给客户。
