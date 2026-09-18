# 金边华人租房 Project Registry V3 / Living Facts V2

生成日期：2026-09-18

## 口径
- 中文展示优先；英文/高棉文用于 canonical identity、alias 与 evidence。
- canonical GEO 与中国租房市场 positioning 分离。
- “附近”只在有来源时建立 relation；没有距离证据的市场说法只记 MARKET_POSITIONING。
- Developer/brand/family 不是 PROJECT；具体社区 identity 能确认才建立 PROJECT。
- LA Villa、Queen Villa、Prince Villa、King Villa 等默认是 property_product_type，不自动建 PROJECT。
- 单套房源费用只作为 OWNER_SPECIFIC；多来源一致才可形成 COMMON_MARKET_PRACTICE/MARKET_RANGE。

## 炳发 Peng Huoth 中文租房位置体系
官方当前项目目录确认多条独立项目线：一号路、60米大道、50米路、598、1928、371、217、6A、Chamkar Dong、Veng Sreng。

### 60米炳发
优先解析 `The Star Diamond` / `The Star Diamond II`。canonical road 为 Hun Sen Blvd / 60m；“永旺3炳发”作为强市场定位/搜索证据，不能反写行政区。

### 50米炳发
优先解析 `The Star Mera Garden`。官方位置为 Ring Road 2 (50m road), Sangkat Cherng Ek, Khan Dangkor。不得与60米The Star Diamond合并。

### 一号路炳发
是 family+geo，不是单一PROJECT。候选包括 The Star Platinum、Athina、Herminus、Mercurean I/II、Capital One、Polaris I/II、Euro Ville、Roseville、Mastery、Eco Romance/Varanda/Sunrise/Melody/Delta、Rosato、Paradigm 等。必须再靠具体项目词解析。

### 铁桥头炳发
比“一号路炳发”更宽，是 Chbar Ampov/Boeung Snor 市场 family 定位；不能直接定某个子项目。

### Norea附近炳发
只作为靠诺利亚桥的一号路/Boeung Snor项目的 MARKET_POSITIONING。Rosato 等有距离/市场证据时提高权重；不得 canonical=Norea。

### 永旺3炳发
优先60米 The Star Diamond 系列；若文本同时出现其他具体项目名，以具体项目名优先。

## 其它 Borey 中文体系
- Orkide：2004路主要拆 The Royal / The Grand；Botanic City=6A；Pochentong=105K/森速。
- Chip Mong：按598、50M、60M、271、6A、TK、Sen Sok、Grand Phnom Penh、Chamkar Dong拆分；“集茂/Chip Mong”单独只有低权重。
- New World：当前可独立确认 Kour Srov 1&2、Kour Srov 3；裸“新世界”只到family。
- Piphup Thmey：当前按 Sen Sok、Chamkar Dong、NR4 位置体系拆；NR4具体子项目名仍需继续核。
- The Palms：独立项目，NR1/Veal Sbov/Nirouth、湄公河边，靠Norea Bridge。
- Chankiri：当前可靠具体项目为 Chankiri Palm Creek，NR2/Prek Kampeus，永旺3/60米大道方向。

## 关键未决冲突
1. Morgan Tower 与市场上的“Morgan Tower 2”住宅称呼存在 identity 混淆，禁止共享租住收费。
2. TK Royal One unit_count 存在不同口径，保持 PARTIAL。
3. M Residence 水费同时存在固定月费与按量计费，不能做简单 min/max。
4. Parc 21 水电存在 0.25 与 0.30 的市场差异，保留 MARKET_RANGE。
5. V2.1 中 BKK Mansion、Diamond Mansion、Wells Mansion、部分 Prince/旧中文名项目 identity/精确位置仍为 NEEDS_VERIFICATION。
6. Piphup Thmey NR4 的具体官方子项目命名证据不足，当前只保留可检索项目候选，不扩造 phase。
7. “Borey Peng Huoth / 炳发城”旧 PROJECT 行仅为历史兼容候选；V3 搜索应优先具体炳发子项目/family，不把品牌当具体项目。

## 统计
- PROJECT rows: 107
- 公寓相关: 46
- 服务式公寓: 4
- Borey/别墅/社区住宅: 53
- 新增具体PROJECT: 53（另将V2.1的3个family级伪PROJECT移出PROJECT表）
- Families: 13
- Normalized aliases: 367
- Location relations: 39
- Relation counts: {"NEARBY_DRIVE": 19, "MARKET_POSITIONING": 16, "WALKABLE": 3, "ADJACENT": 1}
- Living coverage electricity/water/management/parking/pet: 19/19/12/15/10
- Registry verification: {"PARTIAL": 71, "VERIFIED": 18, "NEEDS_VERIFICATION": 16, "UNKNOWN": 2}
- Conflicts: 12

## 研究边界
本轮没有为了完成率填造水电、管理费、距离或项目规则。空值/UNKNOWN 表示未取得足够可靠证据。所有单套广告型收费应继续保留 source_scope，不得升级成项目官方统一规则。
