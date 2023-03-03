---
title: 联机托管说明
permalink: index.html
layout: home
---

# Azure 数据工程练习

以下练习支持 Microsoft Learn 上的培训模块，这些模块支持 [Microsoft Certified: Azure Data Engineer Associate](https://learn.microsoft.com/certifications/azure-data-engineer/) 认证。

要完成这些练习，需要一个有管理访问权限的 [Microsoft Azure 订阅](https://azure.microsoft.com/free)。 对于某些练习，可能还需要访问 [Microsoft Power BI 租户](https://learn.microsoft.com/power-bi/fundamentals/service-self-service-signup-for-power-bi)。

{% assign labs = site.pages | where_exp:"page", "page.url contains '/Instructions/Labs'" %}
| 练习 | 在 ILT 中，这是… |
| --- | --- |
{% 表示实验室 % 中的活动}| [{{ activity.lab.title }}{% if activity.lab.type %} - {{ activity.lab.type }}{% endif %}]({{ site.github.url }}{{ activity.url }}) | {{ activity.lab.ilt-use }} |
{% endfor %}
