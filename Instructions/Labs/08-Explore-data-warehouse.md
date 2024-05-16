---
lab:
  title: 探索关系数据仓库
  ilt-use: Suggested demo
---

# 探索关系数据仓库

Azure Synapse Analytics 基于可缩放集功能构建，以支持企业数据仓库；包括数据湖中的基于文件的数据分析以及用于加载数据的大型关系数据仓库和数据传输与转换管道。 在本实验室中，你将了解如何使用 Azure Synapse Analytics 中的专用 SQL 池在关系数据仓库中存储和查询数据。

完成本实验室大约需要 45 分钟。

## 准备工作

需要一个你在其中具有管理级权限的 [Azure 订阅](https://azure.microsoft.com/free)。

## 预配 Azure Synapse Analytics 工作区

Azure Synapse Analytics 工作区提供用于管理数据和数据处理运行时的中心点。 可以使用 Azure 门户中的交互式界面预配工作区，也可以使用脚本或模板在其中部署工作区和资源。 在大多数生产方案中，最好使用脚本和模板自动预配，以便可以将资源部署合并到可重复的开发和操作 (DevOps) 过程中。

在本练习中，你将组合使用 PowerShell 脚本和 ARM 模板来预配 Azure Synapse Analytics。

1. 登录到 Azure 门户，地址为 [](https://portal.azure.com)。
2. 使用页面顶部搜索栏右侧的 [\>_] 按钮在 Azure 门户中创建新的 Cloud Shell，在出现提示时选择“PowerShell”环境并创建存储。 Cloud Shell 在 Azure 门户底部的窗格中提供命令行界面，如下所示：

    ![具有 Cloud Shell 窗格的 Azure 门户](./images/cloud-shell.png)

    > 注意：如果以前创建了使用 Bash 环境的 Cloud shell，请使用 Cloud Shell 窗格左上角的下拉菜单将其更改为“PowerShell”。

3. 请注意，可以通过拖动窗格顶部的分隔条或使用窗格右上角的 &#8212;、&#9723; 或 X 图标来调整 Cloud Shell 的大小，以最小化、最大化和关闭窗格  。 有关如何使用 Azure Cloud Shell 的详细信息，请参阅 [Azure Cloud Shell 文档](https://docs.microsoft.com/azure/cloud-shell/overview)。

4. 在 PowerShell 窗格中，输入以下命令以克隆此存储库：

    ```
    rm -r dp500 -f
    git clone https://github.com/MicrosoftLearning/DP-500-Azure-Data-Analyst dp500
    ```

5. 克隆存储库后，输入以下命令以更改为此实验室的文件夹，然后运行其中包含的 setup.ps1 脚本：

    ```
    cd dp500/Allfiles/03
    ./setup.ps1
    ```

6. 如果出现提示，请选择要使用的订阅（仅当有权访问多个 Azure 订阅时才会发生这种情况）。
7. 出现提示时，输入要为 Azure Synapse SQL 池设置的合适密码。

    > 注意：请务必记住此密码！

8. 等待脚本完成 - 这通常需要大约 15 分钟，但在某些情况下可能需要更长的时间。 等待时，请查看 Azure Synapse Analytics 文档中的[什么是 Azure Synapse Analytics 中的专用 SQL 池](https://docs.microsoft.com/azure/synapse-analytics/sql-data-warehouse/sql-data-warehouse-overview-what-is)一文。

## 浏览数据仓库架构

在此实验室中，数据仓库托管在 Azure Synapse Analytics 的专用 SQL 池中。

### 启动专用 SQL 池

1. 脚本完成后，在 Azure 门户中转到创建的 dp500-*xxxxxxx* 资源组，然后选择 Synapse 工作区。
2. 在 Synapse 工作区“概述”页的“打开 Synapse Studio”卡中，选择“打开”，以在新浏览器标签页中打开 Synapse Studio；如果出现提示，请进行登录  。
3. 在 Synapse Studio 左侧，使用 &rsaquo;&rsaquo; 图标展开菜单，这将显示 Synapse Studio 中用于管理资源和执行数据分析任务的不同页面。
4. 在“管理”页上，确保选择了“SQL 池”选项卡，然后选择 sql*xxxxxxx* 专用 SQL 池，并使用其 &#9655; 图标启动它；确认在出现系统提示时进行恢复   。
5. 等待 SQL 池恢复。 这可能需要几分钟的时间。 使用“&#8635; 刷新”按钮定期检查其状态。 状态将在准备就绪时显示为“联机”。

### 查看数据库中的表

1. 在 Synapse Studio 中，选择“数据”页并确保“工作区”选项卡已选定并包含“SQL 数据库”  类别。
2. 展开“SQL 数据库”、sql*xxxxxxx* 池及其“表”文件夹以查看数据库中的表。

    关系数据仓库通常基于由“事实”表和“维度”表组成的架构。 这些表针对分析查询进行了优化，其中事实数据表中的数值指标由维度表表示的实体的属性进行聚合，例如，使你能够按产品、客户、日期等聚合 Internet 销售收入。
    
3. 展开“dbo.FactInternetSales”表及其“列”文件夹，以查看此表中的列。 请注意，许多列是引用维度表中行的键。 其他是用于分析的数值（度量值）。
    
    键用于将事实数据表与一个或多个维度表相关联，通常采用星形架构中；其中事实数据表与每个维度表直接相关（形成以事实数据表为中心的多点“星形”）。

4. 查看 dbo.DimPromotion 表的列，并注意它有一个唯一的 PromotionKey，用于唯一标识表中的每一行。 它还有 AlternateKey 属性。

    通常，数据仓库中的数据已从一个或多个事务源导入。 备用键反映源中此实体实例的业务标识符，但通常会生成唯一的数值代理键来唯一标识数据仓库维度表中的每一行。 此方法的优势之一是，它使数据仓库能够在不同时间点包含同一实体的多个实例（例如，反映下订单时同一客户地址的记录）。

5. 查看 dbo.DimProduct 列，并注意它包含 ProductSubcategoryKey 列，用于引用 dbo.DimProductSubcategory 表，该表又包含一个引用 dbo.DimProductCategory 表的 ProductCategoryKey 列。

    在某些情况下，维度部分规范化为多个相关表，以允许不同级别的粒度（例如可分组为子类别和类别的产品）。 这会导致简单的星形扩展到雪花型架构，在该架构中，中心事实数据表与维度表相关，该表又与更多维度表相关。

6. 查看 dbo.DimDate 表的列，并注意该表包含反映日期的不同时态属性的多个列，包括星期几、某月的某日、月、年、日名称、月份名称等。

    数据仓库中的时间维度通常作为维度表实现，其中包含每个最小时态粒度单位（通常称为维度的粒度）的行，你想要通过该行聚合事实数据表中的度量值。 在这种情况下，可聚合度量值的最低粒度是单个日期，表包含从数据中引用的第一个日期到最后一个日期的每个日期的行。 借助 DimDate 表中的属性，分析师能够使用一组一致的时态属性基于事实数据表中的任何日期键聚合度量值（例如，根据订单日期按月查看订单）。 FactInternetSales 表包含三个与 DimDate 表相关的键：OrderDateKey、DueDateKey 和 ShipDateKey   。

## 查询数据仓库表

既然你已经了解了数据仓库架构的一些更重要的方面，现在可以查询表并检索某些数据。

### 查询事实数据表和维度表

关系数据仓库中的数值存储在事实表中，其中相关维度表可用于跨多个属性聚合数据。 此设计意味着关系数据仓库中的大多数查询都涉及（使用聚合函数和 GROUP BY 子句）在相关表（使用 JOIN 子句）中对数据进行聚合和分组。

1. 在“数据”页上，选择 sql*xxxxxxx* SQL 池，并在其“...”菜单中，选择“新建 SQL 脚本” > “空脚本”    。
2. 当新的“SQL 脚本 1”选项卡打开时，在其“属性”窗格中，将脚本名称更改为“分析 Internet 销售”，然后更改“每个查询的结果设置”以返回所有行   。 然后在工具栏上，使用“发布”按钮以保存脚本并使用工具栏右侧的“属性”按钮（类似于 &#128463;.）关闭“属性”窗格，这样就可以看到脚本窗格   。
3. 在空脚本中，添加以下代码：

    ```sql
    SELECT  d.CalendarYear AS Year,
            SUM(i.SalesAmount) AS InternetSalesAmount
    FROM FactInternetSales AS i
    JOIN DimDate AS d ON i.OrderDateKey = d.DateKey
    GROUP BY d.CalendarYear
    ORDER BY Year;
    ```

4. 使用“&#9655;运行”按钮运行脚本，并查看结果，结果应显示每年的 Internet 销售总计。 此查询根据订单日期将 Internet 销售的事实数据表联接到时间维度表，并使用维度表的日历月属性聚合事实数据表中的销售额度量值。

5. 按如下所示修改查询，从时间维度添加月份属性，然后运行修改后的查询。

    ```sql
    SELECT  d.CalendarYear AS Year,
            d.MonthNumberOfYear AS Month,
            SUM(i.SalesAmount) AS InternetSalesAmount
    FROM FactInternetSales AS i
    JOIN DimDate AS d ON i.OrderDateKey = d.DateKey
    GROUP BY d.CalendarYear, d.MonthNumberOfYear
    ORDER BY Year, Month;
    ```

    请注意，时间维度中的属性使你可以在多个分层级别（在本例中为年和月）聚合事实数据表中的度量值。 这是数据仓库中的常见模式。

6. 按如下所示修改查询以删除月份，并将第二个维度添加到聚合中，然后运行该查询以查看结果（其中显示了每个区域的每年 Internet 销售总计）：

    ```sql
    SELECT  d.CalendarYear AS Year,
            g.EnglishCountryRegionName AS Region,
            SUM(i.SalesAmount) AS InternetSalesAmount
    FROM FactInternetSales AS i
    JOIN DimDate AS d ON i.OrderDateKey = d.DateKey
    JOIN DimCustomer AS c ON i.CustomerKey = c.CustomerKey
    JOIN DimGeography AS g ON c.GeographyKey = g.GeographyKey
    GROUP BY d.CalendarYear, g.EnglishCountryRegionName
    ORDER BY Year, Region;
    ```

    请注意，地理位置是一个雪花维度，它通过客户维度与 Internet 销售事实数据表相关。 因此，查询中需要有两个联接才能按地理位置聚合 Internet 销售。

7. 修改并重新运行查询以添加另一个雪花维度，并按产品类别聚合年度区域销售：

    ```sql
    SELECT  d.CalendarYear AS Year,
            pc.EnglishProductCategoryName AS ProductCategory,
            g.EnglishCountryRegionName AS Region,
            SUM(i.SalesAmount) AS InternetSalesAmount
    FROM FactInternetSales AS i
    JOIN DimDate AS d ON i.OrderDateKey = d.DateKey
    JOIN DimCustomer AS c ON i.CustomerKey = c.CustomerKey
    JOIN DimGeography AS g ON c.GeographyKey = g.GeographyKey
    JOIN DimProduct AS p ON i.ProductKey = p.ProductKey
    JOIN DimProductSubcategory AS ps ON p.ProductSubcategoryKey = ps.ProductSubcategoryKey
    JOIN DimProductCategory AS pc ON ps.ProductCategoryKey = pc.ProductCategoryKey
    GROUP BY d.CalendarYear, pc.EnglishProductCategoryName, g.EnglishCountryRegionName
    ORDER BY Year, ProductCategory, Region;
    ```

    此时，产品类别的雪花维度需要有三个联接才能反映产品、子类别和类别之间的分层关系。

8. 发布脚本以进行保存。

### 使用排名函数

分析大量数据时的另一个常见要求是按分区对数据进行分组，并根据特定指标确定分区中每个实体的排名。

1. 在现有查询下，添加以下 SQL 以根据国家/地区名称检索 2022 年分区销售值：

    ```sql
    SELECT  g.EnglishCountryRegionName AS Region,
            ROW_NUMBER() OVER(PARTITION BY g.EnglishCountryRegionName
                              ORDER BY i.SalesAmount ASC) AS RowNumber,
            i.SalesOrderNumber AS OrderNo,
            i.SalesOrderLineNumber AS LineItem,
            i.SalesAmount AS SalesAmount,
            SUM(i.SalesAmount) OVER(PARTITION BY g.EnglishCountryRegionName) AS RegionTotal,
            AVG(i.SalesAmount) OVER(PARTITION BY g.EnglishCountryRegionName) AS RegionAverage
    FROM FactInternetSales AS i
    JOIN DimDate AS d ON i.OrderDateKey = d.DateKey
    JOIN DimCustomer AS c ON i.CustomerKey = c.CustomerKey
    JOIN DimGeography AS g ON c.GeographyKey = g.GeographyKey
    WHERE d.CalendarYear = 2022
    ORDER BY Region;
    ```

2. 仅选择新的查询代码，并使用“&#9655; 运行”按钮运行它。 然后查看结果，结果应类似于下表：

    | 区域 | RowNumber | OrderNo | LineItem | SalesAmount | RegionTotal | RegionAverage |
    |--|--|--|--|--|--|--|
    |澳大利亚|1|SO73943|2|2.2900|2172278.7900|375.8918|
    |澳大利亚|2|SO74100|4|2.2900|2172278.7900|375.8918|
    |...|...|...|...|...|...|...|
    |澳大利亚|5779|SO64284|1|2443.3500|2172278.7900|375.8918|
    |加拿大|1|SO66332|2|2.2900|563177.1000|157.8411|
    |加拿大|2|SO68234|2|2.2900|563177.1000|157.8411|
    |...|...|...|...|...|...|...|
    |加拿大|3568|SO70911|1|2443.3500|563177.1000|157.8411|
    |法国|1|SO68226|3|2.2900|816259.4300|315.4016|
    |法国|2|SO63460|2|2.2900|816259.4300|315.4016|
    |...|...|...|...|...|...|...|
    |法国|2588|SO69100|1|2443.3500|816259.4300|315.4016|
    |德国|1|SO70829|3|2.2900|922368.2100|352.4525|
    |德国|2|SO71651|2|2.2900|922368.2100|352.4525|
    |...|...|...|...|...|...|...|
    |德国|2617|SO67908|1|2443.3500|922368.2100|352.4525|
    |英国|1|SO66124|3|2.2900|1051560.1000|341.7484|
    |英国|2|SO67823|3|2.2900|1051560.1000|341.7484|
    |...|...|...|...|...|...|...|
    |英国|3077|SO71568|1|2443.3500|1051560.1000|341.7484|
    |美国|1|SO74796|2|2.2900|2905011.1600|289.0270|
    |美国|2|SO65114|2|2.2900|2905011.1600|289.0270|
    |...|...|...|...|...|...|...|
    |美国|10051|SO66863|1|2443.3500|2905011.1600|289.0270|

    观察以下有关这些结果的事实：

    - 每个销售订单行项均对应一行。
    - 这些行根据销售的地理位置在分区中进行组织。
    - 每个地理分区中的行按销售额（从低到高）的顺序编号。
    - 每一行都包括行项销售额以及区域总销售额和平均销售额。

3. 在现有查询下，添加以下代码以在 GROUP BY 查询中应用窗口化函数，并根据总销售额对每个区域中的城市进行排名：

    ```sql
    SELECT  g.EnglishCountryRegionName AS Region,
            g.City,
            SUM(i.SalesAmount) AS CityTotal,
            SUM(SUM(i.SalesAmount)) OVER(PARTITION BY g.EnglishCountryRegionName) AS RegionTotal,
            RANK() OVER(PARTITION BY g.EnglishCountryRegionName
                        ORDER BY SUM(i.SalesAmount) DESC) AS RegionalRank
    FROM FactInternetSales AS i
    JOIN DimDate AS d ON i.OrderDateKey = d.DateKey
    JOIN DimCustomer AS c ON i.CustomerKey = c.CustomerKey
    JOIN DimGeography AS g ON c.GeographyKey = g.GeographyKey
    GROUP BY g.EnglishCountryRegionName, g.City
    ORDER BY Region;
    ```

4. 仅选择新的查询代码，并使用“&#9655; 运行”按钮运行它。 然后查看结果，并观察以下内容：
    - 结果包括每个城市（按区域分组）的行。
    - 计算每个城市的总销售额（各销售金额相加之和）
    - 根据区域分区计算区域总销售额（某区域内各城市的销售总额之和）。
    - 按降序排列每个城市的总销售额，从而计算每个城市在其区域分区内的排名。

5. 发布更新后的脚本以保存更改。

> 提示：ROW_NUMBER 和 RANK 是 Transact-SQL 中的可用排名函数的示例。 有关详细信息，请参阅 Transact-SQL 语言文档中的[排名函数](https://docs.microsoft.com/sql/t-sql/functions/ranking-functions-transact-sql)参考。

### 检索近似计数

在浏览大量数据时，查询可能需要大量的时间和资源才能运行。 通常，数据分析不需要绝对精确的值 - 近似值的比较可能就已足够。

1. 在现有查询下，添加以下代码以检索每个日历年的销售订单数：

    ```sql
    SELECT d.CalendarYear AS CalendarYear,
        COUNT(DISTINCT i.SalesOrderNumber) AS Orders
    FROM FactInternetSales AS i
    JOIN DimDate AS d ON i.OrderDateKey = d.DateKey
    GROUP BY d.CalendarYear
    ORDER BY CalendarYear;
    ```

2. 仅选择新的查询代码，并使用“&#9655; 运行”按钮运行它。 然后查看返回的输出：
    - 在查询下，在“结果”选项卡上，查看每年的订单计数。
    - 在“消息”选项卡上，查看查询的总执行时间。
3. 按如下所示修改查询，以返回每年的近似计数。 然后，重新运行查询。

    ```sql
    SELECT d.CalendarYear AS CalendarYear,
        APPROX_COUNT_DISTINCT(i.SalesOrderNumber) AS Orders
    FROM FactInternetSales AS i
    JOIN DimDate AS d ON i.OrderDateKey = d.DateKey
    GROUP BY d.CalendarYear
    ORDER BY CalendarYear;
    ```

4. 查看返回的输出：
    - 在查询下，在“结果”选项卡上，查看每年的订单计数。 这些值应在上一个查询检索的实际计数的 2% 以内。
    - 在“消息”选项卡上，查看查询的总执行时间。 此时间应短于上一个查询。

5. 发布脚本以保存更改。

> **提示**：有关更多详细信息，请参阅 [APPROX_COUNT_DISTINCT](https://docs.microsoft.com/sql/t-sql/functions/approx-count-distinct-transact-sql) 函数文档。

## 挑战 - 分析经销商销售

1. 为 sql*xxxxxxx* SQL 池创建新的空脚本，并使用“分析经销商销售”名称进行保存 。
2. 在脚本中创建 SQL 查询，以基于 FactResellerSales 事实数据表及其相关的维度表查找以下信息：
    - 每个财政年度和季度销售的商品总数。
    - 与销售人员关联的每个财政年度、季度和销售区域销售的商品总数。
    - 每个财政年度、季度和销售区域按产品类别销售的商品总数。
    - 基于年度总销售额的每个销售区域在每个财政年度的排名。
    - 每个销售区域每年的大致销售订单数。

    > 提示：将你的查询与 Synapse Studio 中“开发”页的解决方案脚本中的查询进行比较 。

3. 闲暇时尝试使用查询来探索数据仓库架构中的其余表。
4. 完成后，在“管理”页上暂停 sql*xxxxxxx* 专用 SQL 池 。

## 删除 Azure 资源

你已完成对 Azure Synapse Analytics 的探索，现在应删除已创建的资源，以避免产生不必要的 Azure 成本。

1. 关闭 Synapse Studio 浏览器选项卡并返回到 Azure 门户。
2. 在 Azure 门户的“主页”上，选择“资源组”。
3. 选择 Synapse Analytics 工作区的 dp500-*xxxxxxx* 资源组（不是受管理资源组），并确认它包含 Synapse 工作区、存储帐户和工作区的专用 SQL 池。
4. 在资源组的“概述”页的顶部，选择“删除资源组”。
5. 输入 dp500-*xxxxxxx* 资源组名称以确认要删除该资源组，然后选择“删除” 。

    几分钟后，将删除 Azure Synapse 工作区资源组及其关联的托管工作区资源组。
