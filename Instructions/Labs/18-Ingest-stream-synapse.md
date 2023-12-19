---
lab:
  title: 使用 Azure 流分析和 Azure Synapse Analytics 引入实时数据
  ilt-use: Lab
---

# 使用 Azure 流分析和 Azure Synapse Analytics 引入实时数据

数据分析解决方案通常需要引入和处理数据流。 流处理与批处理的不同之处在于，流通常是无限制的 - 换句话说，它们是连续的数据源，必须持续进行处理，而不是以固定的时间间隔进行处理。

Azure 流分析提供了一种云服务，可用于定义对流式处理源（如 Azure 事件中心或 Azure IoT 中心）数据流进行操作的查询。 你可以使用 Azure 流分析查询，将数据流直接引入数据存储进行进一步分析，或者基于临时窗口筛选、聚合和汇总数据。

在本练习中，你将使用 Azure 流分析处理销售订单数据流，例如从联机零售应用程序生成的数据流。 订单数据将发送到 Azure 事件中心，Azure 流分析作业将从该位置读取数据并将其引入 Azure Synapse Analytics。

完成此练习大约需要 45 分钟。

## 准备工作

需要一个你在其中具有管理级权限的 [Azure 订阅](https://azure.microsoft.com/free)。

## 预配 Azure 资源

在本练习中，你需要一个可以访问 Data Lake Storage 的 Azure Synapse Analytics 工作区，以及一个专用 SQL 池。 还需要一个 Azure 事件中心命名空间，可将流式处理订单数据发送到该命名空间。

你将组合使用 PowerShell 脚本和 ARM 模板来预配这些资源。

1. 登录到 Azure 门户，地址为 [](https://portal.azure.com)。
2. 使用页面顶部搜索栏右侧的 [\>_] 按钮在 Azure 门户中创建新的 Cloud Shell，在出现提示时选择“PowerShell”环境并创建存储。 Cloud Shell 在 Azure 门户底部的窗格中提供命令行界面，如下所示：

    ![具有 Cloud Shell 窗格的 Azure 门户](./images/cloud-shell.png)

    > 注意：如果以前创建了使用 Bash 环境的 Cloud shell，请使用 Cloud Shell 窗格左上角的下拉菜单将其更改为“PowerShell”。

3. 请注意，可以通过拖动窗格顶部的分隔条或使用窗格右上角的 &#8212;、&#9723; 或 X 图标来调整 Cloud Shell 的大小，以最小化、最大化和关闭窗格  。 有关如何使用 Azure Cloud Shell 的详细信息，请参阅 [Azure Cloud Shell 文档](https://docs.microsoft.com/azure/cloud-shell/overview)。

4. 在 PowerShell 窗格中，输入以下命令以克隆包含此练习的存储库：

    ```
    rm -r dp-203 -f
    git clone https://github.com/MicrosoftLearning/dp-203-azure-data-engineer dp-203
    ```

5. 克隆存储库后，输入以下命令以更改为此练习的文件夹，然后运行其中包含的 setup.ps1 脚本：

    ```
    cd dp-203/Allfiles/labs/18
    ./setup.ps1
    ```

6. 如果出现提示，请选择要使用的订阅（仅当有权访问多个 Azure 订阅时才会发生这种情况）。
7. 出现提示时，输入要为 Azure Synapse SQL 池设置的合适密码。

    > 注意：请务必记住此密码！

8. 等待脚本完成 - 这通常需要大约 15 分钟，但在某些情况下可能需要更长的时间。 在等待期间，请查看 Azure 流分析文档中的[欢迎使用 Azure 流分析](https://learn.microsoft.com/azure/stream-analytics/stream-analytics-introduction)一文。

## 将流式处理数据引入专用 SQL 池

首先，将数据流直接引入 Azure Synapse Analytics 专用 SQL 池中的一个表中。

### 查看流式处理源和数据库表

1. 安装脚本运行完毕后，请最小化 Cloud Shell 窗格（稍后将返回到此窗格）。 然后在 Azure 门户中，转到它创建的“dp203-xxxxxxx”资源组，注意此资源组包含 Azure Synapse工作区、数据湖的存储帐户、专用 SQL 池和事件中心命名空间。
2. 选择 Synapse 工作区，并在它的“概述”页的“打开 Synapse Studio”卡中选择“打开”，在新的浏览器标签页中打开 Synapse Studio。Synapse Studio 是一个基于 Web 的界面，可用于处理 Synapse Analytics 工作区  。
3. 在 Synapse Studio 左侧，使用 &rsaquo;&rsaquo; 图标展开菜单，这将显示 Synapse Studio 中用于管理资源和执行数据分析任务的不同页面。
4. 在“管理”页的“SQL 池”部分，选择“sqlxxxxxxx”专用 SQL 池行，然后使用其“&#9655;”图标恢复该行   。
5. 等待 SQL 池启动时，切换回包含 Azure 门户的浏览器标签页，然后重新打开 Cloud Shell 窗格。
6. 在 Cloud Shell 窗格中，输入以下命令以运行一个客户端应用，向 Azure 事件中心发送 100 个模拟订单：

    ```
    node ~/dp-203/Allfiles/labs/18/orderclient
    ```

7. 在发送时观察订单数据，每个订单都包含一个产品 ID 和一个数量。
8. 订单客户端应用完成后，最小化 Cloud Shell 窗格并切换回 Synapse Studio 浏览器标签页。
9. 在“Synapse Studio”的“管理”页上，确保专用 SQL 池的状态为“联机”，然后切换到“数据”页，在“工作区”窗格中，依次展开“SQL 数据库”、“sqlxxxxxxx”SQL 池和“表”以查看 dbo.FactOrder 表。
10. 在 dbo.FactOrder 表的“...”菜单中，选择“新建 SQL 脚本” > “选择前 100 行”   并查看结果。 注意到该表包含 OrderDateTime、ProductID 和 Quantity 列，但当前没有数据行。

### 创建 Azure 流分析作业以引入订单数据

1. 切换回包含 Azure 门户的浏览器标签页，并记下已预配 dp203-*xxxxxxx* 资源组的区域，你将在同一区域中<u></u>创建流分析作业。
2. 在“主页”上，选择“+创建资源”，然后搜索 `Stream Analytics job`。 然后，使用以下属性创建“流分析作业”：
    - 基本信息：
        - **订阅**：Azure 订阅
        - **资源组**：选择现有的 dp203-xxxxxxx 资源组******。
        - **名称**：`ingest-orders`
        - 区域：选择预配了 Synapse Analytics 工作区的<u>同一区域</u>。
        - 托管环境：云
        - 流单元：1
    - **存储**：
        - **添加存储帐户**：已选择
        - 订阅：Azure 订阅
        - **存储帐户**：选择 datalake*xxxxxxx* 存储帐户
        - 身份验证模式：连接字符串
        - 保护存储帐户中的专用数据：已选择
    - **标记**：
        - *无*
3. 等待部署完成，然后转到部署的流分析作业资源。

### 为事件数据流创建输入

1. 在“ingest-orders”概述页上，选择“输入”页面 。 使用“添加流输入”菜单添加具有以下属性的“事件中心”输入 ：
    - **输入别名**：`orders`
    - **从订阅选择事件中心**：选中
    - 订阅：Azure 订阅
    - **事件中心命名空间**：选择 events*xxxxxxx* 事件中心命名空间
    - **事件中心名称**：选择现有的 eventhub*xxxxxxx* 事件中心。
    - **事件中心使用者组**：选择“使用现有项”，然后选择 $Default 使用者组
    - **身份验证模式**：创建系统分配的托管标识
    - **分区键**：留空
    - **事件序列化格式**：JSON
    - **编码**：UTF-8
2. 保存输入并在创建时等待。 你将看到多个通知。 等待出现“连接测试成功”通知。

### 为 SQL 表创建输出

1. 查看“ingest-orders”流分析作业的“输出”页 。 然后使用“添加输出”菜单添加具有以下属性的 Azure Synapse Analytics 输出 ：
    - 输出别名：`FactOrder`
    - 从订阅选择 Azure Synapse Analytics：选中
    - 订阅：Azure 订阅
    - 数据库：选择 sql**xxxxxxx (synapse** xxxxxxx) 数据库
    - 身份验证模式：SQL Server 身份验证
    - 用户名：SQLUser
    - 密码：运行安装脚本时为 SQL 池指定的密码
    - 表：`FactOrder`
2. 保存输出并等待创建。 你将看到多个通知。 等待出现“连接测试成功”通知。

### 创建用于引入事件流的查询

1. 查看“ingest-orders”流分析作业的“查询”页 。 然后稍等片刻，直到（根据之前在事件中心捕获的销售订单事件）显示输入预览。
2. 注意到输入数据包括客户端应用提交的消息中的 ProductID 和 Quantity 字段，还有一些其他事件中心字段，其中包括 EventProcessedUtcTime 字段，它指示事件何时添加到事件中心。
3. 按照以下方式修改默认查询：

    ```
    SELECT
        EventProcessedUtcTime AS OrderDateTime,
        ProductID,
        Quantity
    INTO
        [FactOrder]
    FROM
        [orders]
    ```

    注意到此查询从输入（事件中心）获取字段，并将它们直接写入输出（SQL 表）中。

4. 保存查询。

### 运行流式处理作业以引入订单数据

1. 查看“ingest-orders”流分析作业的“概述”页，并在“属性”选项卡上查看该作业的“输入”、“查询”、“输出”和“函数”      。 如果“输入”和“输出”数为 0，请使用“概述”页上的“&#8635; 刷新”按钮显示“订单”输入和“FactTable”输出     。
2. 选择“&#9655; 开始”按钮，然后立即启动流式处理作业。 等待收到流式处理作业已成功启动的通知。
3. 重新打开 Cloud Shell 窗格，并重新运行以下命令以提交另外 100 个订单。

    ```
    node ~/dp-203/Allfiles/labs/18/orderclient
    ```

4. 在订单客户端应用运行时，切换到 Synapse Studio 浏览器标签页，查看之前运行的查询，以从 dbo.FactOrder 表中选择前 100 行。
5. 使用“&#9655; 运行”按钮重新运行查询，并验证表现在是否包含来自事件流的订单数据（如果不包含，请等待一分钟，然后再次重新运行查询）。 只要作业正在运行并将订单事件发送到事件中心，流分析作业就会将所有新事件数据推送到表中。
6. 在“管理”页上，暂停 sqlxxxxxxx 专用 SQL 池（防止产生不必要的 Azure 费用）。
7. 返回到包含 Azure 门户的浏览器标签页，并最小化 Cloud Shell 窗格。 然后使用“&#128454; 停止”按钮停止流分析作业，并等待流分析作业已成功停止的通知。

## 汇总数据湖中的流式处理数据

到目前为止，你已了解如何使用流分析作业将消息从流式处理源引入 SQL 表。 现在，我们来探讨一下如何使用 Azure 流分析聚合临时窗口的数据，在本例中，计算每 5 秒销售的每个产品的总数量。 我们还将探讨如何在 Data Lake Blob 存储中以 CSV 格式写入结果，从而为作业使用不同类型的输出。

### 创建 Azure 流分析作业以聚合订单数据

1. 在 Azure 门户的“主页”上，选择“+创建资源”，然后搜索 `Stream Analytics job`。 然后，使用以下属性创建“流分析作业”：
    - 基本信息：
        - **订阅**：Azure 订阅
        - **资源组**：选择现有的 dp203-xxxxxxx 资源组******。
        - **名称**：`aggregate-orders`
        - 区域：选择预配了 Synapse Analytics 工作区的<u>同一区域</u>。
        - 托管环境：云
        - 流单元：1
    - **存储**：
        - **添加存储帐户**：已选择
        - 订阅：Azure 订阅
        - **存储帐户**：选择 datalake*xxxxxxx* 存储帐户
        - 身份验证模式：连接字符串
        - 保护存储帐户中的专用数据：已选择
    - **标记**：
        - *无*

2. 等待部署完成，然后转到部署的流分析作业资源。

### 为原始订单数据创建输入

1. 在“aggregate-orders”概述页上，选择“输入”页面 。 使用“添加流输入”菜单添加具有以下属性的“事件中心”输入 ：
    - **输入别名**：`orders`
    - **从订阅选择事件中心**：选中
    - 订阅：Azure 订阅
    - **事件中心命名空间**：选择 events*xxxxxxx* 事件中心命名空间
    - **事件中心名称**：选择现有的 eventhub*xxxxxxx* 事件中心。
    - **事件中心使用者组**：选择现有 $Default 使用者组
    - **身份验证模式**：创建系统分配的托管标识
    - **分区键**：留空
    - **事件序列化格式**：JSON
    - **编码**：UTF-8
2. 保存输入并在创建时等待。 你将看到多个通知。 等待出现“连接测试成功”通知。

### 为 Data Lake Store 创建输出

1. 查看“aggregate-orders”流分析作业的“输出”页 。 然后使用“添加输出”菜单添加具有以下属性的“Blob 存储/ADLS Gen2”输出 ：
    - 输出别名：`datalake`
    - 从订阅中选择“从订阅中选择 Blob 存储/ADLS Gen2”：选中
    - 订阅：Azure 订阅
    - 存储帐户：选择 datalakexxxxxxx 存储帐户
    - **容器**：选择“使用现有项”，然后从列表中选择“文件”容器
    - 身份验证模式：连接字符串
    - 事件序列化格式：CSV - 逗号 (,)
    - **编码**：UTF-8
    - **写入模式**：在结果到达时追加
    - 路径模式：`{date}`
    - 日期格式：YYYY/MM/DD
    - 时间格式：不适用
    - 最小行数：20
    - 最大时间：0 小时，1 分钟，0 秒
2. 保存输出并等待创建。 你将看到多个通知。 等待出现“连接测试成功”通知。

### 创建查询以聚合事件数据

1. 查看“aggregate-orders”流分析作业的“查询”页 。
2. 按照以下方式修改默认查询：

    ```
    SELECT
        DateAdd(second,-5,System.TimeStamp) AS StartTime,
        System.TimeStamp AS EndTime,
        ProductID,
        SUM(Quantity) AS Orders
    INTO
        [datalake]
    FROM
        [orders] TIMESTAMP BY EventProcessedUtcTime
    GROUP BY ProductID, TumblingWindow(second, 5)
    HAVING COUNT(*) > 1
    ```

    注意到此查询使用 System.Timestamp（基于 EventProcessedUtcTime 字段）定义每 5 秒“滚动”（非重叠顺序）的开始和结束时间，计算该时间段内每个产品 ID 的总数量 。

3. 保存查询。

### 运行流式处理作业以聚合订单数据

1. 查看“aggregate-orders”流分析作业的“概述”页，并在“属性”选项卡上查看该作业的“输入”、“查询”、“输出”和“函数”      。 如果“输入”和“输出”数为 0，请使用“概述”页上的“&#8635; 刷新”按钮显示“orders”输入和“datalake”输出     。
2. 选择“&#9655; 开始”按钮，然后立即启动流式处理作业。 等待收到流式处理作业已成功启动的通知。
3. 重新打开 Cloud Shell 窗格，并重新运行以下命令以提交另外 100 个订单：

    ```
    node ~/dp-203/Allfiles/labs/18/orderclient
    ```

4. 订单应用完成后，最小化 Cloud Shell 窗格。 然后切换到 Synapse Studio 浏览器标签页，在“数据”页上的“链接”选项卡上，依次展开“Azure Data Lake Storage Gen2” > “synapsexxxxxxx (主 - datalakexxxxxxx)”，然后选择“files (主)”容器。
5. 如果 files 容器为空，请等待一分钟左右，然后使用“&#8635; 刷新”刷新视图。 最终，应显示一个以当前年份命名的文件夹。 这又包含月份和日期的文件夹。
6. 选择年份的文件夹，然后在“新建 SQL 脚本”菜单中选择“选择前 100 行”。 然后将“文件类型”设置为“文本格式”并应用设置。
7. 在打开的查询窗格中，修改查询以添加 `HEADER_ROW = TRUE` 参数，如下所示：

    ```sql
    SELECT
        TOP 100 *
    FROM
        OPENROWSET(
            BULK 'https://datalakexxxxxxx.dfs.core.windows.net/files/2023/**',
            FORMAT = 'CSV',
            PARSER_VERSION = '2.0',
            HEADER_ROW = TRUE
        ) AS [result]
    ```

8. 使用“&#9655; 运行”按钮运行 SQL 查询并查看结果，结果显示每五秒订购的每个产品的数量。
9. 返回到包含 Azure 门户的浏览器标签页，然后使用“&#128454; 停止”按钮停止流分析作业，并等待流分析作业已成功停止的通知。

## 删除 Azure 资源

你已完成对 Azure 流分析的探索，现在应删除已创建的资源，以避免产生不必要的 Azure 成本。

1. 关闭 Azure Synapse Studio 浏览器标签页并返回到 Azure 门户。
2. 在 Azure 门户的“主页”上，选择“资源组”。
3. 选择包含 Azure Synapse、事件中心和流分析资源的 dp203-xxxxxxx 资源组（不是托管资源组）。
4. 在资源组的“概述”页的顶部，选择“删除资源组”。
5. 输入 dp203-*xxxxxxx* 资源组名称以确认要删除该资源组，然后选择“删除” 。

    几分钟后，将删除本练习中创建的资源。
