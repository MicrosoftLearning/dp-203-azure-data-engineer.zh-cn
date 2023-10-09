---
lab:
  title: 使用 Azure 流分析和 Microsoft Power BI 创建实时报表
  ilt-use: Suggested demo
---

# 使用 Azure 流分析和 Microsoft Power BI 创建实时报表

数据分析解决方案通常需要引入和处理数据流。 流处理与批处理的不同之处在于，流通常是无限制的 - 换句话说，它们是连续的数据源，必须持续进行处理，而不是以固定的时间间隔进行处理。

Azure 流分析提供了一种云服务，可用于定义对流式处理源（如 Azure 事件中心或 Azure IoT 中心）数据流进行操作的查询。 可以使用 Azure 流分析查询来处理数据流，并将结果直接发送到 Microsoft Power BI 以进行实时可视化。

在本练习中，你将使用 Azure 流分析处理销售订单数据流，例如从联机零售应用程序生成的数据流。 订单数据将发送到 Azure 事件中心，Azure 流分析作业将从该位置读取和汇总数据，然后再将其发送到 Power BI，在 Power BI 中，你将在报表中可视化数据。

完成此练习大约需要 45 分钟。

## 准备工作

需要一个你在其中具有管理级权限的 [Azure 订阅](https://azure.microsoft.com/free)。

还将需要访问 Microsoft Power BI 服务。 你的学校或组织可能已经提供此服务，或者你也可以[以个人身份注册 Power BI 服务](https://learn.microsoft.com/power-bi/fundamentals/service-self-service-signup-for-power-bi)。

## 预配 Azure 资源

在本练习中，你需要一个可以访问 Data Lake Storage 的 Azure Synapse Analytics 工作区，以及一个专用 SQL 池。 还需要一个 Azure 事件中心命名空间，可将流式处理订单数据发送到该命名空间。

你将组合使用 PowerShell 脚本和 ARM 模板来预配这些资源。

1. 登录到 Azure 门户，地址为 [](https://portal.azure.com)。
2. 使用页面顶部搜索栏右侧的 [\>_] 按钮在 Azure 门户中创建新的 Cloud Shell，在出现提示时选择“PowerShell”环境并创建存储。 Cloud Shell 在 Azure 门户底部的窗格中提供命令行界面，如下所示：

    ![带有 Cloud Shell 窗格的 Azure 门户的屏幕截图。](./images/cloud-shell.png)

    > 注意：如果以前创建了使用 Bash 环境的 Cloud shell，请使用 Cloud Shell 窗格左上角的下拉菜单将其更改为“PowerShell”。

3. 请注意，可以通过拖动窗格顶部的分隔条或使用窗格右上角的 &#8212;、&#9723; 或 X 图标来调整 Cloud Shell 的大小，以最小化、最大化和关闭窗格  。 有关如何使用 Azure Cloud Shell 的详细信息，请参阅 [Azure Cloud Shell 文档](https://docs.microsoft.com/azure/cloud-shell/overview)。

4. 在 PowerShell 窗格中，输入以下命令以克隆包含此练习的存储库：

    ```
    rm -r dp-203 -f
    git clone https://github.com/MicrosoftLearning/dp-203-azure-data-engineer dp-203
    ```

5. 克隆存储库后，输入以下命令以更改为此练习的文件夹，然后运行其中包含的 setup.ps1 脚本：

    ```
    cd dp-203/Allfiles/labs/19
    ./setup.ps1
    ```

6. 如果出现提示，请选择要使用的订阅（仅当有权访问多个 Azure 订阅时才会发生这种情况）。

7. 在等待脚本完成时，请继续执行下一个任务。

## 创建 Power BI 工作区

在 Power BI 服务中，可以在工作区中组织数据集、报表和其他资源。 每个 Power BI 用户都有一个名为“我的工作区”的默认工作区，可在此练习中使用该工作区；但通常最好为要管理的每个离散报告解决方案创建一个工作区。

1. 使用 Power BI 服务凭据在 [https://app.powerbi.com/](https://app.powerbi.com/) 登录 Power BI 服务。
2. 在左侧菜单栏中，选择“工作区”（图标类似于 &#128455;）。
3. 创建一个新工作区并提供有意义的名称（例如 mslearn-streaming），并选择 Pro 许可模式。

    > 注意：如果使用的是试用帐户，可能需要启用其他试用功能。

4. 查看工作区时，请注意页面 URL 中的全局唯一标识符 (GUID)（应类似于 `https://app.powerbi.com/groups/<GUID>/list`）。 稍后需要用到此 GUID。

## 使用 Azure 流分析处理流数据

Azure 流分析作业可定义永久查询，该查询可对一个或多个输入的流数据进行操作，并将结果发送到一个或多个输出。

### 创建流分析作业

1. 切换回包含 Azure 门户的浏览器选项卡，脚本完成后，请注意预配 dp203-*xxxxxxx* 资源组的区域。
2. 在 Azure 门户的主页上，选择“+创建资源”并搜索 `Stream Analytics job` 。 然后，使用以下属性创建“流分析作业”：
    - 订阅：Azure 订阅
    - **资源组**：选择现有的 dp203-xxxxxxx 资源组**。
    - **名称**：`stream-orders`
    - 区域：选择预配 Synapse Analytics 工作区的区域。
    - 托管环境：云
    - 流单元：1
3. 等待部署完成，然后转到部署的流分析作业资源。

### 为事件数据流创建输入

1. 在“stream-orders”概述页上，选择“输入”页面，然后使用“添加流输入”菜单添加具有以下属性的“事件中心”输入   ：
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
2. 保存输入并在创建时等待。 你将看到多个通知。 等待“连接测试成功”通知。

### 为 Power BI 工作区创建输出

1. 查看“流订单”流分析作业的“输出”页 。 然后使用“添加输出”菜单添加具有以下属性的 Power BI 输出 ：
    - **输出别名**：`powerbi-dataset`
    - **手动选择 Power BI 设置**：已选中
    - **组工作区**：工作区的 GUID
    - **身份验证模式**：选择“用户令牌”，然后使用底部的“授权”按钮登录到 Power BI 帐户
    - **数据集名称**：`realtime-data`
    - **表名**：`orders`

2. 保存输出并在创建时等待。 你将看到多个通知。 等待“连接测试成功”通知。

### 创建查询来汇总事件流

1. 查看“流订单”流分析作业的“查询”页 。
2. 按照以下方式修改默认查询：

    ```
    SELECT
        DateAdd(second,-5,System.TimeStamp) AS StartTime,
        System.TimeStamp AS EndTime,
        ProductID,
        SUM(Quantity) AS Orders
    INTO
        [powerbi-dataset]
    FROM
        [orders] TIMESTAMP BY EventEnqueuedUtcTime
    GROUP BY ProductID, TumblingWindow(second, 5)
    HAVING COUNT(*) > 1
    ```

    请注意，此查询使用 System.Timestamp（基于 EventEnqueuedUtcTime 字段）定义每 5 秒“滚动”（非重叠顺序）的开始和结束时间，计算该时间段内每个产品 ID 的总数量 。

3. 保存查询。

### 运行流式处理作业以处理订单数据

1. 查看“流订单”流分析作业的“概述”页，并在“属性”选项卡上查看该作业的“输入”、“查询”、“输出”和“函数”      。 如果“输入”和“输出”数为 0，请使用“概述”页上的“&#8635; 刷新”按钮显示“订单”输入和“Power BI 数据集”输出     。
2. 选择“&#9655; 开始”按钮，然后立即启动流式处理作业。 等待收到流式处理作业已成功启动的通知。
3. 重新打开 Cloud Shell 窗格并运行以下命令以提交 100 个订单。

    ```
    node ~/dp-203/Allfiles/labs/19/orderclient
    ```

4. 当订单客户端应用运行时，切换到 Power BI 应用浏览器选项卡并查看工作区。
5. 刷新 Power BI 应用页，直到在工作区中看到“实时数据”数据集。 此数据集由 Azure 流分析作业生成。

## 在 Power BI 中可视化流式处理数据

现在，你已拥有流式处理订单数据的数据集，接下来可以创建一个 Power BI 仪表板，以直观方式表示该数据集。

1. 返回到 PowerBI 浏览器选项卡。

2. 在工作区的“+ 新建”下拉菜单中，选择“仪表板”，并创建名为“订单跟踪”的新仪表板  。

3. 在“订单跟踪”仪表板上，选择“&#9999;&#65039; 编辑”菜单中，然后选择“添加磁贴”  。 然后在“添加磁贴”窗格中，选择“自定义流数据”，再选择“下一步”  ：

4. 在“添加自定义流数据磁贴”窗格中的“你的数据集”下，选择“实时数据”数据集，然后选择“下一步”   。

5. 将默认可视化效果类型更改为“折线图”。 然后设置以下属性并选择“下一步”：
    - **轴**：结束时间
    - **值**：订单
    - **显示的时段**：1 分钟

6. 在“磁贴详细信息”窗格中，将“标题”设置为“实时订单计数”，然后选择“应用”   。

7. 切换回包含 Azure 门户的浏览器选项卡，如有必要，请重新打开 Cloud Shell 窗格。 然后重新运行以下命令，再提交另外 100 个订单。

    ```
    node ~/dp-203/Allfiles/labs/19/orderclient
    ```

8. 当订单提交脚本正在运行时，请切换回包含“订单跟踪”Power BI 仪表板的浏览器选项卡，并查看可视化效果更新以反映流分析作业（应仍在运行）处理的新订单数据。

    ![Power BI 报表的屏幕截图，其中显示了订单数据的实时流。](./images/powerbi-line-chart.png)

    可以重新运行 orderclient 脚本，并查看在实时仪表板中捕获的数据。

## 删除资源

你已完成对 Azure 流分析和 Power BI 的探索，现在应删除已创建的资源，以避免产生不必要的 Azure 成本。

1. 关闭包含 Power BI 报表的浏览器选项卡。 然后在“工作区”窗格中，在工作区的“&#8942;”菜单中选择“工作区设置”，然后删除工作区  。
2. 返回到包含 Azure 门户的浏览器选项卡，关闭 Cloud Shell 窗格，并使用“&#128454; 停止”按钮停止流分析作业。 等待流分析作业已成功停止的通知。
3. 在 Azure 门户的“主页”上，选择“资源组”。
4. 选择包含 Azure 事件中心和流分析资源的 dp203-*xxxxxxx* 资源组。
5. 在资源组的“概述”页的顶部，选择“删除资源组”。
6. 输入 dp203-xxxxxxx 资源组名称以确认要删除该资源组，然后选择“删除” 。

    几分钟后，将删除本练习中创建的资源。
