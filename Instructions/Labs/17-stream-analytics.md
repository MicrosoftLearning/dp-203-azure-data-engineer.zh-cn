---
lab:
  title: Azure 流分析入门
  ilt-use: Suggested demo
---

# Azure 流分析入门

在本练习中，你将在 Azure 订阅中预配 Azure 流分析作业，并使用该作业来查询和汇总实时事件数据流，并将结果存储在 Azure 存储中。

完成此练习大约需要 15 分钟。

## 开始之前

需要一个你在其中具有管理级权限的 [Azure 订阅](https://azure.microsoft.com/free)。

## 预配 Azure 资源

在本练习中，你将捕获模拟销售交易数据流，对其进行处理，并将结果存储在 Azure 存储中的 Blob 容器中。 你需要 Azure 事件中心命名空间和 Azure 存储帐户，可将流式处理数据发送到前者，后者用于存储流式处理的结果。

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
    cd dp-203/Allfiles/labs/17
    ./setup.ps1
    ```

6. 如果出现提示，请选择要使用的订阅（仅当有权访问多个 Azure 订阅时才会发生这种情况）。
7. 等待脚本完成 - 这通常需要大约 5 分钟，但在某些情况下可能需要更长的时间。 在等待期间，请查看 Azure 流分析文档中的[欢迎使用 Azure 流分析](https://learn.microsoft.com/azure/stream-analytics/stream-analytics-introduction)一文。

## 查看流式处理数据源

在创建用于处理实时数据的 Azure 流分析作业之前，让我们看一下它需要查询的数据流。

1. 安装脚本运行完毕后，重设 Cloud Shell 窗格的大小或将其最小化，这样你就可以看到 Azure 门户（稍后将返回到 Cloud Shell）。 然后在 Azure 门户中，转到它创建的 dp203-xxxxxxx 资源组，注意该资源组包含 Azure 存储帐户和事件中心命名空间**。

    请记下预配资源的“位置”- 稍后，你将在同一位置创建 Azure 流分析作业。

2. 重新打开 Cloud Shell 窗格，并输入以下命令以运行向 Azure 事件中心发送 100 个模拟订单的客户端应用：

    ```
    node ~/dp-203/Allfiles/labs/17/orderclient
    ```

3. 在发送时观察销售订单数据，每个订单都包含产品 ID 和数量。 应用将在发送 1000 个订单后结束，这需要一分钟左右的时间。

## 创建 Azure 流分析作业

现在你已准备好创建 Azure 流分析作业来处理到达事件中心的销售交易数据。

1. 在 Azure 门户的“dp203-xxxxxxx” 页面上，选择“+ 创建”，然后搜索 `Stream Analytics job` **。 然后，使用以下属性创建“流分析作业”：
    - 基本信息：
        - **订阅**：Azure 订阅
        - **资源组**：选择现有的 dp203-xxxxxxx 资源组**。
        - **名称**：`process-orders`
        - **区域**：选择在其中预配其他 Azure 资源的区域。
        - 托管环境：云
        - 流单元：1
    - **存储**：
        - **保护存储帐户中的专用数据**：未选择
    - **标记**：
        - *无*
2. 等待部署完成，然后转到部署的流分析作业资源。

## 为事件流创建输入

Azure 流分析作业必须从记录销售订单的事件中心获取输入数据。

1. 在“process-orders”概述页上，选择“添加输入” 。 然后在“输入”页上，使用“添加流输入”菜单添加具有以下属性的“事件中心”输入  ：
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

## 为 Blob 存储创建输出

你将以 JSON 格式将聚合的销售订单数据存储在 Azure 存储 Blob 容器中。

1. 查看“process-orders”流分析作业的“输出”页 。 然后使用“添加”菜单添加具有以下属性的“Blob 存储/ADLS Gen2”输出 ：
    - 输出别名：`blobstore`
    - 从订阅中选择“从订阅中选择 Blob 存储/ADLS Gen2”：选中
    - 订阅：Azure 订阅
    - **存储帐户**：选择 storexxxxxxx 存储帐户**
    - **容器**：选择现有的数据容器
    - **身份验证模式**：托管标识：系统分配
    - **事件序列化格式**：JSON
    - **格式**：行分隔
    - **编码**：UTF-8
    - **写入模式**：在结果到达时追加
    - 路径模式：`{date}`
    - 日期格式：YYYY/MM/DD
    - 时间格式：不适用
    - 最小行数：20
    - 最大时间：0 小时，1 分钟，0 秒
2. 保存输出并等待创建。 你将看到多个通知。 等待出现“连接测试成功”通知。

## 创建查询

现在你已为 Azure 流分析作业定义了输入和输出，可以使用查询选择、筛选和聚合输入中的数据，并将结果发送到输出。

1. 查看“process-orders”流分析作业的“查询”页 。 然后稍等片刻，直到（根据之前在事件中心捕获的销售订单事件）显示输入预览。
2. 注意到输入数据包括客户端应用提交的消息中的 ProductID 和 Quantity 字段，还有一些其他事件中心字段，其中包括 EventProcessedUtcTime 字段，它指示事件何时添加到事件中心。
3. 按照以下方式修改默认查询：

    ```
    SELECT
        DateAdd(second,-10,System.TimeStamp) AS StartTime,
        System.TimeStamp AS EndTime,
        ProductID,
        SUM(Quantity) AS Orders
    INTO
        [blobstore]
    FROM
        [orders] TIMESTAMP BY EventProcessedUtcTime
    GROUP BY ProductID, TumblingWindow(second, 10)
    HAVING COUNT(*) > 1
    ```

    注意到此查询使用 System.Timestamp（基于 EventProcessedUtcTime 字段）定义每 10 秒“滚动”（非重叠顺序）时段的开始和结束时间，计算该时间段内每个产品 ID 的总数量 。

4. 使用“&#9655; 测试查询”按钮来验证查询，并确保“测试结果”状态指示“成功”（即使未返回任何行）  。
5. 保存查询。

## 运行流式处理作业

现在你已准备好运行作业并处理一些实时销售订单数据。

1. 查看“process-orders”流分析作业的“概述”页，并在“属性”选项卡上查看该作业的“输入”、“查询”、“输出”和“函数”      。 如果“输入”和“输出”数为 0，请使用“概述”页上的“&#8635; 刷新”按钮显示“orders”输入和“blobstore”输出     。
2. 选择“&#9655; 开始”按钮，然后立即启动流式处理作业。 等待收到流式处理作业已成功启动的通知。
3. 重新打开 Cloud Shell 窗格并重新连接（如有必要），然后重新运行以下命令以提交另外 1000 个订单。

    ```
    node ~/dp-203/Allfiles/labs/17/orderclient
    ```

4. 运行应用时，在 Azure 门户中，返回到 dp203-xxxxxxx 资源组的页面，然后选择 storexxxxxxxxxxxx 存储帐户 ****。
6. 在存储帐户边栏选项卡左侧的窗格中，选择“容器”选项卡。
7. 打开“数据”容器，并使用“&#8635; 刷新”按钮刷新视图，直到看到名称为当前年份的文件夹 。
8. 在“数据”容器中，在文件夹层次结构上导航，其中包括当前年份的文件夹，以及月份和日期的子文件夹。
9. 在小时文件夹中，请注意已创建的文件，该文件的名称应类似于 0_xxxxxxxxxxxxxxxx.json。
10. 在文件的“...”菜单（位于文件详细信息右侧）上选择“查看/编辑”，然后查看文件的内容；其中应包含每 10 秒时间段的 JSON 记录，显示为每个产品 ID 处理的订单数，如下所示 ：

    ```
    {"StartTime":"2022-11-23T18:16:25.0000000Z","EndTime":"2022-11-23T18:16:35.0000000Z","ProductID":6,"Orders":13.0}
    {"StartTime":"2022-11-23T18:16:25.0000000Z","EndTime":"2022-11-23T18:16:35.0000000Z","ProductID":8,"Orders":15.0}
    {"StartTime":"2022-11-23T18:16:25.0000000Z","EndTime":"2022-11-23T18:16:35.0000000Z","ProductID":5,"Orders":15.0}
    {"StartTime":"2022-11-23T18:16:25.0000000Z","EndTime":"2022-11-23T18:16:35.0000000Z","ProductID":1,"Orders":16.0}
    {"StartTime":"2022-11-23T18:16:25.0000000Z","EndTime":"2022-11-23T18:16:35.0000000Z","ProductID":3,"Orders":10.0}
    {"StartTime":"2022-11-23T18:16:25.0000000Z","EndTime":"2022-11-23T18:16:35.0000000Z","ProductID":2,"Orders":25.0}
    {"StartTime":"2022-11-23T18:16:25.0000000Z","EndTime":"2022-11-23T18:16:35.0000000Z","ProductID":7,"Orders":13.0}
    {"StartTime":"2022-11-23T18:16:25.0000000Z","EndTime":"2022-11-23T18:16:35.0000000Z","ProductID":4,"Orders":12.0}
    {"StartTime":"2022-11-23T18:16:25.0000000Z","EndTime":"2022-11-23T18:16:35.0000000Z","ProductID":10,"Orders":19.0}
    {"StartTime":"2022-11-23T18:16:25.0000000Z","EndTime":"2022-11-23T18:16:35.0000000Z","ProductID":9,"Orders":8.0}
    {"StartTime":"2022-11-23T18:16:35.0000000Z","EndTime":"2022-11-23T18:16:45.0000000Z","ProductID":6,"Orders":41.0}
    {"StartTime":"2022-11-23T18:16:35.0000000Z","EndTime":"2022-11-23T18:16:45.0000000Z","ProductID":8,"Orders":29.0}
    ...
    ```

11. 在 Azure Cloud Shell 窗格中，等待订单客户端应用完成。
12. 在 Azure 门户中，刷新文件以查看生成的完整结果集。
13. 返回到 dp203-xxxxxxx 资源组，并重新打开 process-orders 流分析作业 **。
14. 在流分析作业页的顶部，使用“&#11036; 停止”按钮停止作业，并在出现提示时进行确认。

## 删除 Azure 资源

你已完成对 Azure 流分析的探索，现在应删除已创建的资源，以避免产生不必要的 Azure 成本。

1. 在 Azure 门户的**主页**上，选择“资源组”。
2. 选择包含 Azure 存储、事件中心和流分析资源的 dp203-xxxxxxx 资源组**。
3. 在资源组的“概述”页的顶部，选择“删除资源组”。
4. 输入 dp203-xxxxxxx 资源组名称以确认要删除该资源组，然后选择“删除” 。

    几分钟后，将删除本练习中创建的资源。
