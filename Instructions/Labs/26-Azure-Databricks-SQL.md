---
lab:
  title: 在 Azure Databricks 中使用 SQL 仓库
  ilt-use: Optional demo
---

SQL 是用于查询和操作数据的行业标准语言。 许多数据分析师通过使用 SQL 查询关系数据库中的表来执行数据分析。 Azure Databricks 包括基于 Spark 和 Delta Lake 技术生成的 SQL 功能，可针对数据湖中的文件提供关系数据库层。

完成此练习大约需要 30 分钟。

## 预配 Azure Databricks 工作区

> **提示**：如果你已拥有*高级*或*试用版* Azure Databricks 工作区，则可以跳过此过程并使用现有工作区。

本练习包括一个用于预配新 Azure Databricks 工作区的脚本。 该脚本会尝试在一个区域中创建*高级*层 Azure Databricks 工作区资源，在该区域中，Azure 订阅具有本练习所需计算核心的充足配额；该脚本假设你的用户帐户在订阅中具有足够的权限来创建 Azure Databricks 工作区资源。 如果脚本由于配额或权限不足失败，可以尝试 [在 Azure 门户中以交互方式创建 Azure Databricks 工作区](https://learn.microsoft.com/azure/databricks/getting-started/#--create-an-azure-databricks-workspace)。

1. 在 Web 浏览器中，登录到 [Azure 门户](https://portal.azure.com)，网址为 `https://portal.azure.com`。
2. 使用页面顶部搜索栏右侧的 [\>_] 按钮在 Azure 门户中创建新的 Cloud Shell，在出现提示时选择“PowerShell”环境并创建存储。 Cloud Shell 在 Azure 门户底部的窗格中提供命令行界面，如下所示：

    ![具有 Cloud Shell 窗格的 Azure 门户](./images/cloud-shell.png)

    > **注意**：如果以前创建了使用 Bash 环境的 Cloud Shell，请使用 Cloud Shell 窗格左上角的下拉菜单将其更改为 PowerShell********。

3. 请注意，可以通过拖动窗格顶部的分隔条或使用窗格右上角的 &#8212;、&#9723; 或 X 图标来调整 Cloud Shell 的大小，以最小化、最大化和关闭窗格  。 有关如何使用 Azure Cloud Shell 的详细信息，请参阅 [Azure Cloud Shell 文档](https://docs.microsoft.com/azure/cloud-shell/overview)。

4. 在 PowerShell 窗格中，输入以下命令以克隆此存储库：

    ```
    rm -r mslearn-databricks -f
    git clone https://github.com/MicrosoftLearning/mslearn-databricks
    ```

5. 克隆存储库后，请输入以下命令以运行 **setup.ps1** 脚本，以在可用区域中预配 Azure Databricks 工作区：

    ```
    ./mslearn-databricks/setup.ps1
    ```

6. 如果出现提示，请选择要使用的订阅（仅当有权访问多个 Azure 订阅时才会发生这种情况）。
7. 等待脚本完成 - 这通常需要大约 5 分钟，但在某些情况下可能需要更长的时间。 在等待时，请查看 Azure Databricks 文档中的[什么是 Azure Databricks 上的数据仓库？](https://learn.microsoft.com/azure/databricks/sql/)一文。

## 查看和启动 SQL 仓库

1. 部署 Azure Databricks 工作区资源后，请在 Azure 门户转到该资源。
1. 在 Azure Databricks 工作区的“概述”页中，使用“启动工作区”按钮在新的浏览器选项卡中打开 Azure Databricks 工作区；并在出现提示时登录 。

    > 提示：使用 Databricks 工作区门户时，可能会显示各种提示和通知。 消除这些内容，并按照提供的说明完成本练习中的任务。

1. 查看 Azure Databricks 工作区门户，请注意，左侧边栏包含任务类别的名称。
1. 在边栏的“SQL”下，选择“SQL 仓库” 。
1. 请注意，工作区已包含名为“入门仓库”的 SQL 仓库。
1. 在 SQL 仓库的“操作”(&#8285;) 菜单中，选择“编辑”  。 然后将“群集大小”属性设置为 2X-Small 并保存更改 。
1. 使用“开始”按钮启动 SQL 仓库（可能需要一两分钟）。

> **注意**：如果 SQL 仓库无法启动，则订阅在预配 Azure Databricks 工作区的区域中的配额可能不足。 有关详细信息，请参阅[所需的 Azure vCPU 配额](https://docs.microsoft.com/azure/databricks/sql/admin/sql-endpoints#required-azure-vcpu-quota)。 如果发生这种情况，可以在仓库启动失败时尝试请求增加配额，如错误消息中所示。 或者，可以尝试删除工作区，并在其他区域创建新工作区。 可以将区域指定为设置脚本的参数，如下所示：`./setup.ps1 eastus`

## 创建数据库架构

1. SQL 仓库运行时，在边栏中选择“SQL 编辑器”。
2. 在“架构浏览器”窗格中，查看 hive_metastore 目录是否包含名为 default 的数据库。
3. 在“新建查询”窗格中，输入以下 SQL 代码：

    ```sql
   CREATE DATABASE retail_db;
    ```

4. 使用“&#9658;运行(1000)”按钮运行 SQL 代码。
5. 成功执行代码后，在“**架构浏览器**”窗格中，使用窗格底部的刷新按钮刷新列表。 然后，展开 **hive_metastore** 和 **retail_db**，查看数据库是否已创建，但不包含任何表。

可以将“**默认**”数据库用于表，但在生成分析数据存储时，最好为特定数据创建自定义数据库。

## 创建表

1. 将 [`products.csv`](https://raw.githubusercontent.com/MicrosoftLearning/mslearn-databricks/main/data/products.csv) 文件下载到本地计算机，并将其另存为 **products.csv**。
1. 在 Azure Databricks 工作区门户中的边栏中，选择“**(+)新建**”，然后选择“**数据**”。
1. 在“**添加数据**”页中，选择“**创建或修改表**”，并将下载的 **products.csv **文件上传到计算机。
1. 在“**从文件上传创建或修改表**”页中，选择 **retail_db** 架构并将表名设置为**产品**。 然后，选择页面右下角的“**创建表**”。
1. 创建表后，查看其详细信息。

通过从文件导入数据来创建表的功能，可以轻松填充数据库。 还可以使用 Spark SQL 通过代码创建表。 表本身是配置单元元存储中的元数据定义，它们包含的数据以增量格式存储在 Databricks 文件系统 (DBFS) 存储中。

## 创建仪表板

1. 在边栏中，选择“(+) 新建”，然后选择“仪表板” 。
2. 选择“新仪表板名称”并将其更改为 `Retail Dashboard`。
3. 在“**数据**”选项卡中，选择“**从 SQL** 创建”并使用以下查询：

    ```sql
   SELECT ProductID, ProductName, Category
   FROM retail_db.products; 
    ```

4. 选择“**运行**”，然后将无标题数据集重命名为 `Products and Categories`。
5. 选择“**画布**”选项卡，然后选择“**添加可视化效果**”。
6. 在可视化效果编辑器中，设置以下属性：
    
    - **数据集**：产品和类别
    - **可视化效果**：条形图
    - **X 轴**：计数 (ProductID)
    - **Y 轴**：类别

7. 选择“**发布**”以查看将显示给用户的仪表板。

仪表板是与业务用户共享数据表和可视化效果的好方法。 你可以计划定期刷新仪表板，并通过电子邮件发送给订阅者。

## 清理

在 Azure Databricks 门户中的“**SQL 仓库**”页上，选择你的 SQL 仓库并选择“**&#9632; 停止**”将其关闭。

如果已完成对 Azure Databricks 的探索，则可以删除已创建的资源，以避免产生不必要的 Azure 成本并释放订阅中的容量。
