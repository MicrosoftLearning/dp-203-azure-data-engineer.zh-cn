---
lab:
  title: 在 Azure Databricks 中使用 SQL 仓库
  ilt-use: Optional demo
---

# 在 Azure Databricks 中使用 SQL 仓库

SQL 是用于查询和操作数据的行业标准语言。 许多数据分析师通过使用 SQL 查询关系数据库中的表来执行数据分析。 Azure Databricks 包括基于 Spark 和 Delta Lake 技术生成的 SQL 功能，可针对数据湖中的文件提供关系数据库层。

完成此练习大约需要 30 分钟。

## 准备工作

你需要一个 [Azure 订阅](https://azure.microsoft.com/free)，在该订阅中，你至少在一个区域中具有管理级别的访问权限和足够的配额来预配 Azure Databricks SQL 仓库。

## 预配 Azure Databricks 工作区

在本练习中，需要一个高级层 Azure Databricks 工作区。

1. 在 Web 浏览器中，登录到 [Azure 门户](https://portal.azure.com)，网址为 `https://portal.azure.com`。
2. 使用页面顶部搜索栏右侧的 [\>_] 按钮在 Azure 门户中创建新的 Cloud Shell，在出现提示时选择“PowerShell”环境并创建存储。 Cloud Shell 在 Azure 门户底部的窗格中提供命令行界面，如下所示：

    ![具有 Cloud Shell 窗格的 Azure 门户](./images/cloud-shell.png)

    > 注意：如果以前创建了使用 Bash 环境的 Cloud shell，请使用 Cloud Shell 窗格左上角的下拉菜单将其更改为“PowerShell”。

3. 请注意，可以通过拖动窗格顶部的分隔条或使用窗格右上角的 &#8212;、&#9723; 或 X 图标来调整 Cloud Shell 的大小，以最小化、最大化和关闭窗格  。 有关如何使用 Azure Cloud Shell 的详细信息，请参阅 [Azure Cloud Shell 文档](https://docs.microsoft.com/azure/cloud-shell/overview)。

4. 在 PowerShell 窗格中，输入以下命令以克隆此存储库：

    ```
    rm -r dp-203 -f
    git clone https://github.com/MicrosoftLearning/dp-203-azure-data-engineer dp-203
    ```

5. 克隆存储库后，输入以下命令以更改为此实验室的文件夹，然后运行其中包含的 setup.ps1 脚本：

    ```
    cd dp-203/Allfiles/labs/26
    ./setup.ps1
    ```

6. 如果出现提示，请选择要使用的订阅（仅当有权访问多个 Azure 订阅时才会发生这种情况）。

7. 等待脚本完成 - 这通常需要大约 5 分钟，但在某些情况下可能需要更长的时间。 在等待时，请查看 Azure Databricks 文档中的[什么是 Azure Databricks 上的数据仓库？](https://learn.microsoft.com/azure/databricks/sql/)一文。

## 查看和启动 SQL 仓库

1. 部署 Azure Databricks 工作区资源后，请在 Azure 门户转到该资源。
2. 在 Azure Databricks 工作区的“概述”页中，使用“启动工作区”按钮在新的浏览器选项卡中打开 Azure Databricks 工作区；并在出现提示时登录 。
3. 如果显示“当前数据项目是什么？”的消息，请选择“完成”将其关闭 。 然后，查看 Azure Databricks 工作区门户，注意左侧边栏包含任务类别的名称。

    >提示：使用 Databricks 工作区门户时，可能会显示各种提示和通知。 消除这些内容，并按照提供的说明完成本练习中的任务。

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
    CREATE SCHEMA adventureworks;
    ```

4. 使用“&#9658;运行(1000)”按钮运行 SQL 代码。
5. 成功执行代码后，在“架构浏览器”窗格中，使用窗格底部的“刷新”按钮刷新列表。 然后，展开 hive_metastore 和 adventureworks，查看数据库是否已创建，但不包含任何表 。

可以将“默认”数据库用于表，但在生成分析数据存储时，最好为特定数据创建自定义数据库。

## 创建表

1. 将 [products.csv](https://raw.githubusercontent.com/MicrosoftLearning/dp-203-azure-data-engineer/master/Allfiles/labs/26/data/products.csv) 文件下载到本地计算机，并将其另存为 products.csv 。
1. 在 Azure Databricks 工作区门户的边栏中，选择“(+) 新建”，然后选择“文件上传”，将下载的 products.csv 文件上传到计算机  。
1. 在“上传数据”页中，选择 adventureworks 架构并将表名设置为 products  。 然后，选择页面左下角的“创建表”。
1. 创建表后，查看其详细信息。

通过从文件导入数据来创建表的功能，可以轻松填充数据库。 还可以使用 Spark SQL 通过代码创建表。 表本身是配置单元元存储中的元数据定义，它们包含的数据以增量格式存储在 Databricks 文件系统 (DBFS) 存储中。

## 创建查询

1. 在边栏中，选择“(+) 新建”，然后选择“查询” 。
2. 在“架构浏览器”窗格中，展开 hive_metastore 和 adventureworks，并验证 products 表是否已列出   。
3. 在“新建查询”窗格中，输入以下 SQL 代码：

    ```sql
    SELECT ProductID, ProductName, Category
    FROM adventureworks.products; 
    ```

4. 使用“&#9658;运行(1000)”按钮运行 SQL 代码。
5. 查询完成后，查看结果表。
6. 使用查询编辑器右上角的“保存”按钮将查询另存为“产品和类别” 。

保存查询便于以后再次检索相同的数据。

## 创建仪表板

1. 在边栏中，选择“(+) 新建”，然后选择“仪表板” 。
2. 在“新建仪表板”对话框中，输入名称 Adventure Works Products，然后选择“保存”  。
3. 在 Adventure Works Products 仪表板的“添加”下拉列表中，选择“可视化效果”  。
4. 在“添加可视化效果小组件”对话框中，选择“产品和类别”查询 。 然后选择“新建可视化效果”，将标题设置为“每个类别的产品” 。 然后选择“创建可视化效果”。
5. 在可视化效果编辑器中，设置以下属性：
    - **可视化效果类型**：条形图
    - **水平图表**：选中
    - **Y 列**：类别
    - **X 列**：产品 ID：计数
    - **分组依据**：类别
    - **图例放置**：自动（灵活）
    - **图例项顺序**：常规
    - **堆叠**：堆叠
    - **将值规范化为百分比**：<u>未</u>选中
    - 缺失值和 NULL 值：不在图表中显示

6. 保存可视化效果并在仪表板中进行查看。
7. 选择“完成编辑”以查看将显示给用户的仪表板。

仪表板是与业务用户共享数据表和可视化效果的好方法。 你可以计划定期刷新仪表板，并通过电子邮件发送给订阅者。

## 删除 Azure Databricks 资源

你已在 Azure Databricks 中完成对 SQL 仓库的探索，现在必须删除已创建的资源，以避免产生不必要的 Azure 成本并释放订阅中的容量。

1. 关闭 Azure Databricks 工作区浏览器选项卡，并返回到 Azure 门户。
2. 在 Azure 门户的“主页”上，选择“资源组”。
3. 选择包含 Azure Databricks 工作区的资源组（而不是托管资源组）。
4. 在资源组的“概述”页的顶部，选择“删除资源组”。
5. 输入资源组名称以确认要删除该资源组，然后选择“删除”。

    几分钟后，将删除资源组及其关联的托管工作区资源组。
