---
lab:
  title: 了解 Azure Databricks
  ilt-use: Suggested demo
---

# 了解 Azure Databricks

Azure Databricks 是基于 Microsoft Azure 的常用开源 Databricks 平台的一个版本。

与 Azure Synapse Analytics 类似，Azure Databricks 工作区提供了一个中心点，用于管理 Azure 上的 Databricks 群集、数据和资源**。

完成此练习大约需要 30 分钟。

## 准备工作

需要一个你在其中具有管理级权限的 [Azure 订阅](https://azure.microsoft.com/free)。

## 预配 Azure Databricks 工作区

在本练习中，你将使用脚本预配新的 Azure Databricks 工作区。

> **提示**：如果已有*标准版*或*试用版* Azure Databricks 工作区，则可以跳过此过程并使用现有工作区。

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
    cd dp-203/Allfiles/labs/23
    ./setup.ps1
    ```

6. 如果出现提示，请选择要使用的订阅（仅当有权访问多个 Azure 订阅时才会发生这种情况）。

7. 等待脚本完成 - 这通常需要大约 5 分钟，但在某些情况下可能需要更长的时间。 在等待时，请查看 Azure Databricks 文档中的[什么是 Azure Databricks？](https://learn.microsoft.com/azure/databricks/introduction/)一文。

## 创建群集

Azure Databricks 是一个分布式处理平台，可使用 Apache Spark 群集在多个节点上并行处理数据。 每个群集由一个用于协调工作的驱动程序节点和多个用于执行处理任务的工作器节点组成。

在本练习中，将创建一个*单节点*群集，以最大程度地减少实验室环境中使用的计算资源（在实验室环境中，资源可能会受到限制）。 在生产环境中，通常会创建具有多个工作器节点的群集。

> **提示**：如果 Azure Databricks 工作区中已有具有 13.3 LTS 运行时版本的群集，则可以使用它来完成本练习并跳过此过程。

1. 在 Azure 门户中，浏览到脚本创建的 **dp203-*xxxxxxx*** 资源组（或包含现有 Azure Databricks 工作区的资源组）
1. 选择 Azure Databricks 服务资源（如果使用安装脚本创建，则名为 **databricks*xxxxxxx***）。
1. 在工作区的“概述”**** 页中，使用“启动工作区”**** 按钮在新的浏览器标签页中打开 Azure Databricks 工作区；请在出现提示时登录。

    > 提示：使用 Databricks 工作区门户时，可能会显示各种提示和通知。 消除这些内容，并按照提供的说明完成本练习中的任务。

1. 查看 Azure Databricks 工作区门户，请注意，左侧边栏包含可执行的各种类型任务的链接。

1. 选择边栏中的“(+) 新建”**** 链接，然后选择“群集”****。
1. 在“新建群集”页中，使用以下设置创建新群集：
    - 群集名称：用户名的群集（默认群集名称）
    - 群集模式：单节点
    - 访问模式：单用户（选择你的用户帐户）
    - **Databricks 运行时版本**：13.3 LTS（Spark 3.4.1、Scala 2.12）
    - 使用 Photon 加速：已选择
    - 节点类型：Standard_DS3_v2
    - 在处于不活动状态 30 分钟后终止

1. 等待群集创建完成。 这可能需要一到两分钟时间。

> 注意：如果群集无法启动，则订阅在预配 Azure Databricks 工作区的区域中的配额可能不足。 请参阅 [CPU 内核限制阻止创建群集](https://docs.microsoft.com/azure/databricks/kb/clusters/azure-core-limit)，了解详细信息。 如果发生这种情况，可以尝试删除工作区，并在其他区域创建新工作区。 可以将区域指定为设置脚本的参数，如下所示：`./setup.ps1 eastus`

## 使用 Spark 分析数据文件

与许多 Spark 环境一样，Databricks 支持使用笔记本来合并笔记和交互式代码单元格，可用于探索数据。

1. 在边栏中，使用“(+) 新建”**** 链接创建**笔记本**。
1. 将默认笔记本名称 (**Untitled Notebook *[date]***) 更改为“浏览产品”****，然后在“连接”**** 下拉列表中选择你的群集（如果尚未选择）。 如果群集未运行，可能需要一分钟左右才能启动。
1. 将 [products.csv](https://raw.githubusercontent.com/MicrosoftLearning/dp-203-azure-data-engineer/master/Allfiles/labs/23/adventureworks/products.csv) 文件下载到本地计算机，并将其另存为 products.csv 。 然后，在“Explore products”笔记本中的“文件”菜单上，选择“将数据上传到 DBFS”  。
1. 在“上传数据”对话框中，记下要将文件上传到的“DBFS 目标目录”********。 然后选择“文件”区域，并将下载到计算机的 products.csv 文件上传 。 在上传该文件后，选择“下一步”
1. 在“从笔记本访问文件”窗格中，选择示例 PySpark 代码并将其复制到剪贴板****。 你将使用该代码将文件中的数据加载到 DataFrame 中。 然后选择“完成”。
1. 在“浏览产品”笔记本的空白代码单元格中，粘贴复制的代码，应如下所示****：

    ```python
    df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/user@outlook.com/products.csv")
    ```

1. 使用单元格右上角的“&#9656; 运行单元格”菜单选项来运行该代码，启动并在出现提示时附加群集****。
1. 等待代码运行的 Spark 作业完成。 代码根据已上传的文件中的数据创建了一个名为 df1 的 dataframe 对象******。
1. 在现有代码单元格下，使用 + 图标添加新的代码单元格****。 然后在新单元格中，输入以下代码：

    ```python
    display(df1)
    ```

1. 使用右上角的“&#9656; 运行新的单元格”菜单选项来运行该代码****。 此代码显示 dataframe 的内容，其内容应如下所示：

    | ProductID | ProductName | 类别 | ListPrice |
    | -- | -- | -- | -- |
    | 771 | Mountain-100 Silver, 38 | 山地自行车 | 3399.9900 |
    | 772 | Mountain-100 Silver, 42 | 山地自行车 | 3399.9900 |
    | [.] | [.] | [.] | [.] |

1. 在结果表上方，选择 +，然后选择“可视化效果”以查看可视化效果编辑器，然后应用以下选项********：
    - **可视化效果类型**：条形图
    - **X 列**：类别
    - **Y 列**：添加新列并选择“ProductID”******。 应用“计数”聚合********。

    保存可视化效果，并观察它是否显示在笔记本中，如下所示：

    ![按类别显示产品计数的条形图](./images/databricks-chart.png)

## 创建表并进行查询

虽然许多数据分析都习惯于使用 Python 或 Scala 等语言来处理文件中的数据，但许多数据分析解决方案都是建立在关系数据库之上的；其中数据存储在表中并使用 SQL 进行操作。

1. 在“浏览产品”笔记本中，在先前运行的代码单元格的图表输出下，使用 + 图标添加新单元格********。
2. 在新单元格中，输入并运行以下代码：

    ```python
    df1.write.saveAsTable("products")
    ```

3. 单元格完成后，使用以下代码在其下添加新单元格：

    ```sql
    %sql

    SELECT ProductName, ListPrice
    FROM products
    WHERE Category = 'Touring Bikes';
    ```

4. 运行新单元格，其中包含用于返回“旅行自行车”类别中产品名称和价格的 SQL 代码**。
5. 在边栏中，选择“目录”**** 链接，并验证是否已在默认数据库架构（其名称一般为 **default**）中创建**产品**表。 可以使用 Spark 代码来创建自定义数据库架构和关系表架构，数据分析师可以使用它们来探索数据和生成分析报告。

## 删除 Azure Databricks 资源

你已完成对 Azure Databricks 的探索，现在必须删除已创建的资源，以避免产生不必要的 Azure 成本并释放订阅中的容量。

1. 关闭 Azure Databricks 工作区浏览器标签页，并返回到 Azure 门户。
2. 在 Azure 门户的“主页”上，选择“资源组”。
3. 选择 dp203-xxxxxxx 资源组（而不是托管资源组），并验证它是否包含 Azure Databricks 工作区。
4. 在资源组的“概述”页的顶部，选择“删除资源组”。
5. 输入 dp203-xxxxxxx 资源组名称以确认要删除该资源组，然后选择“删除” 。

    几分钟后，将删除资源组及其关联的托管工作区资源组。
