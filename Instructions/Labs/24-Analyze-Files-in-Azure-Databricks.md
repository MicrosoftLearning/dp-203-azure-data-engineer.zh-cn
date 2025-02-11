---
lab:
  title: 在 Azure Databricks 中使用 Spark
  ilt-use: Lab
---

# 在 Azure Databricks 中使用 Spark

Azure Databricks 是基于 Microsoft Azure 的常用开源 Databricks 平台的一个版本。 Azure Databricks 基于 Apache Spark 构建，为涉及处理文件中数据的数据工程和分析任务提供了高度可缩放的解决方案。 Spark 的优点之一是支持各种编程语言，包括 Java、Scala、Python 和 SQL；这让 Spark 一种非常灵活的数据处理工作负载（包括数据清理和操作、统计分析和机器学习以及数据分析和可视化）解决方案。

完成此练习大约需要 45 分钟。

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
    cd dp-203/Allfiles/labs/24
    ./setup.ps1
    ```

6. 如果出现提示，请选择要使用的订阅（仅当有权访问多个 Azure 订阅时才会发生这种情况）。

7. 等待脚本完成 - 这通常需要大约 5 分钟，但在某些情况下可能需要更长的时间。 在等待时，请查看 Azure Databricks 文档中的 [Azure Databricks 上的探索式数据分析](https://learn.microsoft.com/azure/databricks/exploratory-data-analysis/)一文。

## 创建群集

Azure Databricks 是一个分布式处理平台，可使用 Apache Spark 群集在多个节点上并行处理数据。 每个群集由一个用于协调工作的驱动程序节点和多个用于执行处理任务的工作器节点组成。

> **提示**：如果 Azure Databricks 工作区中已有具有 13.3 LTS 运行时版本的群集，则可以使用它来完成本练习并跳过此过程。

1. 在 Azure 门户中，浏览到脚本创建的 **dp203-*xxxxxxx*** 资源组（或包含现有 Azure Databricks 工作区的资源组）
1. 选择 Azure Databricks 服务资源（如果使用安装脚本创建，则名为 **databricks*xxxxxxx***）。
1. 在工作区的“概述”**** 页中，使用“启动工作区”**** 按钮在新的浏览器标签页中打开 Azure Databricks 工作区；请在出现提示时登录。

    > 提示：使用 Databricks 工作区门户时，可能会显示各种提示和通知。 消除这些内容，并按照提供的说明完成本练习中的任务。

1. 查看 Azure Databricks 工作区门户，请注意，左侧边栏包含可执行的各种任务的图标。

1. 选择“(+)新建”任务，然后选择“群集” 。
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

## 使用笔记本探索数据

与许多 Spark 环境一样，Databricks 支持使用笔记本来合并笔记和交互式代码单元格，可用于探索数据。

1. 在工作区的 Azure Databricks 工作区门户的左侧边栏中，选择“工作区”****。 然后选择“&#8962; 主页”文件夹。
1. 在页面顶部的用户名旁边的 &#8942; 菜单中，选择“导入” 。 然后在“导入”对话框中，选择“URL”并从 `https://github.com/MicrosoftLearning/dp-203-azure-data-engineer/raw/master/Allfiles/labs/24/Databricks-Spark.ipynb` 导入笔记本
1. 将笔记本连接到群集，并按照包含的说明进行操作；运行包含的单元格以探索文件中的数据。

## 删除 Azure Databricks 资源

你已完成对 Azure Databricks 的探索，现在必须删除已创建的资源，以避免产生不必要的 Azure 成本并释放订阅中的容量。

1. 关闭 Azure Databricks 工作区浏览器标签页，并返回到 Azure 门户。
2. 在 Azure 门户的“主页”上，选择“资源组”。
3. 选择 dp203-xxxxxxx 资源组（而不是托管资源组），并验证它是否包含 Azure Databricks 工作区。
4. 在资源组的“概述”页的顶部，选择“删除资源组”。
5. 输入 dp203-xxxxxxx 资源组名称以确认要删除该资源组，然后选择“删除” 。

    几分钟后，将删除资源组及其关联的托管工作区资源组。
