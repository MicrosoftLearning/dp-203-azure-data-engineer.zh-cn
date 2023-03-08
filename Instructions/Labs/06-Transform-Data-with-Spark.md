---
lab:
  title: 在 Synapse Analytics 中使用 Spark 转换数据
  ilt-use: Lab
---

# 在 Synapse Analytics 中使用 Spark 转换数据

数据工程师通常使用 Spark 笔记本作为其首选工具之一来执行提取、转换和加载 (ETL) 或提取、加载和转换 (ELT) 活动，以便将数据从一种格式或结构转换为另一种格式或结构  。

在本练习中，你将使用 Azure Synapse Analytics 中的 Spark 笔记本来转换文件中的数据。

完成此练习大约需要 30 分钟。

## 准备工作

需要一个你在其中具有管理级权限的 [Azure 订阅](https://azure.microsoft.com/free)。

## 预配 Azure Synapse Analytics 工作区

需要一个 Azure Synapse Analytics 工作区才能访问 Data Lake Storage 和 Spark 池。

在本练习中，你将组合使用 PowerShell 脚本和 ARM 模板来预配 Azure Synapse Analytics 工作区。

1. 登录到 Azure 门户，地址为 [](https://portal.azure.com)。
2. 使用页面顶部搜索栏右侧的 [\>_] 按钮在 Azure 门户中创建新的 Cloud Shell，在出现提示时选择“PowerShell”环境并创建存储。 Cloud Shell 在 Azure 门户底部的窗格中提供命令行界面，如下所示：

    ![具有 Cloud Shell 窗格的 Azure 门户](./images/cloud-shell.png)

    > 注意：如果以前创建了使用 Bash 环境的 Cloud shell，请使用 Cloud Shell 窗格左上角的下拉菜单将其更改为“PowerShell”。

3. 请注意，可以通过拖动窗格顶部的分隔条或使用窗格右上角的 &#8212;、&#9723; 或 X 图标来调整 Cloud Shell 的大小，以最小化、最大化和关闭窗格  。 有关如何使用 Azure Cloud Shell 的详细信息，请参阅 [Azure Cloud Shell 文档](https://docs.microsoft.com/azure/cloud-shell/overview)。

4. 在 PowerShell 窗格中，输入以下命令以克隆此存储库：

    ```
    rm -r dp-203 -f
    git clone https://github.com/MicrosoftLearning/dp-203-azure-data-engineer dp-203
    ```

5. 克隆存储库后，输入以下命令以更改为此练习的文件夹，然后运行其中包含的 setup.ps1 脚本：

    ```
    cd dp-203/Allfiles/labs/06
    ./setup.ps1
    ```

6. 如果出现提示，请选择要使用的订阅（仅当有权访问多个 Azure 订阅时才会发生这种情况）。
7. 出现提示时，输入要为 Azure Synapse SQL 池设置的合适密码。

    > 注意：请务必记住此密码！

8. 等待脚本完成 - 此过程通常需要大约 10 分钟；但在某些情况下可能需要更长的时间。 等待时，请查看 Azure Synapse Analytics 文档中的 [Azure Synapse Analytics 中的 Apache Spark 的核心概念](https://learn.microsoft.com/azure/synapse-analytics/spark/apache-spark-concepts)一文。

## 使用 Spark 笔记本转换数据

1. 部署脚本完成后，在 Azure 门户转到它创建的 dp203-xxxxxxx 资源组，请注意，此资源组包含 Synapse 工作区、数据湖的存储帐户和 Apache Spark 池**。
2. 选择“Synapse 工作区”，在其“概述”页的“打开 Synapse Studio”卡中，选择“打开”，以在新浏览器标签页中打开 Synapse Studio；如果出现提示，请进行登录  。
3. 在 Synapse Studio 左侧，使用 &rsaquo;&rsaquo; 图标展开菜单，这将显示 Synapse Studio 中用于管理资源和执行数据分析任务的不同页面。
4. 在“管理”页上，选择“Apache Spark 池”选项卡，请注意工作区中已预配名称类似于 spark*xxxxxxx* 的 Spark 池  。
5. 在“数据”页上，查看“已链接”选项卡并验证工作区是否包含 Azure Data Lake Storage Gen2 存储帐户的链接，该帐户的名称应类似于 synapsexxxxxxx* (Primary - datalake xxxxxxx*) ** 。
6. 展开存储帐户，验证它是否包含名为“files (primary)”的文件系统容器。
7. 选择“files”容器，并注意它包含名为 data 和 synapse 的文件夹  。 “synapse”文件夹由 Azure Synapse 使用，而“data”文件夹包含要查询的数据文件。
8. 打开“data”文件夹并观察其中包含三年销售数据的 .csv 文件。
9. 右键单击任一文件，然后选择“预览”以查看它所包含的数据。 请注意，这些文件包含标题行，因此你可以选择显示列标题的选项。
10. 关闭预览。 然后在“开发”页上，展开“笔记本”，然后选择已提供的“Spark 转换”笔记本  。
11. 将笔记本附加到 sparkxxxxxxx Spark 池**。
12. 查看笔记本中的笔记并运行代码单元格。

    > 注意：第一个代码单元需要几分钟才能运行，因为必须启动 Spark 池。 后续单元格的运行速度会更快。

## 删除 Azure 资源

你已完成对 Azure Synapse Analytics 的探索，现在应删除已创建的资源，以避免产生不必要的 Azure 成本。

1. 关闭 Synapse Studio 浏览器选项卡并返回到 Azure 门户。
2. 在 Azure 门户的“主页”上，选择“资源组”。
3. 选择 Synapse Analytics 工作区的 dp203-xxxxxxx 资源组（不是受管理资源组），并确认它包含 Synapse 工作区、存储帐户和工作区的 Spark 池**。
4. 在资源组的“概述”页的顶部，选择“删除资源组”。
5. 输入 dp203-xxxxxxx 资源组名称以确认要删除该资源组，然后选择“删除” **。

    几分钟后，将删除 Azure Synapse 工作区资源组及其关联的托管工作区资源组。
