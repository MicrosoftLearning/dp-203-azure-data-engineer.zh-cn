---
lab:
  title: 在管道中使用 Apache Spark 笔记本
  ilt-use: Lab
---

# 在管道中使用 Apache Spark 笔记本

在本练习中，我们将创建一个 Azure Synapse Analytics 管道，其中包含用于运行 Apache Spark 笔记本的活动。

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

3. 请注意，可以通过拖动窗格顶部的分隔条来调整 Cloud Shell 的大小，或使用窗格右上角的“—”、“&#9723;”和“X”图标来最小化、最大化和关闭窗格 。 有关如何使用 Azure Cloud Shell 的详细信息，请参阅 [Azure Cloud Shell 文档](https://docs.microsoft.com/azure/cloud-shell/overview)。

4. 在 PowerShell 窗格中，输入以下命令以克隆此存储库：

    ```powershell
    rm -r dp-203 -f
    git clone https://github.com/MicrosoftLearning/dp-203-azure-data-engineer dp-203
    ```

5. 克隆存储库后，输入以下命令以更改为此练习的文件夹，然后运行其中包含的 setup.ps1 脚本：

    ```powershell
    cd dp-203/Allfiles/labs/11
    ./setup.ps1
    ```
    
6. 如果出现提示，请选择要使用的订阅（仅当有权访问多个 Azure 订阅时才会发生这种情况）。
7. 出现提示时，输入要为 Azure Synapse SQL 池设置的合适密码。

    > 注意：请务必记住此密码！

8. 等待脚本完成 - 此过程通常需要大约 10 分钟；但在某些情况下可能需要更长的时间。 等待时，请查看 Azure Synapse Analytics 文档中的 [Azure Synapse 管道](https://learn.microsoft.com/en-us/azure/data-factory/concepts-data-flow-performance-pipelines)一文。

## 以交互方式运行 Spark 笔记本

在使用笔记本自动执行数据转换过程之前，建议以交互方式运行笔记本，以便更好地了解稍后要自动执行的过程。

1. 脚本完成后，在 Azure 门户中转到其创建的 dp203-xxxxxxx 资源组，然后选择 Synapse 工作区。
2. 在 Synapse 工作区“概述”页的“打开 Synapse Studio”卡中，选择“打开”，以在新浏览器标签页中打开 Synapse Studio；如果出现提示，请进行登录  。
3. 在 Synapse Studio 左侧，使用“››”图标展开菜单，这将显示 Synapse Studio 中的不同页面。
4. 在“数据”页上，查看“已链接”选项卡并验证工作区是否包含 Azure Data Lake Storage Gen2 存储帐户的链接，该帐户的名称应类似于 synapsexxxxxxx (Primary - datalakexxxxxxx)********。
5. 展开存储帐户，验证它是否包含名为“files (primary)”的文件系统容器。
6. 选择文件容器，并注意它包含一个名为“data”的文件夹，其中包含要转换的数据文件****。
7. 打开“data”文件夹并查看其包含的 CSV 文件。 右键单击任一文件，然后选择“预览”以查看数据示例****。 完成后关闭预览窗口。
8. 在“Synapse Studio”的“开发”页上，展开“笔记本”并打开“Spark 转换”笔记本************。

    > **备注**：如果在运行脚本期间发现笔记本没有上传，则应从 GitHub Allfiles/labs/11/notebooks[](https://github.com/MicrosoftLearning/dp-203-azure-data-engineer/tree/master/Allfiles/labs/11/notebooks) 下载名为 Spark Transform.ipynb 的文件并将其上传到 Synapse。

9. 查看笔记本包含的代码，请注意该代码：
    - 设置变量以定义唯一的文件夹名称。
    - 从 /data 文件夹加载 CSV 销售订单数据****。
    - 通过将客户名称拆分为多个字段来转换数据。
    - 将转换后的数据以 Parquet 格式保存在名称唯一的文件夹中。
10. 在笔记本工具栏中，将笔记本附加到 spark*xxxxxxx* Spark 池，然后使用“&#9655; 全部运行”按钮运行笔记本中的所有代码单元格********。
  
    Spark 会话可能需要几分钟才能启动，随后代码单元格才能运行。

11. 运行所有笔记本单元格后，请记下保存转换后的数据的文件夹的名称。
12. 请切换到仍应打开的“files”选项卡并查看根“files”文件夹********。 如有必要，请在“更多”菜单中选择“刷新”以查看新文件夹********。 然后打开该文件夹，验证它是否包含 Parquet 文件。
13. 返回到根“files”文件夹，选择笔记本生成的唯一名称文件夹，然后在“新建 SQL 脚本”菜单中选择“选择前 100 行”************。
14. 在“选择前 100 行”窗格中，将文件类型设置为“Parquet 格式”并应用更改********。
15. 在打开的新 SQL 脚本窗格中，使用“&#9655; 运行”按钮运行 SQL 代码并验证它是否返回转换后的销售订单数据****。

## 在管道中运行笔记本

现在你已了解转换过程，可以通过将笔记本封装到管道中来自动执行转换过程。

### 创建参数单元格

1. 在“Synapse Studio”中，返回到包含笔记本的“Spark 转换”选项卡，并在工具栏右端的“...”菜单中选择“清除输出”************。
2. 选择包含要设置 folderName 变量的代码的第一个代码单元格****。
3. 在代码单元格右上角的弹出工具栏中的“...”菜单中选择“\[@] 切换参数单元格”********。 验证单元格右下角是否显示“parameters”一词****。
4. 使用工具栏中的“发布”按钮保存更改****。

### 创建管道

1. 在 Synapse Studio 中，选择“集成”页。 然后在“+”菜单中，选择“管道”以创建新的管道 。
2. 在新管道的“属性”窗格中，将其名称从“Pipeline1”更改为“转换销售数据”************。 然后使用“属性”窗格上方的“属性”按钮将其隐藏 。
3. 在“活动”窗格中，展开“Synapse”；然后将一个 Notebook 活动拖动到管道设计图面，如下所示************：

    ![包含 Notebook 活动的管道的屏幕截图。](images/notebook-pipeline.png)

4. 在 Notebook 活动的“常规”选项卡中，将其名称更改为“运行 Spark 转换”********。
5. 在 Notebook 活动的“设置”选项卡中设置以下属性****：
    - **Notebook**：选择“Spark 转换”笔记本****。
    - **基参数**：展开此部分并使用以下设置定义参数：
        - **名称**：folderName
        - **类型：** 字符串
        - **值**：选择“添加动态内容”，并将参数值设置为管道运行 ID 系统变量 (`@pipeline().RunId`)******
    - **Spark 池**：选择 spark*xxxxxxx* 池****。
    - **执行程序大小**：选择“小型(4 个 vCore、28GB 内存)”****。

    代码窗格应类似于：

    ![包含 Notebook 活动的管道的屏幕截图，其中显示了各项设置。](images/notebook-pipeline-settings.png)

### 发布并运行管道

1. 使用“全部发布”按钮发布管道（以及任何其他未保存的资产）。
2. 在管道设计器窗格顶部的“添加触发器”菜单中，选择“立即触发” 。 然后选择“确定”，确认要运行管道。

    注意：还可以创建触发器，在计划的时间或响应特定事件时运行管道。

3. 管道开始运行时，在“监视”页上，查看“管道运行”选项卡，并查看“转换销售数据”管道的状态************。
4. 选择“转换销售数据”管道以查看其详细信息，并留意“活动运行”窗格中的管道运行 ID********。

    管道运行可能需要 5 分钟或更长时间才能完成。 可以使用工具栏上的“&#8635;刷新”按钮检查管道状态。

5. 管道运行成功后，在“数据”页上浏览到“files”存储容器，并验证是否已创建名称与管道运行 ID 对应的新文件夹，以及该文件夹是否包含已转换的销售数据的 Parquet 文件********。
   
## 删除 Azure 资源

你已完成对 Azure Synapse Analytics 的探索，现在应删除已创建的资源，以避免产生不必要的 Azure 成本。

1. 关闭 Synapse Studio 浏览器选项卡并返回到 Azure 门户。
2. 在 Azure 门户的“主页”上，选择“资源组”。
3. 选择 Synapse Analytics 工作区的 dp203-xxxxxxx 资源组（不是受管理资源组），并确认它包含 Synapse 工作区、存储帐户和工作区的 Spark 池**。
4. 在资源组的“概述”页的顶部，选择“删除资源组”。
5. 输入 dp203-xxxxxxx 资源组名称以确认要删除该资源组，然后选择“删除” 。

    几分钟后，将删除 Azure Synapse 工作区资源组及其关联的托管工作区资源组。
