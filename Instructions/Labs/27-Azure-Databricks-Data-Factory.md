---
lab:
  title: 使用 Azure 数据工厂自动化 Azure Databricks 笔记本
  ilt-use: Suggested demo
---

# 使用 Azure 数据工厂自动化 Azure Databricks 笔记本

可以使用 Azure Databricks 中的笔记本来执行数据工程任务，例如处理数据文件以及将数据加载到表中。 如果需要将这些任务作为数据工程管道的一部分进行协调，可以使用 Azure 数据工厂。

完成此练习大约需要 40 分钟。

## 准备工作

需要一个你在其中具有管理级权限的 [Azure 订阅](https://azure.microsoft.com/free)。

## 预配 Azure 资源

在本练习中，你将使用脚本在 Azure 订阅中预配新的 Azure Databricks 工作区和 Azure 数据工厂资源。

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
    cd dp-203/Allfiles/labs/27
    ./setup.ps1
    ```

6. 如果出现提示，请选择要使用的订阅（仅当有权访问多个 Azure 订阅时才会发生这种情况）。

7. 等待脚本完成 - 这通常需要大约 5 分钟，但在某些情况下可能需要更长的时间。 在等待时，请查看[什么是 Azure 数据工厂？](https://docs.microsoft.com/azure/data-factory/introduction)。
8. 脚本完成后，关闭 cloud shell 窗格，浏览到由脚本创建的 dp203-xxxxxxx 资源组，验证它是否包含 Azure Databricks 工作区和 Azure 数据工厂 (V2) 资源（可能需要刷新资源组视图）。

## 导入笔记本

可以在 Azure Databricks 工作区中创建笔记本，运行用一系列编程语言编写的代码。 在本练习中，导入一个包含一些 Python 代码的现有笔记本。

1. 在 Azure 门户中，浏览到由运行的脚本创建的 dp203-xxxxxxx 资源组。
2. 选择 databricksxxxxxxx Azure Databricks 服务资源。
3. 在 databricksxxxxxxx 的“概述”页中，使用“启动工作区”按钮在新的浏览器标签页中打开 Azure Databricks 工作区；并在出现提示时登录。
4. 如果显示“当前数据项目是什么？”消息，请选择“完成”将其关闭 。 然后查看 Azure Databricks 工作区门户，注意左侧边栏包含可执行的各种任务的图标。

    >提示：使用 Databricks 工作区门户时，可能会显示各种提示和通知。 消除这些内容，并按照提供的说明完成本练习中的任务。

1. 在左侧边栏中，选择“工作区”。 然后选择“&#8962; 主页”文件夹。
1. 在页面顶部的用户名旁边的 &#8942; 菜单中，选择“导入” 。 然后在“导入”对话框中，选择“URL”并从 `https://github.com/MicrosoftLearning/dp-203-azure-data-engineer/raw/master/Allfiles/labs/27/Process-Data.ipynb` 导入笔记本 
1. 查看笔记本的内容，包括一些 Python 代码单元格，以便：
    - 如果已传递名为 folder 的参数，则检索该参数（否则请使用默认值 data）。
    - 从 GitHub 下载数据，并将其保存在 Databricks 文件系统 (DBFS) 的指定文件夹中。
    - 退出笔记本，返回将数据保存为输出的路径

    > 提示：笔记本几乎可以包含所需的任何数据处理逻辑。 此简单示例旨在展示关键原则。

## 启用 Azure Databricks 与 Azure 数据工厂的集成

若要从 Azure 数据工厂管道使用 Azure Databricks，需要在 Azure 数据工厂中创建一个链接服务，以便能够访问 Azure Databricks 工作区。

### 生成访问令牌

1. 在 Azure Databricks 门户的右上方菜单栏中，选择用户名，然后从下拉列表中选择“用户设置”。
1. 在“用户设置”页中，选择“开发人员” 。 然后在“访问令牌”旁边，选择“管理” 。
1. 选择“生成新令牌”，并使用注释“数据工厂”和空白生存期生成新令牌（这样令牌不会过期）。 在选择“完成”之前，请注意在显示令牌时复制令牌<u></u>。
1. 将复制的令牌粘贴到文本文件中，以便稍后在本练习中使用。

### 在 Azure 数据工厂中创建链接服务

1. 返回到 Azure 门户，在 dp203-xxxxxxx 资源组中，选择 Azure 数据工厂资源 adfxxxxxxx 。
2. 在“概述”页上，选择“启动工作室”以打开 Azure 数据工厂工作室 。 根据提示登录。
3. 在 Azure 数据工厂工作室中，使用 >> 图标展开左侧的导航窗格。 然后选择“管理”页。
4. 在“管理”页的“链接服务”选项卡中，选择“+ 新建”添加新的链接服务  。
5. 在“新建链接服务”窗格中，选择顶部的“计算”选项卡 。 然后选择“Azure Databricks”。
6. 继续，使用以下设置创建链接服务：
    - 名称：AzureDatabricks
    - 说明：Azure Databricks 工作区
    - 通过集成运行时连接：AutoResolveInegrationRuntime
    - 帐户选择方式：从 Azure 订阅
    - Azure 订阅：选择你的订阅
    - Databricks 工作区：选择你的 databricksxxxxxxx 工作区**
    - 选择群集：新建作业群集
    - Databrick 工作区 URL：自动设置为 Databricks 工作区 URL
    - 身份验证类型：访问令牌
    - 访问令牌：粘贴访问令牌
    - 群集版本：12.2 LTS（Scala 2.12、Spark 3.2.2）
    - 群集节点类型：Standard_DS3_v2
    - Python 版本：3
    - 辅助角色选项：已修复
    - 辅助角色：1

## 使用管道运行 Azure Databricks 笔记本

创建链接服务后，可以在管道中使用它来运行之前查看的笔记本。

### 创建管道

1. 在 Azure 数据工厂工作室的导航窗格中，选择“创作”。
2. 在“创作”页上的“工厂资源”窗格中，使用 + 图标添加“管道”   。
3. 在新管道的“属性”窗格中，将其名称更改为“使用 Databricks 处理数据” 。 然后使用工具栏右侧的“属性”按钮（看起来类似于“&#128463;”<sub>*</sub>）隐藏“属性”窗格  。
4. 在“活动”窗格中展开“Databricks”，将“笔记本”活动拖动到管道设计器图面  。
5. 选择新的“Notebook1”活动后，在底部窗格中设置以下属性：
    - 常规：
        - 名称：处理数据
    - Azure Databricks：
        - Databricks 链接服务：选择之前创建的 AzureDatabricks 链接服务**
    - 设置：
        - 笔记本路径：浏览到 Users/your_user_name 文件夹，然后选择“Process-Data”笔记本* *
        - 基参数：添加名为“folder”的新参数，其值为“product_data”* *
6. 使用管道设计器图面上方的“验证”按钮验证管道。 然后使用“全部发布”按钮发布（保存）。

### 运行管道

1. 在管道设计器图面上方，选择“添加触发器”，然后选择“立即触发” 。
2. 在“管道运行”窗格中，选择“确定”运行管道 。
3. 在左侧导航窗格中，选择“监视”，并在“管道运行”选项卡上观察“使用 Databricks 处理数据”管道。运行可能需要一段时间，因为它会动态创建 Spark 群集并运行笔记本  。 可以使用“管道运行”页上的“&#8635;刷新”按钮刷新状态 。

    > 注意：如果管道运行失败，则订阅在预配 Azure Databricks 工作区的区域中的配额可能不足，无法创建作业群集。 请参阅 [CPU 内核限制阻止创建群集](https://docs.microsoft.com/azure/databricks/kb/clusters/azure-core-limit)，了解详细信息。 如果发生这种情况，可以尝试删除工作区，并在其他区域创建新工作区。 可以将区域指定为设置脚本的参数，如下所示：`./setup.ps1 eastus`

4. 运行成功后，选择其名称，查看运行详细信息。 然后，在“使用 Databricks 处理数据”页的“活动运行”部分，选择“处理数据”活动，并使用其“输出”图标查看该活动的输出 JSON，应如下所示  ：
    ```json
    {
        "runPageUrl": "https://adb-..../run/...",
        "runOutput": "dbfs:/product_data/products.csv",
        "effectiveIntegrationRuntime": "AutoResolveIntegrationRuntime (East US)",
        "executionDuration": 61,
        "durationInQueue": {
            "integrationRuntimeQueue": 0
        },
        "billingReference": {
            "activityType": "ExternalActivity",
            "billableDuration": [
                {
                    "meterType": "AzureIR",
                    "duration": 0.03333333333333333,
                    "unit": "Hours"
                }
            ]
        }
    }
    ```

5. 请注意 runOutput 值，该值是笔记本保存数据的路径变量。

## 删除 Azure Databricks 资源

你已完成对 Azure 数据工厂与 Azure Databricks 的集成的探索，现在必须删除已创建的资源，以避免产生不必要的 Azure 成本并释放订阅中的容量。

1. 关闭 Azure Databricks 工作区和 Azure 数据工厂工作室浏览器标签页，然后返回到 Azure 门户。
2. 在 Azure 门户的“主页”上，选择“资源组”。
3. 选择包含 Azure Databricks 和 Azure 数据工厂工作区的 dp203-xxxxxxx 资源组（而不是托管资源组）。
4. 在资源组的“概述”页的顶部，选择“删除资源组”。
5. 输入资源组名称以确认要删除该资源组，然后选择“删除”。

    几分钟后，将删除资源组及其关联的托管工作区资源组。
