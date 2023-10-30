---
lab:
  title: 使用 Azure Synapse Link for SQL
  ilt-use: Suggested demo
---

# 使用 Azure Synapse Link for SQL

Azure Synapse Link for SQL 使你能够自动将 SQL Server 或 Azure SQL 数据库中的事务数据库与 Azure Synapse Analytics 中的专用 SQL 池同步。 通过此同步，便能够在 Synapse Analytics 中执行低延迟分析工作负载，而不会在源操作数据库中产生查询开销。

完成此练习大约需要 35 分钟。

## 准备工作

需要一个你在其中具有管理级权限的 [Azure 订阅](https://azure.microsoft.com/free)。

## 预配 Azure 资源

在本练习中，你将从 Azure SQL 数据库资源将数据同步到 Azure Synapse Analytics 工作区。 首先，你将使用脚本在 Azure 订阅中预配这些资源。

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
    cd dp-203/Allfiles/labs/15
    ./setup.ps1
    ```

6. 如果出现提示，请选择要使用的订阅（仅当有权访问多个 Azure 订阅时才会发生这种情况）。
7. 出现提示时，为 Azure SQL 数据库输入合适的密码。

    > 注意：请务必记住此密码！

8. 等待脚本完成 - 这通常需要大约 15 分钟，但在某些情况下可能需要更长的时间。 等待时，请查看 Azure Synapse Analytics 文档中的[什么是 Azure Synapse Link for SQL？](https://docs.microsoft.com/azure/synapse-analytics/synapse-link/sql-synapse-link-overview)一文。

## 配置 Azure SQL 数据库

在为 Azure SQL 数据库设置 Azure Synapse Link 之前，必须确保 Azure SQL 数据库服务器中已应用所需配置设置。

1. 在 [Azure 门户](https://portal.azure.com)中，浏览到由安装脚本创建的 dp203-xxxxxxx 资源组，然后选择 sqldbxxxxxxxx Azure SQL 服务器 ****。

    > 注意：请留意不要将 Azure SQL 服务器资源 (sqldbxxxxxxxx) 与 Azure Synapse Analytics 专用 SQL 池 (sqlxxxxxxxx) 混为一谈 ****** ******。

2. 在 Azure SQL Server 资源的页面左侧窗格中，在“安全”部分（靠近底部）选择“标识” 。 然后在“系统分配的托管标识”下，将“状态”选项设置为“开启”  。 然后使用“&#128427;保存”图标，保存配置更改。

    ![Azure 门户中 Azure SQL 服务器“标识”页的屏幕截图。](./images/sqldb-identity.png)

3. 在左侧窗格中的“安全”部分，选择“网络” 。 然后，在“防火墙规则”下，选择“允许 Azure 服务和资源访问此服务器”的例外情况 。

4. 使用“&#65291; 添加防火墙规则”按钮可使用以下设置添加新防火墙规则：

    | 规则名称 | 起始 IP | 结束 IP |
    | -- | -- | -- |
    | AllClients | 0.0.0.0 | 255.255.255.255 |

    > 注意：此规则会允许从任何连接 Internet 的计算机访问服务器。 我们启用此规则是为了简化练习，但在生产场景中，应将访问权限限制给需要使用数据库的网络地址。

5. 使用“保存”按钮保存配置更改：

    ![Azure 门户中 Azure SQL 服务器“网络”页的屏幕截图。](./images/sqldb-network.png)

## 浏览事务数据库

Azure SQL 服务器托管名为 AdventureWorksLT 的示例数据库。 此数据库表示用于操作应用程序数据的事务数据库。

1. 在 Azure SQL 服务器的概述页面底部，选择 AdventureWorksLT 数据库 ：
2. 在 AdventureWorksLT 数据库页中，选择“查询编辑器”选项卡，然后通过以下凭据使用 SQL Server 身份验证登录 ：
    - 登录名：SQLUser
    - 密码：运行安装脚本时指定的密码。
3. 当查询编辑器打开时，展开“表”节点并查看数据库中的表列表。 请注意，其中包括 SalesLT 架构中的表（例如 SalesLT.Customer） 。

## 配置 Azure Synapse Link

现在已准备就绪，可在 Synapse Analytics 工作区中配置 Azure Synapse Link for SQL。

### 启动专用 SQL 池

1. 在 Azure 门户中，关闭 Azure SQL 数据库的查询编辑器（放弃所有更改），并返回到 dp203-xxxxxxx 资源组的页面**。
2. 打开 synapsexxxxxxx Synapse 工作区，并在其概述页面上的“打开 Synapse Studio”卡中选择“打开”，以在新浏览器标签页中打开 Synapse Studio；如果出现提示，请登录   **。
3. 在 Synapse Studio 左侧，使用 &rsaquo;&rsaquo; 图标展开菜单，这将显示 Synapse Studio 中的不同页面。
4. 在“管理”页的“SQL 池”选项卡上，选择 sqlxxxxxxx 专用 SQL 池所在的行，并使用其 &#9655; 图标进行启动；在出现系统提示时确认进行恢复   **。
5. 等待 SQL 池恢复。 这可能需要几分钟的时间。 可以使用“&#8635; 刷新”按钮定期检查其状态。 状态将在准备就绪时显示为“联机”。

### 创建目标架构

1. 在 Synapse Studio 中，在“数据”页的“工作区”选项卡上，展开 SQL 数据库并选择 sqlxxxxxxx 数据库   **。
2. 在 sqlxxxxxxx 数据库的“...”菜单中，选择“新建 SQL 脚本” > “空脚本”   **。
3. 在“SQL 脚本 1”窗格中，输入以下 SQL 代码，并使用 “&#9655; 运行”按钮运行代码 。

    ```sql
    CREATE SCHEMA SalesLT;
    GO
    ```

4. 等待查询成功完成。 此代码在数据库中为专用 SQL 池创建名为 SalesLT 的架构，使你能够从 Azure SQL 数据库同步采用该名称的架构中的表。

### 创建链接连接

1. 在 Synapse Studio 的“集成”页上的“&#65291;”下拉菜单中，选择“链接连接”  。 然后创建一个新的链接函数，设置如下：
    - 源类型：Azure SQL 数据库
    - 源链接服务：添加设置如下的新链接服务（将打开新选项卡）：
        - 名称：SqlAdventureWorksLT
        - 说明：与 AdventureWorksLT 数据库的连接
        - 通过集成运行时连接：AutoResolveIntegrationRuntime
        - 连接字符串：已选择
        - 来自 Azure 订阅：已选择
        - Azure 订阅：选择自己的 Azure订阅
        - 服务器名称：选择 sqldbxxxxxxx Azure SQL服务器
        - 数据库名称：AdventureWorksLT
        - 身份验证类型：SQL 身份验证
        - 用户名：SQLUser
        - 密码：运行安装脚本时设置的密码。

        在继续之前，请使用“测试连接”选项确保连接设置正确！之后，单击“创建” 。

    - 源表：选择以下表：
        - SalesLT.Customer
        - SalesLT.Product
        - SalesLT.SalesOrderDetail
        - SalesLT.SalesOrderHeader

        继续配置以下设置：

    > 注意：由于使用自定义数据类型或源表中的数据与聚集列存储索引的默认结构类型不兼容，某些目标表会显示错误。

    - 目标池：选择 sqlxxxxxxx 专用 SQL 池

        继续配置以下设置：

    - 链接连接名称：sql-adventureworkslt-conn
    - 核心计数：4（+ 4 个驱动程序核心）

2. 在创建的“sql-adventureworkslt-conn”页中，查看已创建的表映射。 可以使用“属性”按钮（类似于 &#128463;<sub>*</sub>）隐藏“属性”面板，这样更易于显示所有内容  。 

3. 修改表映射中的这些结构类型，如下所示：

    | 源表 | 目标表 | 分布类型 | 分布列 | 结构类型 |
    |--|--|--|--|--|
    | SalesLT.Customer &#8594; | \[SalesLT] . \[Customer] | 轮循机制 | - | 聚集列存储索引 |
    | SalesLT.Product &#8594; | \[SalesLT] . \[Product] | 轮循机制 | - | 堆 |
    | SalesLT.SalesOrderDetail &#8594; | \[SalesLT] . \[SalesOrderDetail] | 轮循机制 | - | 聚集列存储索引 |
    | SalesLT.SalesOrderHeader &#8594; | \[SalesLT] . \[SalesOrderHeader] | 轮循机制 | - | 堆 |

4. 在创建的“sql-adventureworkslt-conn”页顶部，使用“&#9655; 开始”按钮开始同步 。 出现提示时，选择“确定”以发布并启动链接连接。
5. 启动连接后，在“监视”页上，查看“链接连接”选项卡并选择“sql-adventureworkslt-conn”连接  。 可以使用“&#8635; 刷新”按钮定期更新其状态。 完成初始快照复制过程并开始复制可能需要数分钟时间，之后，源数据库表中的所有更改都将在同步的表中自动重播。

### 查看复制的数据

1. 在表状态更改为“正在运行”后，选择“数据”页并使用右上角的“&#8635;”图标，刷新视图  。
2. 在“工作区”选项卡上，展开 SQL 数据库、sqlxxxxxxx 数据库及其“表”文件夹，以查看复制的表   **。
3. 在 sqlxxxxxxx 数据库的“...”菜单中，选择“新建 SQL 脚本” > “空脚本”   **。 然后在新脚本页中，输入以下 SQL 代码：

    ```sql
    SELECT  oh.SalesOrderID, oh.OrderDate,
            p.ProductNumber, p.Color, p.Size,
            c.EmailAddress AS CustomerEmail,
            od.OrderQty, od.UnitPrice
    FROM SalesLT.SalesOrderHeader AS oh
    JOIN SalesLT.SalesOrderDetail AS od 
        ON oh.SalesOrderID = od.SalesOrderID
    JOIN  SalesLT.Product AS p 
        ON od.ProductID = p.ProductID
    JOIN SalesLT.Customer as c
        ON oh.CustomerID = c.CustomerID
    ORDER BY oh.SalesOrderID;
    ```

4. 使用“&#9655; 运行”按钮运行脚本并查看结果。 查询针对专用 SQL 池（而不是源数据库）中的复制表运行，使你能够在不影响业务应用程序的情况下运行分析查询。
5. 完成后，在“管理”页上暂停 sql*xxxxxxx* 专用 SQL 池 。

## 删除 Azure 资源

你已完成对 Azure Synapse Analytics 的探索，现在应删除已创建的资源，以避免产生不必要的 Azure 成本。

1. 关闭 Synapse Studio 浏览器选项卡并返回到 Azure 门户。
2. 在 Azure 门户的“主页”上，选择“资源组”。
3. 选择在本练习开始时由安装脚本创建的 dp203-xxxxxxx 资源组**。
4. 在资源组的“概述”页的顶部，选择“删除资源组”。
5. 输入 dp203-xxxxxxx 资源组名称以确认要删除该资源组，然后选择“删除” **。

    几分钟后，资源组及其包含的资源都将删除。
