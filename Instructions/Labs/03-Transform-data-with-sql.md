---
lab:
  title: 使用无服务器 SQL 池转换数据
  ilt-use: Lab
---

# 使用无服务器 SQL 池转换文件

数据分析师通常使用 SQL 来查询数据以进行分析和报告。 数据工程师还可以使用 SQL 来操作和转换数据；通常作为数据引入管道或提取、转换和加载 (ETL) 过程的一部分。

在本练习中，你将使用 Azure Synapse Analytics 中的无服务器 SQL 池来转换文件中的数据。

完成此练习大约需要 30 分钟。

## 准备工作

需要一个你在其中具有管理级权限的 [Azure 订阅](https://azure.microsoft.com/free)。

## 预配 Azure Synapse Analytics 工作区

需要一个 Azure Synapse Analytics 工作区才能访问 Data Lake Storage。 可以使用内置的无服务器 SQL 池查询 Data Lake 中的文件。

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
    cd dp-203/Allfiles/labs/03
    ./setup.ps1
    ```

6. 如果出现提示，请选择要使用的订阅（仅当有权访问多个 Azure 订阅时才会发生这种情况）。
7. 出现提示时，输入要为 Azure Synapse SQL 池设置的合适密码。

    > 注意：请务必记住此密码！

8. 等待脚本完成 - 此过程通常需要大约 10 分钟；但在某些情况下可能需要更长的时间。 等待时，请查看 Azure Synapse Analytics 文档中的 [Synapse SQL 的 CETAS](https://docs.microsoft.com/azure/synapse-analytics/sql/develop-tables-cetas) 一文。

## 查询文件中的数据

该脚本预配 Azure Synapse Analytics 工作区和 Azure 存储帐户来托管 Data Lake，然后将一些数据文件上传到 Data Lake。

### 查看 Data Lake 中的文件

1. 脚本完成后，在 Azure 门户中转到创建的 dp203-*xxxxxxx* 资源组，然后选择 Synapse 工作区。
2. 在 Synapse 工作区“概述”页的“打开 Synapse Studio”卡中，选择“打开”，以在新浏览器标签页中打开 Synapse Studio；如果出现提示，请进行登录  。
3. 在 Synapse Studio 左侧，使用 &rsaquo;&rsaquo; 图标展开菜单，这将显示 Synapse Studio 中用于管理资源和执行数据分析任务的不同页面。
4. 在“数据”页上，查看“已链接”选项卡并验证工作区是否包含 Azure Data Lake Storage Gen2 存储帐户的链接，该帐户的名称应类似于 synapsexxxxxxx* (Primary - datalake xxxxxxx*) ** 。
5. 展开存储帐户，验证它是否包含名为 files 的文件系统容器。
6. 选择“files”容器，并注意它包含名为 sales 的文件夹 。 此文件夹包含要查询的数据文件。
7. 打开 sales 文件夹及其包含的 csv 文件夹，注意此文件夹中包含具有三年销售数据的 .csv 文件 。
8. 右键单击任一文件，然后选择“预览”以查看它所包含的数据。 请注意，文件包含标题行。
9. 关闭预览，然后使用“&#8593;”按钮导航回 sales 文件夹 。

### 使用 SQL 查询 CSV 文件

1. 选择 csv 文件夹，然后在工具栏上的“新建 SQL 脚本”列表中，选中“选择前 100 行”  。
2. 在“文件类型”列表中，选择“文本格式”，然后应用设置以打开查询文件夹中数据的新 SQL 脚本 。
3. 在创建的“SQL 脚本 1”的“属性”窗格中，将名称更改为“Query Sales CSV 文件”，并更改结果设置以显示“所有行”   。 然后在工具栏中，选择“发布”以保存脚本并使用工具栏右侧的“属性”按钮（类似于 &#128463;<sub></sub>）以隐藏“属性”窗格  ***** 。
4. 查看已经生成的 SQL 代码，代码应该类似于：

    ```SQL
    -- This is auto-generated code
    SELECT
        TOP 100 *
    FROM
        OPENROWSET(
            BULK 'https://datalakexxxxxxx.dfs.core.windows.net/files/sales/csv/**',
            FORMAT = 'CSV',
            PARSER_VERSION='2.0'
        ) AS [result]
    ```

    此代码使用 OPENROWSET 从 sales 文件夹中的 CSV 文件读取数据，并检索前 100 行数据。

5. 在本例中，数据文件在第一行中包含列名；因此，请修改查询以将 `HEADER_ROW = TRUE` 参数添加到 `WITH` 子句，如下所示（不要忘记在上一个参数后面添加逗号）：

    ```SQL
    SELECT
        TOP 100 *
    FROM
        OPENROWSET(
            BULK 'https://datalakexxxxxxx.dfs.core.windows.net/files/sales/csv/**',
            FORMAT = 'CSV',
            PARSER_VERSION='2.0',
            HEADER_ROW = TRUE
        ) AS [result]
    ```

6. 在“连接到”列表中，确保已选中“内置”，这表示通过工作区创建的内置 SQL 池。 然后在工具栏上，使用“&#9655; 运行”按钮运行 SQL 代码，并查看结果，结果应如下所示：

    | SalesOrderNumber | SalesOrderLineNumber | OrderDate | CustomerName | EmailAddress | 项 | 数量 | 单价 | TaxAmount |
    | -- | -- | -- | -- | -- | -- | -- | -- | -- |
    | SO43701 | 1 | 2019-07-01 | Christy Zhu | christy12@adventure-works.com |Mountain-100 Silver, 44 | 1 | 3399.99 | 271.9992 |
    | ... | ... | ... | ... | ... | ... | ... | ... | ... |

7. 将更改发布到脚本，然后关闭脚本窗格。

## 使用 CREATE EXTERAL TABLE AS SELECT (CETAS) 语句转换数据

使用 SQL 转换文件中的数据并将结果保存在另一个文件中的一种简单方法是使用 CREATE EXTERNAL TABLE AS SELECT (CETAS) 语句。 此语句基于查询的请求创建表，但表的数据作为文件存储在数据湖中。 然后，可以通过外部表查询转换后的数据，或直接在文件系统中进行访问（例如，为包含到下游过程中，可以将转换后的数据加载到数据仓库）。

### 创建外部数据源和文件格式

通过在数据库中定义外部数据源，可以使用它来引用要为外部表存储文件的数据湖位置。 使用外部文件格式可以定义这些文件的格式，例如 Parquet 或 CSV。 若要使用这些对象来处理外部表，需要在默认 master 数据库以外的数据库中创建它们。

1. 在 Synapse Studio 的“开发”页上的“+”菜单中，选择“SQL 脚本”  。
2. 在新脚本窗格中，添加以下代码（将 datalakexxxxxxx 替换为 Data Lake Storage 帐户的名称）以创建新数据库并向其添加外部数据源。

    ```sql
    -- Database for sales data
    CREATE DATABASE Sales
      COLLATE Latin1_General_100_BIN2_UTF8;
    GO;
    
    Use Sales;
    GO;
    
    -- External data is in the Files container in the data lake
    CREATE EXTERNAL DATA SOURCE sales_data WITH (
        LOCATION = 'https://datalakexxxxxxx.dfs.core.windows.net/files/'
    );
    GO;
    
    -- Format for table files
    CREATE EXTERNAL FILE FORMAT ParquetFormat
        WITH (
                FORMAT_TYPE = PARQUET,
                DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
            );
    GO;
    ```

3. 修改脚本属性以将其名称更改为“创建销售数据库”，然后发布。
4. 确保脚本已连接到内置 SQL 池和 master 数据库，然后运行 。
5. 切换回“数据”页，并使用 Synapse Studio 右上角的“&#8635;”按钮刷新页面 。 然后，在“数据”窗格中查看“工作区”选项卡，此时会显示“SQL 数据库”列表  。 展开此列表以验证是否已创建 Sales 数据库。
6. 展开 Sales 数据库、其 External Resources 文件夹以及其下的 External data sources 文件夹，以查看所创建的 sales_data 外部数据源   。

### 创建外部表

1. 在 Synapse Studio 的“开发”页上的“+”菜单中，选择“SQL 脚本”  。
2. 在新的脚本窗格中，添加以下代码以使用外部数据源从 CSV 销售文件中检索和聚合数据 - 请注意 BULK 路径与定义数据源的文件夹位置相关：

    ```sql
    USE Sales;
    GO;
    
    SELECT Item AS Product,
           SUM(Quantity) AS ItemsSold,
           ROUND(SUM(UnitPrice) - SUM(TaxAmount), 2) AS NetRevenue
    FROM
        OPENROWSET(
            BULK 'sales/csv/*.csv',
            DATA_SOURCE = 'sales_data',
            FORMAT = 'CSV',
            PARSER_VERSION = '2.0',
            HEADER_ROW = TRUE
        ) AS orders
    GROUP BY Item;
    ```

3. 运行该脚本。 结果应如下所示：

    | 产品 | ItemsSold | NetRevenue |
    | -- | -- | -- |
    | AWC Logo Cap | 1063 | 8791.86 |
    | ... | ... | ... |

4. 修改 SQL 代码以将查询结果保存在外部表中，如下所示：

    ```sql
    CREATE EXTERNAL TABLE ProductSalesTotals
        WITH (
            LOCATION = 'sales/productsales/',
            DATA_SOURCE = sales_data,
            FILE_FORMAT = ParquetFormat
        )
    AS
    SELECT Item AS Product,
        SUM(Quantity) AS ItemsSold,
        ROUND(SUM(UnitPrice) - SUM(TaxAmount), 2) AS NetRevenue
    FROM
        OPENROWSET(
            BULK 'sales/csv/*.csv',
            DATA_SOURCE = 'sales_data',
            FORMAT = 'CSV',
            PARSER_VERSION = '2.0',
            HEADER_ROW = TRUE
        ) AS orders
    GROUP BY Item;
    ```

5. 运行该脚本。 此次操作不会产生输出，但代码应已基于查询结果创建了一个外部表。
6. 将脚本命名为“Create ProductSalesTotals 表”并发布该脚本。
7. 在“数据”页上的“工作区”选项卡中，查看 Sales SQL 数据库的“External tables”文件夹的内容，以验证是否已创建名为 ProductSalesTotals 的新表    。
8. 在 ProductSalesTotals 表的“...”菜单中，选择“新建 SQL 脚本” > “选择前 100 行”   。 然后运行生成的脚本并验证它是否返回汇总的产品销售数据。
9. 在包含数据湖文件系统的“文件”选项卡上，查看 sales 文件夹的内容（必要时刷新视图）并验证是否已创建新的 productsales 文件夹  。
10. 在 productsales 文件夹中，查看是否已创建一个或多个名称类似于 ABC123DE----.parquet 的文件。 这些文件包含汇总的产品销售数据。 若要证明这一点，可以选择其中一个文件，并使用“新建 SQL 脚本” > “选择前 100 行”菜单直接进行查询 。

## 在存储过程中封装数据转换

如果需要频繁转换数据，可以使用存储过程来封装 CETAS 语句。

1. 在 Synapse Studio 的“开发”页上的“+”菜单中，选择“SQL 脚本”  。
2. 在新的脚本窗格中，添加以下代码以在 Sales 数据库中创建一个存储过程，该存储过程按年份汇总销售额并将结果保存在外部表中：

    ```sql
    USE Sales;
    GO;
    CREATE PROCEDURE sp_GetYearlySales
    AS
    BEGIN
        -- drop existing table
        IF EXISTS (
                SELECT * FROM sys.external_tables
                WHERE name = 'YearlySalesTotals'
            )
            DROP EXTERNAL TABLE YearlySalesTotals
        -- create external table
        CREATE EXTERNAL TABLE YearlySalesTotals
        WITH (
                LOCATION = 'sales/yearlysales/',
                DATA_SOURCE = sales_data,
                FILE_FORMAT = ParquetFormat
            )
        AS
        SELECT YEAR(OrderDate) AS CalendarYear,
                SUM(Quantity) AS ItemsSold,
                ROUND(SUM(UnitPrice) - SUM(TaxAmount), 2) AS NetRevenue
        FROM
            OPENROWSET(
                BULK 'sales/csv/*.csv',
                DATA_SOURCE = 'sales_data',
                FORMAT = 'CSV',
                PARSER_VERSION = '2.0',
                HEADER_ROW = TRUE
            ) AS orders
        GROUP BY YEAR(OrderDate)
    END
    ```

3. 运行该脚本以创建存储过程。
4. 在刚刚运行的代码下，添加以下代码以调用存储过程：

    ```sql
    EXEC sp_GetYearlySales;
    ```

5. 仅选择刚刚添加的 `EXEC sp_GetYearlySales;` 语句，并使用“&#9655; 运行”按钮运行它。
6. 在包含数据湖文件系统的“文件”选项卡上，查看 sales 文件夹的内容（必要时刷新视图）并验证是否已创建新的 yearlysales 文件夹  。
7. 在 yearlysales 文件夹中，查看是否已创建包含汇总的年度销售数据的 parquet 文件。
8. 切换回 SQL 脚本并重新运行 `EXEC sp_GetYearlySales;` 语句，并查看是否出现错误。

    即使脚本删除了外部表，也不会删除包含数据的文件夹。 若要重新运行存储过程（例如，在计划数据转换管道中），必须删除旧数据。

9. 切换回“文件”选项卡，并查看 sales 文件夹 。 然后选择“yearlysales”文件夹并将其删除。
10. 切换回 SQL 脚本并重新运行 `EXEC sp_GetYearlySales;` 语句。 此次操作执行成功，并会生成一个新的数据文件。

## 删除 Azure 资源

你已完成对 Azure Synapse Analytics 的探索，现在应删除已创建的资源，以避免产生不必要的 Azure 成本。

1. 关闭 Synapse Studio 浏览器选项卡并返回到 Azure 门户。
2. 在 Azure 门户的“主页”上，选择“资源组”。
3. 选择 Synapse Analytics 工作区的 dp203-*xxxxxxx* 资源组（不是受管理资源组），并确认是否它包含 Synapse 工作区和你的工作区的存储帐户。
4. 在资源组的“概述”页的顶部，选择“删除资源组”。
5. 输入 dp203-*xxxxxxx* 资源组名称以确认要删除该资源组，然后选择“删除” 。

    几分钟后，将删除 Azure Synapse 工作区资源组及其关联的托管工作区资源组。
