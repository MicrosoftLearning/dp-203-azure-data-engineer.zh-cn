---
lab:
  title: 使用无服务器 SQL 池查询文件
  ilt-use: Lab
---

# 使用无服务器 SQL 池查询文件

SQL 可能是世界上处理数据最常用的语言。 大多数数据分析师都擅长使用 SQL 查询来检索、筛选和聚合数据 - 这在关系数据库中最常见。 随着组织越来越多地利用可缩放的文件存储来创建 Data Lake，SQL 通常仍是查询数据的首选选项。 Azure Synapse Analytics 提供了无服务器 SQL 池，使你能够将 SQL 查询引擎与数据存储分离，并针对通用文件格式（如分隔文本和 Parquet）的数据文件运行查询。

完成本实验室大约需要 40 分钟。

## 开始之前

需要一个你在其中具有管理级权限的 [Azure 订阅](https://azure.microsoft.com/free)。

## 预配 Azure Synapse Analytics 工作区

需要一个 Azure Synapse Analytics 工作区才能访问 Data Lake Storage。 可以使用内置的无服务器 SQL 池查询 Data Lake 中的文件。

在本练习中，你将组合使用 PowerShell 脚本和 ARM 模板来预配 Azure Synapse Analytics 工作区。

1. 登录到 Azure 门户，地址为 [](https://portal.azure.com)。
2. 使用页面顶部搜索栏右侧的 [\>_] 按钮在 Azure 门户中创建新的 Cloud Shell，在出现提示时选择“PowerShell”环境并创建存储。 Cloud Shell 在 Azure 门户底部的窗格中提供命令行界面，如下所示：

    ![具有 Cloud Shell 窗格的 Azure 门户](./images/cloud-shell.png)

    > 注意：如果以前创建了使用 Bash 环境的 Cloud shell，请使用 Cloud Shell 窗格左上角的下拉菜单将其更改为“PowerShell”。

3. 请注意，可以通过拖动窗格顶部的分隔条或使用窗格右上角的 &#8212;、&#9723; 或 X 图标来调整 Cloud Shell 的大小，以最小化、最大化和关闭窗格  。 有关如何使用 Azure Cloud Shell 的详细信息，请参阅 [Azure Cloud Shell 文档](https://docs.microsoft.com/azure/cloud-shell/overview)。

4. 在 PowerShell 窗格中，手动输入以下命令以克隆此存储库：

    ```
    rm -r dp203 -f
    git clone https://github.com/MicrosoftLearning/dp-203-azure-data-engineer dp203
    ```

5. 克隆存储库后，输入以下命令以更改为此实验室的文件夹，然后运行其中包含的 setup.ps1 脚本：

    ```
    cd dp203/Allfiles/labs/02
    ./setup.ps1
    ```

6. 如果出现提示，请选择要使用的订阅（仅当有权访问多个 Azure 订阅时才会发生这种情况）。
7. 出现提示时，输入要为 Azure Synapse SQL 池设置的合适密码。

    > 注意：请务必记住此密码！

8. 等待脚本完成 - 此过程通常需要大约 10 分钟；但在某些情况下可能需要更长的时间。 等待时，请查看 Azure Synapse Analytics 文档中的 [Azure Synapse Analytics 中的无服务器 SQL 池](https://docs.microsoft.com/azure/synapse-analytics/sql/on-demand-workspace-overview)一文。

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
8. 右键单击任一文件，然后选择“预览”以查看它所包含的数据。 请注意，这些文件不包含标题行，因此你可以取消选择显示列标题的选项。
9. 关闭预览，然后使用“&#8593;”按钮导航回 sales 文件夹 。
10. 在 sales 文件夹中，打开 json 文件夹，注意它包含 .json 文件中的一些示例销售订单 。 预览这些文件中的任何一个，以查看用于销售订单的 JSON 格式。
11. 关闭预览，然后使用“&#8593;”按钮导航回 sales 文件夹 。
12. 在 sales 文件夹中，打开 parquet 文件夹，注意它包含（2019-2021 年）每年的子文件夹，每个子文件夹中名为 orders.snappy.parquet 的每个文件包含该年份的订单数据  。 
13. 返回到 sales 文件夹，以便可以看到 csv、 json 和 parquet 文件夹   。

### 使用 SQL 查询 CSV 文件

1. 选择 csv 文件夹，然后在工具栏上的“新建 SQL 脚本”列表中，选中“选择前 100 行”  。
2. 在“文件类型”列表中，选择“文本格式”，然后应用设置以打开查询文件夹中数据的新 SQL 脚本 。
3. 在创建的 SQL 脚本 1 的“属性”窗格中，将名称更改为“Sales CSV 查询”，并更改结果设置以显示“所有行”   。 然后在工具栏中，选择“发布”以保存脚本并使用工具栏右侧的“属性”按钮（类似于 &#128463;.）隐藏“属性”窗格   。
4. 查看已经生成的 SQL 代码，代码应该类似于：

    ```SQL
    -- This is auto-generated code
    SELECT
        TOP 100 *
    FROM
        OPENROWSET(
            BULK 'https://datalakexxxxxxx.dfs.core.windows.net/files/sales/csv/',
            FORMAT = 'CSV',
            PARSER_VERSION='2.0'
        ) AS [result]
    ```

    此代码使用 OPENROWSET 从 sales 文件夹中的 CSV 文件读取数据，并检索前 100 行数据。

5. 在“连接到”列表中，确保已选中“内置”，这表示通过工作区创建的内置 SQL 池。
6. 在工具栏上，使用“▷ 运行”按钮运行 SQL 代码，并查看结果，结果应如下所示：

    | C1 | C2 | C3 | C4 | C5 | C6 | C7 | C8 | C9 |
    | -- | -- | -- | -- | -- | -- | -- | -- | -- |
    | SO45347 | 1 | 2020-01-01 | Clarence Raji | clarence35@adventure-works.com |Road-650 Black, 52 | 1 | 699.0982 | 55.9279 |
    | [.] | [.] | [.] | [.] | [.] | [.] | [.] | [.] | [.] |

7. 请注意，结果由名为 C1、C2 等的列组成。 在此示例中，CSV 文件不包含列标题。 虽然可以使用已分配的泛型列名或按序号位置处理数据，但如果定义表格架构，则更容易理解数据。 为此，请向如下所示的 OPENROWSET 函数添加 WITH 子句（将 datalakexxxxxxx 替换为 Data Lake Storage 帐户的名称），然后重新运行查询：

    ```SQL
    SELECT
        TOP 100 *
    FROM
        OPENROWSET(
            BULK 'https://datalakexxxxxxx.dfs.core.windows.net/files/sales/csv/',
            FORMAT = 'CSV',
            PARSER_VERSION='2.0'
        )
        WITH (
            SalesOrderNumber VARCHAR(10) COLLATE Latin1_General_100_BIN2_UTF8,
            SalesOrderLineNumber INT,
            OrderDate DATE,
            CustomerName VARCHAR(25) COLLATE Latin1_General_100_BIN2_UTF8,
            EmailAddress VARCHAR(50) COLLATE Latin1_General_100_BIN2_UTF8,
            Item VARCHAR(30) COLLATE Latin1_General_100_BIN2_UTF8,
            Quantity INT,
            UnitPrice DECIMAL(18,2),
            TaxAmount DECIMAL (18,2)
        ) AS [result]
    ```

    现在，结果如下所示：

    | SalesOrderNumber | SalesOrderLineNumber | OrderDate | CustomerName | EmailAddress | 项 | 数量 | 单价 | TaxAmount |
    | -- | -- | -- | -- | -- | -- | -- | -- | -- |
    | SO45347 | 1 | 2020-01-01 | Clarence Raji | clarence35@adventure-works.com |Road-650 Black, 52 | 1 | 699.10 | 55.93 |
    | [.] | [.] | [.] | [.] | [.] | [.] | [.] | [.] | [.] |

8. 将更改发布到脚本，然后关闭脚本窗格。

### 使用 SQL 查询 parquet 文件

虽然 CSV 是一种易于使用的格式，但在大数据处理场景中通常使用已针对压缩、索引和分区进行优化的文件格式。 其中最常见的一种格式是 parquet。

1. 在包含 Data Lake 文件系统的“文件”选项卡中，返回到 sales 文件夹，以便可以看到 csv、 json 和 parquet 文件夹    。
2. 选择 parquet 文件夹，然后在工具栏上的“新建 SQL 脚本”列表中，选中“选择前 100 行”  。
3. 在“文件类型”列表中，选择“Parquet 格式”，然后应用设置以打开查询文件夹中数据的新 SQL 脚本 。 脚本应类似于：

    ```SQL
    -- This is auto-generated code
    SELECT
        TOP 100 *
    FROM
        OPENROWSET(
            BULK 'https://datalakexxxxxxx.dfs.core.windows.net/files/sales/parquet/**',
            FORMAT = 'PARQUET'
        ) AS [result]
    ```

4. 运行代码，注意它将返回与前面浏览的 CSV 文件相同的架构中的销售订单数据。 架构信息嵌入到 parquet 文件中，因此结果中会显示相应的列名称。
5. 按如下所示修改代码（将 datalakexxxxxxx 替换为 Data Lake Storage 帐户的名称），然后运行代码。

    ```sql
    SELECT YEAR(OrderDate) AS OrderYear,
           COUNT(*) AS OrderedItems
    FROM
        OPENROWSET(
            BULK 'https://datalakexxxxxxx.dfs.core.windows.net/files/sales/parquet/**',
            FORMAT = 'PARQUET'
        ) AS [result]
    GROUP BY YEAR(OrderDate)
    ORDER BY OrderYear
    ```

6. 请注意，结果包括三年的所有订单计数 - BULK 路径中使用的通配符会导致查询返回所有子文件夹中的数据。

    子文件夹反映 parquet 数据中的分区，该技术通常用于优化可并行处理多个数据分区的系统的性能。 分区还可用来筛选数据。

7. 按如下所示修改代码（将 datalakexxxxxxx 替换为 Data Lake Storage 帐户的名称），然后运行代码。

    ```sql
    SELECT YEAR(OrderDate) AS OrderYear,
           COUNT(*) AS OrderedItems
    FROM
        OPENROWSET(
            BULK 'https://datalakexxxxxxx.dfs.core.windows.net/files/sales/parquet/year=*/',
            FORMAT = 'PARQUET'
        ) AS [result]
    WHERE [result].filepath(1) IN ('2019', '2020')
    GROUP BY YEAR(OrderDate)
    ORDER BY OrderYear
    ```

8. 查看结果，注意它们仅包括 2019 年和 2020 年的销售计数。 实现此过滤的方式是添加 BULK 路径中的分区文件夹值的通配符 (year=) 和基于 OPENROWSET 返回的结果的 filepath 属性的 WHERE 子句（在本例中具有别名 [result]） *\**  。

9. 将脚本命名为“Sales Parquet 查询”，然后发布。 然后关闭脚本窗格。

### 使用 SQL 查询 JSON 文件

JSON 是另一种常用的数据格式，因此有助于实现够查询无服务器 SQL 池中的 .json 文件。

1. 在包含 Data Lake 文件系统的“文件”选项卡中，返回到 sales 文件夹，以便可以看到 csv、 json 和 parquet 文件夹    。
2. 选择 json 文件夹，然后在工具栏上的“新建 SQL 脚本”列表中，选中“选择前 100 行”  。
3. 在“文件类型”列表中，选择“文本格式”，然后应用设置以打开查询文件夹中数据的新 SQL 脚本 。 脚本应类似于：

    ```sql
    -- This is auto-generated code
    SELECT
        TOP 100 *
    FROM
        OPENROWSET(
            BULK 'https://datalakexxxxxxx.dfs.core.windows.net/files/sales/json/',
            FORMAT = 'CSV',
            PARSER_VERSION = '2.0'
        ) AS [result]
    ```

    该脚本旨在查询以逗号分隔的 (CSV) 数据，而不是 JSON，因此需要前进行一些修改才能正常使用。

4. 按如下所示修改脚本（将 datalakexxxxxxx 替换为 Data Lake Storage 帐户的名称）以：
    - 删除分析器版本参数。
    - 使用字符代码 0x0b 为字段终止符、带引号的字段和行终止符添加参数。
    - 将结果格式化为包含 JSON 数据行的单个字段，作为 NVARCHAR (MAX) 字符串。

    ```sql
    SELECT
        TOP 100 *
    FROM
        OPENROWSET(
            BULK 'https://datalakexxxxxxx.dfs.core.windows.net/files/sales/json/',
            FORMAT = 'CSV',
            FIELDTERMINATOR ='0x0b',
            FIELDQUOTE = '0x0b',
            ROWTERMINATOR = '0x0b'
        ) WITH (Doc NVARCHAR(MAX)) as rows
    ```

5. 运行修改的代码，注意结果包含每个订单的 JSON 文档。

6. 按如下所示修改查询（将 datalakexxxxxxx 替换为 Data Lake Storage 帐户的名称），以便它使用 JSON_VALUE 函数从 JSON 数据中提取各个字段值。

    ```sql
    SELECT JSON_VALUE(Doc, '$.SalesOrderNumber') AS OrderNumber,
           JSON_VALUE(Doc, '$.CustomerName') AS Customer,
           Doc
    FROM
        OPENROWSET(
            BULK 'https://datalakexxxxxxx.dfs.core.windows.net/files/sales/json/',
            FORMAT = 'CSV',
            FIELDTERMINATOR ='0x0b',
            FIELDQUOTE = '0x0b',
            ROWTERMINATOR = '0x0b'
        ) WITH (Doc NVARCHAR(MAX)) as rows
    ```

7. 将脚本命名为“Sales JSON 查询”，然后发布。 然后关闭脚本窗格。

## 访问数据库中的外部数据

到目前为止，已在 SELECT 查询中使用 OPENROWSET 函数从 Data Lake 中的文件检索数据。 查询已在无服务器 SQL 池的 master 数据库上下文中运行。 此方法适用于初始探索数据，但如果计划创建更复杂的查询，则使用 Synapse SQL 的 PolyBase 功能在数据库中创建引用外部数据位置的对象可能更有效。

### 创建外部数据源

通过在数据库中定义外部数据源，可以使用它引用存储文件的 Data Lake 位置。

1. 在 Synapse Studio 的“开发”页上的“+”菜单中，选择“SQL 脚本”  。
2. 在新脚本窗格中，添加以下代码（将 datalakexxxxxxx 替换为 Data Lake Storage 帐户的名称）以创建新数据库并向其添加外部数据源。

    ```sql
    CREATE DATABASE Sales
      COLLATE Latin1_General_100_BIN2_UTF8;
    GO;

    Use Sales;
    GO;

    CREATE EXTERNAL DATA SOURCE sales_data WITH (
        LOCATION = 'https://datalakexxxxxxx.dfs.core.windows.net/files/sales/'
    );
    GO;
    ```

3. 修改脚本属性以将其名称更改为“创建销售数据库”，然后发布。
4. 确保脚本已连接到内置 SQL 池和 master 数据库，然后运行 。
5. 切换回“数据”页，并使用 Synapse Studio 右上角的“&#8635;”按钮刷新页面 。 然后，在“数据”窗格中查看“工作区”选项卡，此时会显示“SQL 数据库”列表  。 展开此列表以验证是否已创建 Sales 数据库。
6. 展开 Sales 数据库、其 External Resources 文件夹以及其下的 External data sources 文件夹，以查看所创建的 sales_data 外部数据源   。
7. 在 Sales 数据库的“...”菜单中，选择“新建 SQL 脚本” > “空脚本”   。 然后在新的脚本窗格中，输入并运行以下查询：

    ```sql
    SELECT *
    FROM
        OPENROWSET(
            BULK 'csv/*.csv',
            DATA_SOURCE = 'sales_data',
            FORMAT = 'CSV',
            PARSER_VERSION = '2.0'
        ) AS orders
    ```

    查询使用外部数据源连接到 Data Lake，OPENROWSET 函数现在只需要引用 .csv 文件的相对路径。

8. 按如下所示修改代码，以使用数据源查询 parquet 文件。

    ```sql
    SELECT *
    FROM  
        OPENROWSET(
            BULK 'parquet/year=*/*.snappy.parquet',
            DATA_SOURCE = 'sales_data',
            FORMAT='PARQUET'
        ) AS orders
    WHERE orders.filepath(1) = '2019'
    ```

### 创建外部表

虽然使用外部数据源可以更轻松地访问 Data Lake 中的文件，但使用 SQL 的大多数数据分析师需要处理数据库中的表。 幸运的是，还可以定义外部文件格式和外部表，在数据集表中封装文件的行集。

1. 将 SQL 代码替换为以下语句来定义 CSV 文件的外部数据格式，以及引用 CSV 文件的外部表，并运行：

    ```sql
    CREATE EXTERNAL FILE FORMAT CsvFormat
        WITH (
            FORMAT_TYPE = DELIMITEDTEXT,
            FORMAT_OPTIONS(
            FIELD_TERMINATOR = ',',
            STRING_DELIMITER = '"'
            )
        );
    GO;

    CREATE EXTERNAL TABLE dbo.orders
    (
        SalesOrderNumber VARCHAR(10),
        SalesOrderLineNumber INT,
        OrderDate DATE,
        CustomerName VARCHAR(25),
        EmailAddress VARCHAR(50),
        Item VARCHAR(30),
        Quantity INT,
        UnitPrice DECIMAL(18,2),
        TaxAmount DECIMAL (18,2)
    )
    WITH
    (
        DATA_SOURCE =sales_data,
        LOCATION = 'csv/*.csv',
        FILE_FORMAT = CsvFormat
    );
    GO
    ```

2. 刷新并展开“数据”窗格中的 External tables 文件夹，并确认已在 Sales 数据库中创建了名为 dbo.orders 的表   。
3. 在 dbo.orders 表的“...”菜单中，选择“新建 SQL 脚本” > “选择前 100 行”   。
4. 运行已生成的 SELECT 脚本，并验证它是否从表中检索前 100 行数据，从而引用 Data Lake 中的文件。

    >注意：应始终选择最适合你的特定需求和使用场景的方法。 有关更多详细信息，可查看[如何在 Azure Synapse Analytics 中通过无服务器 SQL 池使用 OPENROWSET](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql/develop-openrowset) 和[在 Azure Synapse Analytics 中使用无服务器 SQL 池访问外部存储](https://learn.microsoft.com/en-us/azure/synapse-analytics/sql/develop-storage-files-overview?tabs=impersonation)文章。

## 可视化查询结果

现在，你已了解了使用 SQL 查询来查询 Data Lake 中文件的各种方法，还可以分析这些查询的结果，以便深入了解数据。 通常，通过在图表中可视化查询结果更容易发现见解；在 Synapse Studio 查询编辑器中使用集成图表功能可以轻松进行可视化。

1. 在“开发”页上，新建一个空的 SQL 查询。
2. 确保脚本已连接到内置 SQL 池和 Sales 数据库 。
3. 输入并运行下面的 SQL 代码：

    ```sql
    SELECT YEAR(OrderDate) AS OrderYear,
           SUM((UnitPrice * Quantity) + TaxAmount) AS GrossRevenue
    FROM dbo.orders
    GROUP BY YEAR(OrderDate)
    ORDER BY OrderYear;
    ```

4. 在“结果”窗格中，选择“图表”并查看为你创建的图表；该图表应为折线图 。
5. 将“类别列”更改为 OrderYear，以便折线图显示 2019 年到 2021 年三年内的收入趋势 ：

    ![按年显示收入的折线图](./images/yearly-sales-line.png)

6. 将“图表类型”切换为“柱形图”，以柱形图的形式显示年度收入 ：

    ![按年显示收入的柱形图](./images/yearly-sales-column.png)

7. 在查询编辑器中试验图表功能。 它提供了一些可以在以交互方式浏览数据时使用的基本图表功能，并且你可以将图表另存为图像以包含在报表中。 但是，功能与 Microsoft Power BI 等企业数据可视化工具相比是有限的。

## 删除 Azure 资源

你已完成对 Azure Synapse Analytics 的探索，现在应删除已创建的资源，以避免产生不必要的 Azure 成本。

1. 关闭 Synapse Studio 浏览器选项卡并返回到 Azure 门户。
2. 在 Azure 门户的“主页”上，选择“资源组”。
3. 选择 Synapse Analytics 工作区的 dp203-*xxxxxxx* 资源组（不是受管理资源组），并确认是否它包含 Synapse 工作区和你的工作区的存储帐户。
4. 在资源组的“概述”页的顶部，选择“删除资源组”。
5. 输入 dp203-xxxxxxx 资源组名称以确认要删除该资源组，然后选择“删除” 。

    几分钟后，将删除 Azure Synapse 工作区资源组及其关联的托管工作区资源组。