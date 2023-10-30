---
lab:
  title: 将数据加载到关系数据仓库中
  ilt-use: Lab
---

# 将数据加载到关系数据仓库中

在本练习中，你会将数据加载到专用 SQL 池中。

完成此练习大约需要 30 分钟。

## 准备工作

需要一个你在其中具有管理级权限的 [Azure 订阅](https://azure.microsoft.com/free)。

## 预配 Azure Synapse Analytics 工作区

你需要一个可以访问 Data Lake Storage 的 Azure Synapse Analytics 工作区，以及一个托管数据仓库的专用 SQL 池。

在本练习中，你将组合使用 PowerShell 脚本和 ARM 模板来预配 Azure Synapse Analytics 工作区。

1. 登录到 Azure 门户，地址为 [](https://portal.azure.com)。
2. 使用页面顶部搜索栏右侧的 [\>_] 按钮在 Azure 门户中创建新的 Cloud Shell，在出现提示时选择“PowerShell”环境并创建存储。 Cloud Shell 在 Azure 门户底部的窗格中提供命令行界面，如下所示：

    ![具有 Cloud Shell 窗格的 Azure 门户](./images/cloud-shell.png)

    > 注意：如果以前创建了使用 Bash 环境的 Cloud shell，请使用 Cloud Shell 窗格左上角的下拉菜单将其更改为“PowerShell”。

3. 可以通过拖动窗格顶部的分隔条来调整 Cloud Shell 的大小，或使用窗格右上角的“—”、“&#9723;”和“X”图标来最小化、最大化和关闭窗格 。 有关如何使用 Azure Cloud Shell 的详细信息，请参阅 [Azure Cloud Shell 文档](https://docs.microsoft.com/azure/cloud-shell/overview)。

4. 在 PowerShell 窗格中，输入以下命令以克隆此存储库：

    ```powershell
    rm -r dp-203 -f
    git clone https://github.com/MicrosoftLearning/dp-203-azure-data-engineer dp-203
    ```

5. 克隆存储库后，输入以下命令以更改为此练习的文件夹，然后运行其中包含的 setup.ps1 脚本：

    ```powershell
    cd dp-203/Allfiles/labs/09
    ./setup.ps1
    ```

6. 如果出现提示，请选择要使用的订阅（仅当有权访问多个 Azure 订阅时才会出现这种选项）。
7. 出现提示时，输入要为 Azure Synapse SQL 池设置的合适密码。

    > 注意：请务必记住此密码！

8. 等待脚本完成，这通常需要大约 10 分钟，但在某些情况下可能需要更长的时间。 等待时，请查看 Azure Synapse Analytics 文档中的 [Azure Synapse Analytics 中专用 SQL 池的数据加载策略](https://learn.microsoft.com/azure/synapse-analytics/sql-data-warehouse/design-elt-data-loading)一文。

## 准备加载数据

1. 脚本完成后，在 Azure 门户中转到其创建的 dp203-xxxxxxx 资源组，然后选择 Synapse 工作区**。
2. 在 Synapse 工作区“概述”页的“打开 Synapse Studio”卡中，选择“打开”，以在新浏览器标签页中打开 Synapse Studio；如果出现提示，请进行登录  。
3. 在 Synapse Studio 左侧，使用“››”图标展开菜单，这将显示 Synapse Studio 中用于管理资源和执行数据分析任务的不同页面。
4. 在“管理”页的“SQL 池”选项卡上，选择 sqlxxxxxxx 专用 SQL 池（会托管此练习的数据仓库）所在的行，并使用其 &#9655; 图标进行启动；在出现系统提示时确认进行恢复   **。

    恢复池可能需要几分钟时间。 可以使用“&#8635; 刷新”按钮定期检查其状态。 准备就绪时状态将显示为“联机”。 等待时，请继续执行以下步骤，查看要加载的数据文件。

5. 在“数据”页上，查看“已链接”选项卡并验证工作区是否包含 Azure Data Lake Storage Gen2 存储帐户的链接，该帐户的名称应类似于 synapsexxxxxxx (Primary - datalakexxxxxxx)  。
6. 展开存储帐户，验证它是否包含名为“files (primary)”的文件系统容器。
7. 选择“files”容器，并注意它包含名为“data”的文件夹。 此文件夹包含要加载到数据仓库中的数据文件。
8. 打开“data”文件夹，并观察其中是否包含客户和产品数据的 .csv 文件。
9. 右键单击任一文件，然后选择“预览”以查看它所包含的数据。 请注意，这些文件包含标题行，因此你可以选择显示列标题的选项。
10. 返回到“管理”页，验证专用 SQL 池是否处于联机状态。

## 加载数据仓库表

我们来看一些将数据加载到 Data Warehouse 的基于 SQL 的方法。

1. 在“数据”页上，选择“工作区”选项卡 。
2. 展开 SQL 数据库并选择 sqlxxxxxxx 数据库 **。 然后，在其“…”菜单中，选择“新建 SQL 脚本” > 
“空脚本”  。

你现在有一个空白的 SQL 页，该页连接到以下练习的实例。 你将使用此脚本来探索可用于加载数据的多种 SQL 技术。

### 使用 COPY 语句从数据湖加载数据

1. 在 SQL 脚本中，在窗口中输入以下代码。

    ```sql
    SELECT COUNT(1) 
    FROM dbo.StageProduct
    ```

2. 在工具栏上，使用“&#9655; 运行”按钮运行 SQL 代码并确认 StageProduct 表中当前有 0 行  。
3. 将代码替换为以下 COPY 语句（将 datalakexxxxxx 更改为自己的数据湖名称**：

    ```sql
    COPY INTO dbo.StageProduct
        (ProductID, ProductName, ProductCategory, Color, Size, ListPrice, Discontinued)
    FROM 'https://datalakexxxxxx.blob.core.windows.net/files/data/Product.csv'
    WITH
    (
        FILE_TYPE = 'CSV',
        MAXERRORS = 0,
        IDENTITY_INSERT = 'OFF',
        FIRSTROW = 2 --Skip header row
    );


    SELECT COUNT(1) 
    FROM dbo.StageProduct
    ```

4. 运行脚本并查看结果。 有 11 行应已经加载到 StageProduct 表中。

    现在，我们使用同一种技术来加载另一个表，这次会记录可能发生的任何错误。

5. 将脚本窗格中的 SQL 代码替换为以下代码，将 datalakexxxxxx 更改为 ```FROM``` 和 ```ERRORFILE``` 子句中数据湖的名称**：

    ```sql
    COPY INTO dbo.StageCustomer
    (GeographyKey, CustomerAlternateKey, Title, FirstName, MiddleName, LastName, NameStyle, BirthDate, 
    MaritalStatus, Suffix, Gender, EmailAddress, YearlyIncome, TotalChildren, NumberChildrenAtHome, EnglishEducation, 
    SpanishEducation, FrenchEducation, EnglishOccupation, SpanishOccupation, FrenchOccupation, HouseOwnerFlag, 
    NumberCarsOwned, AddressLine1, AddressLine2, Phone, DateFirstPurchase, CommuteDistance)
    FROM 'https://datalakexxxxxx.dfs.core.windows.net/files/data/Customer.csv'
    WITH
    (
    FILE_TYPE = 'CSV'
    ,MAXERRORS = 5
    ,FIRSTROW = 2 -- skip header row
    ,ERRORFILE = 'https://datalakexxxxxx.dfs.core.windows.net/files/'
    );
    ```

6. 运行脚本并查看生成的消息。 源文件包含包含一行无效数据，因此拒绝了一行。 上面的代码最多指定 5 个错误，因此单个错误不应阻止有效行进行加载。 可以通过运行以下查询来查看已加载的行。

    ```sql
    SELECT *
    FROM dbo.StageCustomer
    ```

7. 在“文件”选项卡上，查看数据湖的根文件夹并验证是否已创建名为“_rejectedrows”的新文件夹（如果未看到此文件夹，请在“更多”菜单中，选择“刷新”以刷新视图）   。
8. 打开“_rejectedrows”文件夹及其包含的日期和时间特定子文件夹，请注意，名称类似于 *QID123_1_2*.Error.Txt 和 *QID123_1_2*.Row.Txt 的文件已经创建  。 可以右键单击其中每个文件，然后选择“预览”，以查看错误的详细信息以及被拒绝的行。

    通过使用临时表，你可以在移动或使用数据追加或更新插入任何现有维度表之前验证或转换数据。 COPY 语句提供了一种简单但性能高的技术，可用于将数据从数据湖中的文件轻松加载到临时表中，而且如你所见，还可用于标识和重定向无效行。

### 使用 CREATE TABLE AS (CTAS) 语句

1. 返回到脚本窗格，并将其包含的代码替换为以下代码：

    ```sql
    CREATE TABLE dbo.DimProduct
    WITH
    (
        DISTRIBUTION = HASH(ProductAltKey),
        CLUSTERED COLUMNSTORE INDEX
    )
    AS
    SELECT ROW_NUMBER() OVER(ORDER BY ProductID) AS ProductKey,
        ProductID AS ProductAltKey,
        ProductName,
        ProductCategory,
        Color,
        Size,
        ListPrice,
        Discontinued
    FROM dbo.StageProduct;
    ```

2. 运行脚本，该脚本根据暂存产品数据创建名为 DimProduct 的新表，该数据使用 ProductAltKey 作为其哈希分布键并具有聚集列存储索引 。
4. 使用以下查询查看新 DimProduct 表的内容：

    ```sql
    SELECT ProductKey,
        ProductAltKey,
        ProductName,
        ProductCategory,
        Color,
        Size,
        ListPrice,
        Discontinued
    FROM dbo.DimProduct;
    ```

    CREATE TABLE AS SELECT (CTAS) 表达式具有多种用途，其中包括：

    - 重新分发表的哈希键以与其他表保持一致，从而提升查询性能。
    - 执行增量分析后，根据现有值将代理键分配给临时表。
    - 快速创建聚合表以用于报告。

### 合并 INSERT 和 UPDATE 语句以加载缓慢变化的维度表

DimCustomer 表支持类型 1 和类型 2 缓慢变化的维度 (SCD)，其中类型 1 变化会导致就地更新到现有行，而类型 2 变化会产生新的一行来指示特定维度实体实例的最新版本。 加载此表需要结合使用 INSERT 语句（用于加载新客户）和 UPDATE 语句（用于应用类型 1 或类型 2 变化）。

1. 在查询窗格中，将现有 SQL 代码替换为以下代码：

    ```sql
    INSERT INTO dbo.DimCustomer ([GeographyKey],[CustomerAlternateKey],[Title],[FirstName],[MiddleName],[LastName],[NameStyle],[BirthDate],[MaritalStatus],
    [Suffix],[Gender],[EmailAddress],[YearlyIncome],[TotalChildren],[NumberChildrenAtHome],[EnglishEducation],[SpanishEducation],[FrenchEducation],
    [EnglishOccupation],[SpanishOccupation],[FrenchOccupation],[HouseOwnerFlag],[NumberCarsOwned],[AddressLine1],[AddressLine2],[Phone],
    [DateFirstPurchase],[CommuteDistance])
    SELECT *
    FROM dbo.StageCustomer AS stg
    WHERE NOT EXISTS
        (SELECT * FROM dbo.DimCustomer AS dim
        WHERE dim.CustomerAlternateKey = stg.CustomerAlternateKey);

    -- Type 1 updates (change name, email, or phone in place)
    UPDATE dbo.DimCustomer
    SET LastName = stg.LastName,
        EmailAddress = stg.EmailAddress,
        Phone = stg.Phone
    FROM DimCustomer dim inner join StageCustomer stg
    ON dim.CustomerAlternateKey = stg.CustomerAlternateKey
    WHERE dim.LastName <> stg.LastName OR dim.EmailAddress <> stg.EmailAddress OR dim.Phone <> stg.Phone

    -- Type 2 updates (address changes triggers new entry)
    INSERT INTO dbo.DimCustomer
    SELECT stg.GeographyKey,stg.CustomerAlternateKey,stg.Title,stg.FirstName,stg.MiddleName,stg.LastName,stg.NameStyle,stg.BirthDate,stg.MaritalStatus,
    stg.Suffix,stg.Gender,stg.EmailAddress,stg.YearlyIncome,stg.TotalChildren,stg.NumberChildrenAtHome,stg.EnglishEducation,stg.SpanishEducation,stg.FrenchEducation,
    stg.EnglishOccupation,stg.SpanishOccupation,stg.FrenchOccupation,stg.HouseOwnerFlag,stg.NumberCarsOwned,stg.AddressLine1,stg.AddressLine2,stg.Phone,
    stg.DateFirstPurchase,stg.CommuteDistance
    FROM dbo.StageCustomer AS stg
    JOIN dbo.DimCustomer AS dim
    ON stg.CustomerAlternateKey = dim.CustomerAlternateKey
    AND stg.AddressLine1 <> dim.AddressLine1;
    ```

2. 运行脚本并查看输出。

## 执行加载后优化

将新数据加载到数据仓库后，建议重新生成表索引，并更新常用查询列的统计信息。

1. 将脚本窗格中的代码替换为以下代码：

    ```sql
    ALTER INDEX ALL ON dbo.DimProduct REBUILD;
    ```

2. 运行脚本以在 DimProduct 表上重新生成索引。
3. 将脚本窗格中的代码替换为以下代码：

    ```sql
    CREATE STATISTICS customergeo_stats
    ON dbo.DimCustomer (GeographyKey);
    ```

4. 运行脚本以在 DimCustomer 表的 GeographyKey 列上创建或更新统计信息 。

## 删除 Azure 资源

你已完成对 Azure Synapse Analytics 的探索，现在应删除已创建的资源，以避免产生不必要的 Azure 成本。

1. 关闭 Synapse Studio 浏览器选项卡并返回到 Azure 门户。
2. 在 Azure 门户的“主页”上，选择“资源组”。
3. 选择 Synapse Analytics 工作区的 dp203-*xxxxxxx* 资源组（不是受管理资源组），并确认它包含 Synapse 工作区、存储帐户和工作区的 Spark 池。
4. 在资源组的“概述”页的顶部，选择“删除资源组”。
5. 输入 dp203-xxxxxxx 资源组名称以确认要删除该资源组，然后选择“删除” 。

    几分钟后，将删除 Azure Synapse 工作区资源组及其关联的托管工作区资源组。
