---
lab:
  title: 在 Azure Synapse Analytics 中使用 Delta Lake
  ilt-use: Lab
---

# 在 Azure Synapse Analytics 中使用 Delta Lake 和 Spark

Delta Lake 是一个开源项目，用于在数据湖之上生成事务数据存储层。 Delta Lake 为批处理和流式处理数据操作添加了对关系语义的支持，并支持创建 Lakehouse 体系结构。在该体系结构中，Apache Spark 可用于处理和查询基于数据湖中基础文件的表中的数据。

完成此练习大约需要 40 分钟。

## 准备工作

需要一个你在其中具有管理级权限的 [Azure 订阅](https://azure.microsoft.com/free)。

## 预配 Azure Synapse Analytics 工作区

需要一个 Azure Synapse Analytics 工作区，该工作区可以访问 Data Lake Storage 和 Apache Spark 池（可用于查询和处理 Data Lake 中的文件）。

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
    cd dp-203/Allfiles/labs/07
    ./setup.ps1
    ```

6. 如果出现提示，请选择要使用的订阅（仅当有权访问多个 Azure 订阅时才会发生这种情况）。
7. 出现提示时，输入要为 Azure Synapse SQL 池设置的合适密码。

    > 注意：请务必记住此密码！

8. 等待脚本完成 - 此过程通常需要大约 10 分钟；但在某些情况下可能需要更长的时间。 在等待期间，请查看 Azure Synapse Analytics 文档中的[什么是 Delta Lake？](https://docs.microsoft.com/azure/synapse-analytics/spark/apache-spark-what-is-delta-lake)一文。

## 创建 Delta 表

该脚本预配 Azure Synapse Analytics 工作区和 Azure 存储帐户来托管数据湖，然后将数据文件上传到数据湖。

### 浏览数据湖中的数据

1. 脚本完成后，在 Azure 门户中转到创建的 dp203-*xxxxxxx* 资源组，然后选择 Synapse 工作区。
2. 在 Synapse 工作区“概述”页的“打开 Synapse Studio”卡中，选择“打开”，以在新浏览器标签页中打开 Synapse Studio；如果出现提示，请进行登录  。
3. 在 Synapse Studio 左侧，使用 &rsaquo;&rsaquo; 图标展开菜单，这将显示 Synapse Studio 中用于管理资源和执行数据分析任务的不同页面。
4. 在“数据”页上，查看“已链接”选项卡并验证工作区是否包含 Azure Data Lake Storage Gen2 存储帐户的链接，该帐户的名称应类似于 synapsexxxxxxx* (Primary - datalake xxxxxxx*) ** 。
5. 展开存储帐户，验证它是否包含名为 files 的文件系统容器。
6. 选择“files”容器，并注意它包含名为“products”的文件夹 。 此文件夹包含你将在本练习中处理的数据。
7. 打开“products”文件夹，并观察它是否包含名为 products.csv 的文件 。
8. 选择 products.csv，然后在工具栏上的“新建笔记本”列表中选择“加载到 DataFrame”  。
9. 在打开的“笔记本 1”窗格中，在“附加到”列表中，选择 Spark 池“sparkxxxxxxx”，并确保“语言”设置为“PySpark (Python)”    。
10. 查看笔记本中的第一个（也是唯一一个）单元格中的代码，它应该如下所示：

    ```Python
    %%pyspark
    df = spark.read.load('abfss://files@datalakexxxxxxx.dfs.core.windows.net/products/products.csv', format='csv'
    ## If header exists uncomment line below
    ##, header=True
    )
    display(df.limit(10))
    ```

11. 取消注释“,header=True”这一行（因为 products.csv 文件的第一行包含列标题），因此代码如下所示：

    ```Python
    %%pyspark
    df = spark.read.load('abfss://files@datalakexxxxxxx.dfs.core.windows.net/products/products.csv', format='csv'
    ## If header exists uncomment line below
    , header=True
    )
    display(df.limit(10))
    ```

12. 使用代码单元格左侧的“▷”图标运行它，并等待结果。 当你第一次在笔记本中运行单元格时，Spark 池会启动，因此可能需要一分钟左右的时间来返回任何结果。 最终，结果应显示在单元格下面，应如下所示：

    | ProductID | ProductName | 类别 | ListPrice |
    | -- | -- | -- | -- |
    | 771 | Mountain-100 Silver, 38 | 山地自行车 | 3399.9900 |
    | 772 | Mountain-100 Silver, 42 | 山地自行车 | 3399.9900 |
    | ... | ... | ... | ... |

### 将文件数据加载到 delta 表中

1. 在第一个代码单元格返回的结果下，使用“+ 代码”按钮添加新的代码单元格。 然后在新单元格中输入以下代码并运行它：

    ```Python
    delta_table_path = "/delta/products-delta"
    df.write.format("delta").save(delta_table_path)
    ```

2. 在“files”选项卡上使用工具栏中的“&#8593;”图标返回到“files”容器的根目录，然后请注意观察已创建的“delta”新文件   。 打开此文件夹及其包含的 products-delta 表，应该能在其中看到包含数据的 Parquet 格式文件。

3. 返回到“笔记本 1”选项卡并添加另一个新的代码单元格。 然后，在新单元格中添加以下代码并运行它：

    ```Python
    from delta.tables import *
    from pyspark.sql.functions import *

    # Create a deltaTable object
    deltaTable = DeltaTable.forPath(spark, delta_table_path)

    # Update the table (reduce price of product 771 by 10%)
    deltaTable.update(
        condition = "ProductID == 771",
        set = { "ListPrice": "ListPrice * 0.9" })

    # View the updated data as a dataframe
    deltaTable.toDF().show(10)
    ```

    数据将加载到 DeltaTable 对象中并更新。 更新的内容会体现在查询结果中。

4. 使用以下代码添加另一个新的代码单元并运行它：

    ```Python
    new_df = spark.read.format("delta").load(delta_table_path)
    new_df.show(10)
    ```

    代码将 delta 表数据从数据湖中的位置加载到数据帧中，验证通过 DeltaTable 对象所做的更改是否已持久保存。

5. 修改刚才运行的代码（如下所示），选择使用 Delta Lake 的“按时间顺序查看”功能来查看旧版数据。

    ```Python
    new_df = spark.read.format("delta").option("versionAsOf", 0).load(delta_table_path)
    new_df.show(10)
    ```

    运行修改后的代码时，结果将显示数据的原始版本。

6. 使用以下代码添加另一个新的代码单元并运行它：

    ```Python
    deltaTable.history(10).show(20, False, True)
    ```

    将显示该表最近 20 次更改的历史记录：此时应该有两条记录（一个是原始创建操作，另一个是你进行的更改。）

## 创建目录表

到目前为止，你已通过从包含表所基于 parquet 文件的文件夹加载数据来处理 Delta 表。 可以定义封装数据的目录表，并提供可以在 SQL 代码中引用的命名表实体。 对于 Delta Lake，Spark 支持两种类型的目录表：

- 由包含表数据的 parquet 文件的路径定义的外部表。
- 在 Hive 元存储中为 Spark 池定义的托管表。

### 创建外部表

1. 在新的代码单元格中，添加并运行以下代码：

    ```Python
    spark.sql("CREATE DATABASE AdventureWorks")
    spark.sql("CREATE TABLE AdventureWorks.ProductsExternal USING DELTA LOCATION '{0}'".format(delta_table_path))
    spark.sql("DESCRIBE EXTENDED AdventureWorks.ProductsExternal").show(truncate=False)
    ```

    此代码创建名为“AdventureWorks”的新数据库，然后基于前面定义的 parquet 文件的路径在该数据库中创建名为“ProductsExternal”的外部表 。 随后会显示关于表属性的说明。 请注意，Location 属性是指定的路径。

2. 添加新的代码单元格，然后输入并运行以下代码：

    ```sql
    %%sql

    USE AdventureWorks;

    SELECT * FROM ProductsExternal;
    ```

    该代码使用 SQL 将上下文切换到 AdventureWorks 数据库（该数据库不返回任何数据），然后查询 ProductsExternal 表（返回包含 Delta Lake 表中的产品数据的结果集） 。

### 创建托管表

1. 在新的代码单元格中，添加并运行以下代码：

    ```Python
    df.write.format("delta").saveAsTable("AdventureWorks.ProductsManaged")
    spark.sql("DESCRIBE EXTENDED AdventureWorks.ProductsManaged").show(truncate=False)
    ```

    该代码基于你最初从 products.csv 文件加载的 DataFrame（在更新产品 771 的价格之前）创建名为 ProductsManaged 的托管表 。 你不需要为表使用的 parquet 文件指定路径，在 Hive 元存储中系统会为你管理该路径，并将其显示在表说明的 Location 属性中（在 files/synapse/workspaces/synapsexxxxxxx/warehouse 路径中） 。

2. 添加新的代码单元格，然后输入并运行以下代码：

    ```sql
    %%sql

    USE AdventureWorks;

    SELECT * FROM ProductsManaged;
    ```

    该代码使用 SQL 查询 ProductsManaged 表。

### 比较外部表和托管表

1. 在新的代码单元格中，添加并运行以下代码：

    ```sql
    %%sql

    USE AdventureWorks;

    SHOW TABLES;
    ```

    此代码列出 AdventureWorks 数据库中的表。

2. 按如下所示修改代码单元格，然后运行它：

    ```sql
    %%sql

    USE AdventureWorks;

    DROP TABLE IF EXISTS ProductsExternal;
    DROP TABLE IF EXISTS ProductsManaged;
    ```

    此代码会从元存储中删除表。

3. 返回到“files”选项卡并查看“files/delta/products-delta”文件夹 。 请注意，数据文件仍存在于此位置。 删除外部表的操作已从元存储中删除该表，但数据文件保持不变。
4. 查看 files/synapse/workspaces/synapsexxxxxxx/warehouse 文件夹，并注意没有对应于 ProductsManaged 表数据的文件夹 。 删除托管表的操作会从元存储中删除该表，同时删除该表的数据文件。

### 使用 SQL 创建表

1. 添加新的代码单元格，然后输入并运行以下代码：

    ```sql
    %%sql

    USE AdventureWorks;

    CREATE TABLE Products
    USING DELTA
    LOCATION '/delta/products-delta';
    ```

2. 添加新的代码单元格，然后输入并运行以下代码：

    ```sql
    %%sql

    USE AdventureWorks;

    SELECT * FROM Products;
    ```

    观察是否为现有的 Delta Lake 表文件夹创建了新的目录表，这反映了以前所做的更改。

## 使用 Delta 表对数据进行流式处理

Delta Lake 支持流式处理数据。 Delta 表可以是接收器，也可以是使用 Spark 结构化流式处理 API 创建的数据流的数据源 。 在此示例中，你将使用 Delta 表作为模拟物联网 (IoT) 方案中部分流式处理数据的接收器。

1. 返回到“笔记本 1”选项卡并添加新的代码单元格。 然后，在新单元格中添加以下代码并运行它：

    ```python
    from notebookutils import mssparkutils
    from pyspark.sql.types import *
    from pyspark.sql.functions import *

    # Create a folder
    inputPath = '/data/'
    mssparkutils.fs.mkdirs(inputPath)

    # Create a stream that reads data from the folder, using a JSON schema
    jsonSchema = StructType([
    StructField("device", StringType(), False),
    StructField("status", StringType(), False)
    ])
    iotstream = spark.readStream.schema(jsonSchema).option("maxFilesPerTrigger", 1).json(inputPath)

    # Write some event data to the folder
    device_data = '''{"device":"Dev1","status":"ok"}
    {"device":"Dev1","status":"ok"}
    {"device":"Dev1","status":"ok"}
    {"device":"Dev2","status":"error"}
    {"device":"Dev1","status":"ok"}
    {"device":"Dev1","status":"error"}
    {"device":"Dev2","status":"ok"}
    {"device":"Dev2","status":"error"}
    {"device":"Dev1","status":"ok"}'''
    mssparkutils.fs.put(inputPath + "data.txt", device_data, True)
    print("Source stream created...")
    ```

    确保打印消息“已创建源流...”。 刚刚运行的代码创建了一个基于文件夹的流数据源，其中保存了一些数据，表示来自虚拟 IoT 设备的读数。

2. 在新的代码单元格中，添加并运行以下代码：

    ```python
    # Write the stream to a delta table
    delta_stream_table_path = '/delta/iotdevicedata'
    checkpointpath = '/delta/checkpoint'
    deltastream = iotstream.writeStream.format("delta").option("checkpointLocation", checkpointpath).start(delta_stream_table_path)
    print("Streaming to delta sink...")
    ```

    此代码以 Delta 格式写入流式处理设备数据。

3. 在新的代码单元格中，添加并运行以下代码：

    ```python
    # Read the data in delta format into a dataframe
    df = spark.read.format("delta").load(delta_stream_table_path)
    display(df)
    ```

    此代码将 Delta 格式的流式处理数据读入数据帧。 请注意，用于加载流式处理数据的代码与用于从 Delta 文件夹加载静态数据的代码没有什么不同。

4. 在新的代码单元格中，添加并运行以下代码：

    ```python
    # create a catalog table based on the streaming sink
    spark.sql("CREATE TABLE IotDeviceData USING DELTA LOCATION '{0}'".format(delta_stream_table_path))
    ```

    此代码基于 Delta 文件夹在默认数据库中创建名为 IotDeviceData 的目录表 。 同样，此代码与用于非流式处理数据的代码相同。

5. 在新的代码单元格中，添加并运行以下代码：

    ```sql
    %%sql

    SELECT * FROM IotDeviceData;
    ```

    此代码查询 IotDeviceData 表，其中包含来自流式处理源的设备数据。

6. 在新的代码单元格中，添加并运行以下代码：

    ```python
    # Add more data to the source stream
    more_data = '''{"device":"Dev1","status":"ok"}
    {"device":"Dev1","status":"ok"}
    {"device":"Dev1","status":"ok"}
    {"device":"Dev1","status":"ok"}
    {"device":"Dev1","status":"error"}
    {"device":"Dev2","status":"error"}
    {"device":"Dev1","status":"ok"}'''

    mssparkutils.fs.put(inputPath + "more-data.txt", more_data, True)
    ```

    此代码将更多假设的设备数据写入流式处理源。

7. 在新的代码单元格中，添加并运行以下代码：

    ```sql
    %%sql

    SELECT * FROM IotDeviceData;
    ```

    此代码再次查询 IotDeviceData 表，该表现在应包括已添加到流式处理源的其他数据。

8. 在新的代码单元格中，添加并运行以下代码：

    ```python
    deltastream.stop()
    ```

    此代码停止流。

## 从无服务器 SQL 池查询 Delta 表

除了 Spark 池，Azure Synapse Analytics 还包括内置的无服务器 SQL 池。 可以使用此池中的关系数据库引擎通过 SQL 查询 Delta 表。

1. 在“files”选项卡中浏览到“files/delta”文件夹 。
2. 选择 products-delta 文件夹，然后在工具栏上的“新建 SQL 脚本”下拉列表中选择“选择前 100 行”  。
3. 在“选择前 100 行”窗格中，选择“文件类型”列表中的“Delta 格式”，然后选择“应用”   。
4. 查看生成的 SQL 代码，该代码应类似于：

    ```sql
    -- This is auto-generated code
    SELECT
        TOP 100 *
    FROM
        OPENROWSET(
            BULK 'https://datalakexxxxxxx.dfs.core.windows.net/files/delta/products-delta/',
            FORMAT = 'DELTA'
        ) AS [result]
    ```

5. 使用“&#9655; 运行”图标运行脚本并查看结果。 结果应如下所示：

    | ProductID | ProductName | 类别 | ListPrice |
    | -- | -- | -- | -- |
    | 771 | Mountain-100 Silver, 38 | 山地自行车 | 3059.991 |
    | 772 | Mountain-100 Silver, 42 | 山地自行车 | 3399.9900 |
    | ... | ... | ... | ... |

    这演示了如何使用无服务器 SQL 池来查询使用 Spark 创建的 Delta 格式文件，并将结果用于报告或分析工作。

6. 将查询替换为以下 SQL 代码：

    ```sql
    USE AdventureWorks;

    SELECT * FROM Products;
    ```

7. 运行代码，并观察还可以使用无服务器 SQL 池查询定义 Spark 元存储的目录表中的 Delta Lake 数据。

## 删除 Azure 资源

你已完成对 Azure Synapse Analytics 的探索，现在应删除已创建的资源，以避免产生不必要的 Azure 成本。

1. 关闭 Synapse Studio 浏览器选项卡并返回到 Azure 门户。
2. 在 Azure 门户的“主页”上，选择“资源组”。
3. 选择 Synapse Analytics 工作区的 dp203-*xxxxxxx* 资源组（不是受管理资源组），并确认它包含 Synapse 工作区、存储帐户和工作区的 Spark 池。
4. 在资源组的“概述”页的顶部，选择“删除资源组”。
5. 输入 dp203-xxxxxxx 资源组名称以确认要删除该资源组，然后选择“删除” 。

    几分钟后，将删除 Azure Synapse 工作区资源组及其关联的托管工作区资源组。
