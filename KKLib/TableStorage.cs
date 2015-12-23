using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace KKLib
{
    /// <summary>
    /// Table Storage
    /// </summary>
    public class TableStorage<T> where T : TableEntity, new()
    {
        private string tableConn;
        private string tableName;
        private string partitionKey;
        private CloudTable table;

        /// <summary>
        /// 初始化 Table 物件。
        /// </summary>
        /// <param name="tableConn">連線字串</param>
        /// <param name="tableName">存取表格名稱</param>
        public TableStorage(string tableConn, string tableName, string partitionKey = null)
        {
            this.tableConn = tableConn;
            this.tableName = tableName;
            this.partitionKey = partitionKey;

            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(tableConn);
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            table = tableClient.GetTableReference(tableName);
            table.CreateIfNotExists();
        }

        /// <summary>
        /// 新增資料至Table儲存區。
        /// 注意，不能存放重覆key值資料。
        /// </summary>
        /// <param name="entity"></param>
        /// <returns></returns>
        public string Insert(T entity)
        {
            TableOperation insertOperation = TableOperation.Insert(entity);
            try
            {
                table.Execute(insertOperation);
            }
            catch (Exception ex)
            {
                return $"Error:{ex.Message}";
            }

            return "OK";
        }

        /// <summary>
        /// 新增或覆寫資料至Table儲存區。
        /// </summary>
        /// <param name="entity"></param>
        /// <returns></returns>
        public string InsertOrReplace(T entity)
        {
            try
            {
                TableResult retrievedResult = GetServerTableResult(entity);
                T updateEntity = (T)retrievedResult.Result;

                if (updateEntity != null)
                {
                    // Replace
                    updateEntity = entity;
                    TableOperation insertOrReplaceOperation = TableOperation.InsertOrReplace(updateEntity);
                    try
                    {
                        table.Execute(insertOrReplaceOperation);
                        return "OK";
                    }
                    catch (Exception ex)
                    {
                        return $"Error:{ex.Message}";
                    }
                }
                else
                {
                    // Insert
                    TableOperation insertOrReplaceOperation = TableOperation.InsertOrReplace(entity);
                    try
                    {
                        table.Execute(insertOrReplaceOperation);
                        return "OK";
                    }
                    catch (Exception ex)
                    {
                        return $"Error:{ex.Message}";
                    }
                }
            }
            catch (Exception ex)
            {
                return $"Error:{ex.Message}";
            }
        }

        /// <summary>
        /// 批次新增資料至Table儲存區。
        /// </summary>
        /// <param name="entities">欲快取的集合</param>
        /// <returns></returns>
        public string BatchInsert(IEnumerable<T> entities)
        {
            TableBatchOperation batchOperation = new TableBatchOperation();
            foreach (var cache in entities)
            {
                batchOperation.Insert(cache);
            }

            try
            {
                table.ExecuteBatch(batchOperation);
            }
            catch (Exception ex)
            {
                return $"Error:{ex.Message}";
            }

            return "OK";
        }

        /// <summary>
        /// 取得 table 所有集合。
        /// </summary>
        /// <returns></returns>
        public IEnumerable<T> GetAll()
        {
            TableQuery<T> query = new TableQuery<T>().Where(
                TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey));
            return table.ExecuteQuery(query);
        }

        /// <summary>
        /// 以非同步取得大量(1000個以上)實體。
        /// </summary>
        /// <returns></returns>
        public async Task<IEnumerable<T>> GetAllAsync()
        {
            TableQuery<T> tableQuery = new TableQuery<T>();
            TableContinuationToken continuationToken = null;
            do
            {
                // 接收一個段落資料(1000以上實體)
                TableQuerySegment<T> tableQueryResult =
                    await table.ExecuteQuerySegmentedAsync(tableQuery, continuationToken);
                continuationToken = tableQueryResult.ContinuationToken;
                return tableQueryResult.Results;
            } while (continuationToken != null);
        }

        /// <summary>
        /// 以 rowkey 為關鍵字，取得相關集合。
        /// </summary>
        /// <param name="rowkey"></param>
        /// <returns></returns>
        public IEnumerable<T> GetRange(string rowkey)
        {
            TableQuery<T> rangeQuery = new TableQuery<T>().Where(
                TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey),
                    TableOperators.And,
                    TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThan, rowkey)));

            return table.ExecuteQuery(rangeQuery);
        }

        /// <summary>
        /// 取得單一實體
        /// </summary>
        /// <param name="entity"></param>
        /// <returns></returns>
        public T GetSingle(T entity)
        {
            TableResult retrievedResult = GetServerTableResult(entity);
            T singleData = (T)retrievedResult.Result;
            if (singleData != null)
            {
                return singleData;
            }
            else
            {
                return null;
            }
        }

        /// <summary>
        /// 刪除實體
        /// </summary>
        /// <param name="entity"></param>
        /// <returns></returns>
        public string Delete(T entity)
        {
            TableResult retrievedResult = GetServerTableResult(entity);
            T deleteEntity = (T)retrievedResult.Result;
            if (deleteEntity != null)
            {
                TableOperation deleteOperation = TableOperation.Delete(deleteEntity);
                table.Execute(deleteOperation);
                return "OK";
            }
            else
            {
                return "Error: deleteEntity is null.";
            }
        }

        /// <summary>
        /// 刪除資料表。
        /// 在刪除後一段時間內，將無法重新建立同名表格。
        /// </summary>
        /// <returns></returns>
        public string DeleteTable()
        {
            try
            {
                table.DeleteIfExists();
                return "OK";
            }
            catch (Exception ex)
            {
                return $"Error:{ex.Message}";
            }

        }

        /// <summary>
        /// 取回伺服器上的Table結果集
        /// </summary>
        /// <param name="tableData"></param>
        /// <returns></returns>
        private TableResult GetServerTableResult(T tableData)
        {
            TableOperation retrieveOperation = TableOperation.Retrieve<T>(tableData.PartitionKey, tableData.RowKey.ToString());
            TableResult retrievedResult = table.Execute(retrieveOperation);
            return retrievedResult;
        }
    }
}
