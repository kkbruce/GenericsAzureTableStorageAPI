using System;
using NUnit.Framework;
using System.Collections.Generic;
using System.Linq;
using Microsoft.WindowsAzure.Storage.Table;

namespace KKLib.Tests
{
    [TestFixture()]
    public class TableCacheTests
    {
        private string tableConn;
        private string tableName;
        private string partitionKey;

        [SetUp()]
        public void Init()
        {
            //!++執行測試前，請先啟動本機儲存體模擬器
            this.tableConn = "UseDevelopmentStorage=true";
            this.tableName = "testTable";
            this.partitionKey = "testKey";
        }

        [TearDown()]
        public void Cleanup()
        {
            var table = new TableStorage<TestEntity>(tableConn, tableName);
            table.DeleteTable();
        }

        [Test()]
        public void Insert_插入實體至table_應取得OK回應()
        {
            var data = new TestEntity(partitionKey, "InsertTest") { Value = "InsertTest Data 1." };
            var expected = "OK";

            var table = new TableStorage<TestEntity>(tableConn, tableName);
            var actual = table.Insert(data);

            Assert.AreEqual(expected, actual);
        }

        [Test()]
        public void InsertOrReplace_先插入實體並進行實體覆寫_應取得OK回應()
        {
            var expected = "OK";

            // Insert Test
            var data = new TestEntity(partitionKey, "InsertTest2") { Value = "InsertTest2 Data 1." };
            var table = new TableStorage<TestEntity>(tableConn, tableName);
            var actual1 = table.InsertOrReplace(data);
            Assert.AreEqual(expected, actual1);

            // Replace Text
            var data2 = new TestEntity(partitionKey, "InsertTest2") { Value = "[Replace]InsertTest2 Data 1." };
            var actual2 = table.InsertOrReplace(data2);

            Assert.AreEqual(expected, actual2);
        }

        [Test()]
        public void Insert_進行BatchInsert_應取得OK回應()
        {
            var data1 = new TestEntity(partitionKey, "batch1") { Value = "Batch Test Data 1." };
            var data2 = new TestEntity(partitionKey, "batch2") { Value = "Batch Test Data 2." };
            var data3 = new TestEntity(partitionKey, "batch3") { Value = "Batch Test Data 3." };
            var datas = new List<TestEntity>();
            datas.Add(data1);
            datas.Add(data2);
            datas.Add(data3);
            var expected = "OK";

            var table = new TableStorage<TestEntity>(tableConn, tableName);
            var actual = table.BatchInsert(datas);

            Assert.AreEqual(expected, actual);
        }

        /// <summary>
        /// Ref https://docs.microsoft.com/zh-tw/azure/cosmos-db/table-storage-how-to-use-dotnet#insert-a-batch-of-entities
        /// </summary>
        [Test()]
        public void Insert_產生超過100實體進行BatchInsert_應取得例外回應()
        {
            // C# 7 Support
            void Validate()
            {
                var data = new List<TestEntity>();
                for (int i = 1; i <= 101; i++)
                {
                    var testEntity = new TestEntity(partitionKey, i.ToString()) { Value = i.ToString() };
                    data.Add(testEntity);
                }

                var table = new TableStorage<TestEntity>(tableConn, tableName);
                var actual = table.BatchInsert(data);
            }

            Assert.Throws(typeof(ArgumentOutOfRangeException), Validate);
        }

        [Test()]
        public void GetAll_取回所有實體集合()
        {
            var expected = 3;

            TableStorage<TestEntity> table;
            AddTest(out table);
            var datas  = table.GetAll();

            Assert.AreEqual(expected, datas.Count());
        }

        [Test()]
        public void GetAllAsync_非同步取回所有實體集合()
        {
            var expected = 3;

            TableStorage<TestEntity> table;
            AddTest(out table);
            var datas = table.GetAllAsync();

            Assert.AreEqual(expected, datas.Result.Count());
        }

        [Test()]
        public void GetRange_取回某一Rowkey區間實體集合()
        {
            var expected = 3;

            TableStorage<TestEntity> table;
            AddTest(out table);
            // 小於的比較 "b"atch，所以取 "c"
            var datas = table.GetRange("c");

            Assert.AreEqual(expected, datas.Count());
        }

        [Test()]
        public void GetSingle_取回單一實體()
        {
            var expected = "Batch Test Data 1.";

            TableStorage<TestEntity> table;
            AddTest(out table);

            var data = new TestEntity(partitionKey, "batch1");
            var actual = table.GetSingle(data);

            Assert.AreEqual(expected, actual.Value);
        }

        [Test()]
        public void Delete_刪除實體()
        {
            var data = new TestEntity(partitionKey, "batch1");
            var expected = "OK";

            TableStorage<TestEntity> table;
            AddTest(out table);
            var actual = table.Delete(data);

            Assert.AreEqual(expected, actual);
        }

        [Test()]
        public void DeleteTable_刪除表格()
        {
            var expected = "OK";

            var table = new TableStorage<TestEntity>(tableConn, tableName);
            var actual = table.DeleteTable();

            Assert.AreEqual(expected, actual);
        }

        private void AddTest(out TableStorage<TestEntity> table)
        {
            var data1 = new TestEntity(partitionKey, "batch1") { Value = "Batch Test Data 1." };
            var data2 = new TestEntity(partitionKey, "batch2") { Value = "Batch Test Data 2." };
            var data3 = new TestEntity(partitionKey, "batch3") { Value = "Batch Test Data 3." };
            List<TestEntity> datas = new List<TestEntity>();
            datas.Add(data1);
            datas.Add(data2);
            datas.Add(data3);
            table = new TableStorage<TestEntity>(tableConn, tableName, partitionKey);
            table.BatchInsert(datas);
        }
    }

    /// <summary>
    /// 類別需繼承 TableEntity，並指定 PartitionKey 與 RowKey
    /// </summary>
    class TestEntity : TableEntity
    {

        public TestEntity(string PartitionKey, string RowKey)
        {
            this.PartitionKey = PartitionKey;
            this.RowKey = RowKey;
        }

        // 規定
        public TestEntity() { }

        public string Value { get; set; }

    }
}