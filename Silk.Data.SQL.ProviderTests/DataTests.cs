using Microsoft.VisualStudio.TestTools.UnitTesting;
using Silk.Data.SQL.Expressions;
using Silk.Data.SQL.Providers;
using Silk.Data.SQL.Queries;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Silk.Data.SQL.ProviderTests
{
	public partial class SqlProviderTests
	{
		[TestMethod]
		public async Task Data_InsertAutoIncrementRow()
		{
			using (var table = await DataTestHelpers.CreateAutoIncrementTable(DataProvider))
			{
				await DataProvider.ExecuteNonQueryAsync(
					QueryExpression.Insert(DataTestHelpers.AUTOINC_TABLE_NAME, new[] { "Data" }, new object[] { 1 })
					);
			}
		}

		[TestMethod]
		public async Task Data_InsertAutoIncrementRowAndSelectLastId()
		{
			using (var table = await DataTestHelpers.CreateAutoIncrementTable(DataProvider))
			{
				using (var transaction = await DataProvider.CreateTransactionAsync())
				{
					await transaction.ExecuteNonQueryAsync(
						QueryExpression.Insert(DataTestHelpers.AUTOINC_TABLE_NAME, new[] { "Data" }, new object[] { 1 })
						);
					using (var queryResult = await transaction.ExecuteReaderAsync(
						QueryExpression.Select(new[] { QueryExpression.LastInsertIdFunction() })
						))
					{
						Assert.IsTrue(queryResult.HasRows);
						Assert.IsTrue(await queryResult.ReadAsync());
						Assert.AreEqual(1, queryResult.GetInt32(0));
					}

					await transaction.ExecuteNonQueryAsync(
						QueryExpression.Insert(DataTestHelpers.AUTOINC_TABLE_NAME, new[] { "Data" }, new object[] { 1 })
						);
					using (var queryResult = await transaction.ExecuteReaderAsync(
						QueryExpression.Select(new[] { QueryExpression.LastInsertIdFunction() }, QueryExpression.Table(DataTestHelpers.AUTOINC_TABLE_NAME))
						))
					{
						Assert.IsTrue(queryResult.HasRows);
						Assert.IsTrue(await queryResult.ReadAsync());
						Assert.AreEqual(2, queryResult.GetInt32(0));
					}
				}
			}
		}

		[TestMethod]
		public async Task Data_StoreString()
		{
			await Data_TestStoreDataType(SqlDataType.Text(), "Test Data");
		}

		[TestMethod]
		public async Task Data_StoreLengthString()
		{
			await Data_TestStoreDataType(SqlDataType.Text(255), "Test Data");
		}

		[TestMethod]
		public async Task Data_StoreBit()
		{
			await Data_TestStoreDataType(SqlDataType.Bit(), true);
		}

		[TestMethod]
		public async Task Data_StoreTinyInt()
		{
			await Data_TestStoreDataType(SqlDataType.TinyInt(), byte.MaxValue);
		}

		[TestMethod]
		public async Task Data_StoreSmallInt()
		{
			await Data_TestStoreDataType(SqlDataType.SmallInt(), short.MaxValue);
		}

		[TestMethod]
		public async Task Data_StoreInt()
		{
			await Data_TestStoreDataType(SqlDataType.Int(), int.MaxValue);
		}

		[TestMethod]
		public async Task Data_StoreBigInt()
		{
			await Data_TestStoreDataType(SqlDataType.BigInt(), long.MaxValue);
		}

		[TestMethod]
		public async Task Data_StoreDateTime()
		{
			//  for DateTime values to be equal they need to have the same DateTimeKind
			//  database providers should return a DateTime value with a DateTimeKind of Unspecified.
			await Data_TestStoreDataType(SqlDataType.DateTime(), new DateTime(2018, 1, 1, 0, 0, 0, 0, DateTimeKind.Unspecified));
		}

		[TestMethod]
		public async Task Data_StoreDate()
		{
			//  for DateTime values to be equal they need to have the same DateTimeKind
			//  database providers should return a DateTime value with a DateTimeKind of Unspecified.
			await Data_TestStoreDataType(SqlDataType.Date(), new DateTime(2018, 1, 1, 0, 0, 0, 0, DateTimeKind.Unspecified));
		}

		[TestMethod]
		public async Task Data_StoreFloat()
		{
			await Data_TestStoreDataType(SqlDataType.Float(), float.MaxValue);
		}

		[TestMethod]
		public async Task Data_StoreDoublePrecisionFloat()
		{
			await Data_TestStoreDataType(SqlDataType.Float(SqlDataType.DOUBLE_MAX_PRECISION), double.MaxValue);
		}

		[TestMethod]
		public async Task Data_StoreWideDecimal()
		{
			//  test the full decimal storage capabilities of the engine using the max decimal value
			await Data_TestStoreDataType(SqlDataType.Decimal(38), decimal.MaxValue);
		}

		[TestMethod]
		public async Task Data_StoreShortDecimal()
		{
			//  test the "short" decimal storage capabilities, 15 digits
			var value = 123451234.512345m;
			await Data_TestStoreDataType(SqlDataType.Decimal(15, 6), value);
		}

		[TestMethod]
		public async Task Data_StoreGuid()
		{
			await Data_TestStoreDataType(SqlDataType.Guid(), Guid.NewGuid());
		}

		protected async Task Data_TestStoreDataType<T>(SqlDataType dataType, T value)
		{
			using (var dataTable = await DataTestHelpers.CreateDataTable(dataType, DataProvider))
			{
				await DataProvider.ExecuteNonQueryAsync(QueryExpression.Insert(
					dataTable.TableName, new[] { "Data" }, new object[] { value }
					));

				using (var queryResult = await DataProvider.ExecuteReaderAsync(QueryExpression.Select(
					new[] { QueryExpression.All() },
					QueryExpression.Table(dataTable.TableName)
					)))
				{
					Assert.IsTrue(queryResult.HasRows);
					Assert.IsTrue(await queryResult.ReadAsync());
					var retrievedValue = DataTestHelpers.ReadFromResult<T>(queryResult, 0);
					Assert.AreEqual(value, retrievedValue);
				}
			}
		}

		protected static class DataTestHelpers
		{
			public const string AUTOINC_TABLE_NAME = "AutoIncrementTestTable";

			private static readonly Dictionary<Type, Func<QueryResult, int, object>> _typeReaders =
				new Dictionary<Type, Func<QueryResult, int, object>>()
				{
					{ typeof(bool), (q,o) => q.GetBoolean(o) },
					{ typeof(byte), (q,o) => q.GetByte(o) },
					{ typeof(short), (q,o) => q.GetInt16(o) },
					{ typeof(int), (q,o) => q.GetInt32(o) },
					{ typeof(long), (q,o) => q.GetInt64(o) },
					{ typeof(float), (q,o) => q.GetFloat(o) },
					{ typeof(double), (q,o) => q.GetDouble(o) },
					{ typeof(decimal), (q,o) => q.GetDecimal(o) },
					{ typeof(string), (q,o) => q.GetString(o) },
					{ typeof(Guid), (q,o) => q.GetGuid(o) },
					{ typeof(DateTime), (q,o) => q.GetDateTime(o) },
				};

			public static T ReadFromResult<T>(QueryResult queryResult, int ord)
			{
				if (!_typeReaders.TryGetValue(typeof(T), out var readFunc))
					throw new Exception($"No reader for type '{typeof(T)}'.");
				return (T)readFunc(queryResult, ord);
			}

			public static async Task<TemporaryTestTable> CreateAutoIncrementTable(IDataProvider dataProvider)
			{
				await dataProvider.ExecuteNonQueryAsync(QueryExpression.CreateTable(
					AUTOINC_TABLE_NAME,
					QueryExpression.DefineColumn("Id", SqlDataType.Int(), isAutoIncrement: true, isPrimaryKey: true),
					QueryExpression.DefineColumn("Data", SqlDataType.Int())
					));
				return new TemporaryTestTable(AUTOINC_TABLE_NAME, dataProvider);
			}

			public static async Task<TemporaryTestTable> CreateDataTable(SqlDataType sqlDataType, IDataProvider dataProvider)
			{
				var tableName = $"DataTestTable_{sqlDataType.BaseType}";
				await dataProvider.ExecuteNonQueryAsync(QueryExpression.CreateTable(
					tableName,
					QueryExpression.DefineColumn("Data", sqlDataType)
					));
				return new TemporaryTestTable(tableName, dataProvider);
			}
		}
	}
}
