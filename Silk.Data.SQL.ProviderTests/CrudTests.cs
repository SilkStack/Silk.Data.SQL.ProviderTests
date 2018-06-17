using Microsoft.VisualStudio.TestTools.UnitTesting;
using Silk.Data.SQL.Expressions;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Silk.Data.SQL.ProviderTests
{
	public partial class SqlProviderTests
	{
		[TestMethod]
		public async Task Crud_InsertFailsWithMissingNonNullable()
		{
			var exceptionCaught = false;
			using (var tempTable = await DataTestHelpers.CreateDataTable(SqlDataType.Int(), DataProvider))
			{
				try
				{
					await DataProvider.ExecuteNonQueryAsync(
						QueryExpression.Insert(
							tempTable.TableName,
							new[] { "Data" },
							new object[] { null },
							new object[] { null }
							)
						);
				}
				catch (Exception)
				{
					exceptionCaught = true;
				}
				Assert.IsTrue(exceptionCaught);
			}
		}

		[TestMethod]
		public async Task Crud_Update()
		{
			using (var tempTable = await DataTestHelpers.CreateAutoIncrementTable(DataProvider))
			using (var queryResult = await DataProvider.ExecuteReaderAsync(
				QueryExpression.Transaction(
					QueryExpression.Insert(
						tempTable.TableName,
						new[] { "Id", "Data" },
						new object[] { 1, 1 }
						),
					QueryExpression.Update(
						QueryExpression.Table(tempTable.TableName),
						QueryExpression.Compare(QueryExpression.Column("Id"), ComparisonOperator.AreEqual, QueryExpression.Value(1)),
						QueryExpression.Assign(QueryExpression.Column("Data"), QueryExpression.Value(5))
						),
					QueryExpression.Select(
						new[] { QueryExpression.Column("Data") },
						from: QueryExpression.Table(tempTable.TableName),
						where: QueryExpression.Compare(QueryExpression.Column("Id"), ComparisonOperator.AreEqual, QueryExpression.Value(1))
						)
				)))
			{
				Assert.IsTrue(queryResult.HasRows);
				Assert.IsTrue(await queryResult.ReadAsync());
				Assert.AreEqual(5, queryResult.GetInt32(0));
			}
		}

		[TestMethod]
		public async Task Crud_Delete()
		{
			using (var tempTable = await DataTestHelpers.CreateAutoIncrementTable(DataProvider))
			using (var queryResult = await DataProvider.ExecuteReaderAsync(
				QueryExpression.Transaction(
					QueryExpression.Insert(
						tempTable.TableName,
						new[] { "Id", "Data" },
						new object[] { 1, 1 }
						),
					QueryExpression.Delete(
						QueryExpression.Table(tempTable.TableName),
						QueryExpression.Compare(QueryExpression.Column("Id"), ComparisonOperator.AreEqual, QueryExpression.Value(1))
						),
					QueryExpression.Select(
						new[] { QueryExpression.Column("Data") },
						from: QueryExpression.Table(tempTable.TableName),
						where: QueryExpression.Compare(QueryExpression.Column("Id"), ComparisonOperator.AreEqual, QueryExpression.Value(1))
						)
				)))
			{
				Assert.IsFalse(queryResult.HasRows);
			}
		}
	}
}
