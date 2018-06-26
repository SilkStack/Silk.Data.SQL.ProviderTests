using Microsoft.VisualStudio.TestTools.UnitTesting;
using Silk.Data.SQL.Expressions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Silk.Data.SQL.ProviderTests
{
	public partial class SqlProviderTests
	{
		[TestMethod]
		public async Task Functions_Random()
		{
			using (var queryResult = await DataProvider.ExecuteReaderAsync(
				QueryExpression.Select(new[] { QueryExpression.Random(), QueryExpression.Random(), QueryExpression.Random() })
				))
			{
				Assert.IsTrue(queryResult.HasRows);
				Assert.IsTrue(await queryResult.ReadAsync());
				//  this will throw an exception on failure
				var values = new[] {
					queryResult.GetInt64(0),
					queryResult.GetInt64(1),
					queryResult.GetInt64(2)
					};
				Assert.AreEqual(3, values.GroupBy(q => q).Count());
			}
		}

		[TestMethod]
		public async Task Functions_Count()
		{
			//  some database engines require the subquery to be aliased
			using (var tempTable = await Select_CreatePopulatedSelectTable())
			{
				using (var queryResult = await DataProvider.ExecuteReaderAsync(
					QueryExpression.Select(
						new[] { QueryExpression.CountFunction(QueryExpression.All()) },
						from: QueryExpression.Table(tempTable.TableName)
					)))
				{
					Assert.IsTrue(queryResult.HasRows);
					Assert.IsTrue(queryResult.Read());
					Assert.AreEqual(6, queryResult.GetInt32(0));
				}
			}
		}

		[TestMethod]
		public async Task Functions_CountDistinct()
		{
			using (var tempTable = await DataTestHelpers.CreateAutoIncrementTable(DataProvider))
			{
				await DataProvider.ExecuteNonQueryAsync(QueryExpression.Insert(
					tempTable.TableName,
					new[] { "Data" },
					new object[] { 10 },
					new object[] { 10 },
					new object[] { 15 },
					new object[] { 20 }
					));


				using (var queryResult = await DataProvider.ExecuteReaderAsync(
					QueryExpression.Select(
						new[] { QueryExpression.CountFunction(
						QueryExpression.Distinct(QueryExpression.Column("Data"))
						) },
						from: QueryExpression.Table(tempTable.TableName)
					)))
				{
					Assert.IsTrue(queryResult.HasRows);
					Assert.IsTrue(await queryResult.ReadAsync());
					Assert.AreEqual(3, queryResult.GetInt32(0));
				}
			}
		}
	}
}
