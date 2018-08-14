using Microsoft.VisualStudio.TestTools.UnitTesting;
using Silk.Data.SQL.Expressions;
using System.Threading.Tasks;

namespace Silk.Data.SQL.ProviderTests
{
	public partial class SqlProviderTests
	{
		[TestMethod]
		public virtual async Task Select_SelectValues()
		{
			using (var queryResult = await DataProvider.ExecuteReaderAsync(
				QueryExpression.Select(
					new[] { QueryExpression.Value(1), QueryExpression.Value("Hello World") }
				)))
			{
				Assert.IsTrue(queryResult.HasRows);
				Assert.IsTrue(await queryResult.ReadAsync());
				Assert.AreEqual(1, queryResult.GetInt32(0));
				Assert.AreEqual("Hello World", queryResult.GetString(1));
			}
		}

		[TestMethod]
		public virtual async Task Select_AliasValues()
		{
			using (var queryResult = await DataProvider.ExecuteReaderAsync(
				QueryExpression.Select(
					new[] {
						QueryExpression.Alias(QueryExpression.Value(1), "int"),
						QueryExpression.Alias(QueryExpression.Value("Hello World"), "str")
					}
				)))
			{
				Assert.IsTrue(queryResult.HasRows);
				Assert.AreEqual("int", queryResult.GetName(0));
				Assert.AreEqual("str", queryResult.GetName(1));
				Assert.AreEqual(0, queryResult.GetOrdinal("int"));
				Assert.AreEqual(1, queryResult.GetOrdinal("str"));
				Assert.IsTrue(await queryResult.ReadAsync());
				Assert.AreEqual(1, queryResult.GetInt32(0));
				Assert.AreEqual("Hello World", queryResult.GetString(1));
			}
		}

		[TestMethod]
		public virtual async Task Select_MultipleResultSets()
		{
			using (var queryResult = await DataProvider.ExecuteReaderAsync(
				new CompositeQueryExpression(
					QueryExpression.Select(new[] { QueryExpression.Value(1) }),
					QueryExpression.Select(new[] { QueryExpression.Value(2) }),
					QueryExpression.Select(new[] { QueryExpression.Value(3) })
				)))
			{
				Assert.IsTrue(queryResult.HasRows);
				Assert.IsTrue(await queryResult.ReadAsync());
				Assert.AreEqual(1, queryResult.GetInt32(0));

				Assert.IsTrue(await queryResult.NextResultAsync());
				Assert.IsTrue(queryResult.HasRows);
				Assert.IsTrue(await queryResult.ReadAsync());
				Assert.AreEqual(2, queryResult.GetInt32(0));

				Assert.IsTrue(await queryResult.NextResultAsync());
				Assert.IsTrue(queryResult.HasRows);
				Assert.IsTrue(await queryResult.ReadAsync());
				Assert.AreEqual(3, queryResult.GetInt32(0));
			}
		}

		[TestMethod]
		public virtual async Task Select_FromSubSelect()
		{
			//  some database engines require the subquery to be aliased
			var subQueryAlias = QueryExpression.Alias(
				QueryExpression.Select(
						new[] { QueryExpression.Alias(QueryExpression.Value(1), "value") }
						),
				"subQuery"
				);
			using (var queryResult = await DataProvider.ExecuteReaderAsync(
				QueryExpression.Select(
					new[] { QueryExpression.All() },
					from: subQueryAlias
				)))
			{
				Assert.IsTrue(queryResult.HasRows);
				Assert.IsTrue(await queryResult.ReadAsync());
				Assert.AreEqual(1, queryResult.GetInt32(0));
			}
		}

		[TestMethod]
		public virtual async Task Select_WithOrderBy()
		{
			using (var tempTable = await Select_CreatePopulatedSelectTable())
			using (var queryResult = await DataProvider.ExecuteReaderAsync(
				QueryExpression.Select(
					new[] { QueryExpression.All() },
					from: QueryExpression.Table(tempTable.TableName),
					orderBy: new[] { QueryExpression.Descending(QueryExpression.Column("Data")) }
				)))
			{
				Assert.IsTrue(queryResult.HasRows);

				Assert.IsTrue(await queryResult.ReadAsync());
				Assert.AreEqual(3, queryResult.GetInt32(1));
				Assert.IsTrue(await queryResult.ReadAsync());
				Assert.AreEqual(3, queryResult.GetInt32(1));

				Assert.IsTrue(await queryResult.ReadAsync());
				Assert.AreEqual(2, queryResult.GetInt32(1));
				Assert.IsTrue(await queryResult.ReadAsync());
				Assert.AreEqual(2, queryResult.GetInt32(1));

				Assert.IsTrue(await queryResult.ReadAsync());
				Assert.AreEqual(1, queryResult.GetInt32(1));
				Assert.IsTrue(await queryResult.ReadAsync());
				Assert.AreEqual(1, queryResult.GetInt32(1));
			}
		}

		[TestMethod]
		public virtual async Task SelectWithGroupBy()
		{
			using (var tempTable = await Select_CreatePopulatedSelectTable())
			using (var queryResult = await DataProvider.ExecuteReaderAsync(
				QueryExpression.Select(
					new[] { QueryExpression.Column("Data") },
					from: QueryExpression.Table(tempTable.TableName),
					orderBy: new[] { QueryExpression.Column("Data") },
					groupBy: new[] { QueryExpression.Column("Data") }
				)))
			{
				Assert.IsTrue(queryResult.HasRows);

				Assert.IsTrue(await queryResult.ReadAsync());
				Assert.AreEqual(1, queryResult.GetInt32(0));

				Assert.IsTrue(await queryResult.ReadAsync());
				Assert.AreEqual(2, queryResult.GetInt32(0));

				Assert.IsTrue(await queryResult.ReadAsync());
				Assert.AreEqual(3, queryResult.GetInt32(0));
			}
		}

		[TestMethod]
		public virtual async Task Select_WithLimit()
		{
			using (var tempTable = await Select_CreatePopulatedSelectTable())
			using (var queryResult = await DataProvider.ExecuteReaderAsync(
				QueryExpression.Select(
					new[] { QueryExpression.All() },
					from: QueryExpression.Table(tempTable.TableName),
					orderBy: new[] { QueryExpression.Column("Data") },
					limit: QueryExpression.Value(1)
				)))
			{
				Assert.IsTrue(queryResult.HasRows);

				Assert.IsTrue(await queryResult.ReadAsync());
				Assert.AreEqual(1, queryResult.GetInt32(1));

				Assert.IsFalse(await queryResult.ReadAsync());
			}
		}

		[TestMethod]
		public virtual async Task Select_WithOffset()
		{
			using (var tempTable = await Select_CreatePopulatedSelectTable())
			using (var queryResult = await DataProvider.ExecuteReaderAsync(
				QueryExpression.Select(
					new[] { QueryExpression.All() },
					from: QueryExpression.Table(tempTable.TableName),
					orderBy: new[] { QueryExpression.Column("Data") },
					offset: QueryExpression.Value(2)
				)))
			{
				Assert.IsTrue(queryResult.HasRows);

				Assert.IsTrue(await queryResult.ReadAsync());
				Assert.AreEqual(2, queryResult.GetInt32(1));
			}
		}

		[TestMethod]
		public virtual async Task Select_WithOffsetAndLimit()
		{
			using (var tempTable = await Select_CreatePopulatedSelectTable())
			using (var queryResult = await DataProvider.ExecuteReaderAsync(
				QueryExpression.Select(
					new[] { QueryExpression.All() },
					from: QueryExpression.Table(tempTable.TableName),
					orderBy: new[] { QueryExpression.Column("Data") },
					offset: QueryExpression.Value(2),
					limit: QueryExpression.Value(1)
				)))
			{
				Assert.IsTrue(queryResult.HasRows);

				Assert.IsTrue(await queryResult.ReadAsync());
				Assert.AreEqual(2, queryResult.GetInt32(1));

				Assert.IsFalse(await queryResult.ReadAsync());
			}
		}

		[TestMethod]
		public virtual async Task Select_WithWhere()
		{
			using (var tempTable = await Select_CreatePopulatedSelectTable())
			using (var queryResult = await DataProvider.ExecuteReaderAsync(
				QueryExpression.Select(
					new[] { QueryExpression.All() },
					from: QueryExpression.Table(tempTable.TableName),
					where: QueryExpression.Compare(QueryExpression.Column("Data"), ComparisonOperator.AreEqual, QueryExpression.Value(3)),
					limit: QueryExpression.Value(1)
				)))
			{
				Assert.IsTrue(queryResult.HasRows);

				Assert.IsTrue(await queryResult.ReadAsync());
				Assert.AreEqual(3, queryResult.GetInt32(1));

				Assert.IsFalse(await queryResult.ReadAsync());
			}
		}

		protected virtual async Task<TemporaryTestTable> Select_CreatePopulatedSelectTable()
		{
			var ret = await DataTestHelpers.CreateAutoIncrementTable(DataProvider);
			await DataProvider.ExecuteNonQueryAsync(
				QueryExpression.Insert(
					ret.TableName,
					new[] { "Data" },
					new object[] { 1 },
					new object[] { 1 },
					new object[] { 2 },
					new object[] { 2 },
					new object[] { 3 },
					new object[] { 3 }
					)
				);
			return ret;
		}
	}
}
