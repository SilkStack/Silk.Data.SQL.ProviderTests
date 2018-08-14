using Microsoft.VisualStudio.TestTools.UnitTesting;
using Silk.Data.SQL.Expressions;
using System.Threading.Tasks;

namespace Silk.Data.SQL.ProviderTests
{
	public partial class SqlProviderTests
	{
		[TestMethod]
		public async Task Arithmatic_Addition()
		{
			using (var queryResult = await DataProvider.ExecuteReaderAsync(
				QueryExpression.Select(QueryExpression.Add(
					QueryExpression.Value(1), QueryExpression.Value(2)
					))
				))
			{
				Assert.IsTrue(queryResult.HasRows);
				Assert.IsTrue(await queryResult.ReadAsync());
				Assert.AreEqual(3, queryResult.GetInt32(0));
			}
		}

		[TestMethod]
		public async Task Arithmatic_Subtraction()
		{
			using (var queryResult = await DataProvider.ExecuteReaderAsync(
				QueryExpression.Select(QueryExpression.Subtract(
					QueryExpression.Value(3), QueryExpression.Value(2)
					))
				))
			{
				Assert.IsTrue(queryResult.HasRows);
				Assert.IsTrue(await queryResult.ReadAsync());
				Assert.AreEqual(1, queryResult.GetInt32(0));
			}
		}

		[TestMethod]
		public async Task Arithmatic_Multiply()
		{
			using (var queryResult = await DataProvider.ExecuteReaderAsync(
				QueryExpression.Select(QueryExpression.Multiply(
					QueryExpression.Value(2), QueryExpression.Value(3)
					))
				))
			{
				Assert.IsTrue(queryResult.HasRows);
				Assert.IsTrue(await queryResult.ReadAsync());
				Assert.AreEqual(6, queryResult.GetInt32(0));
			}
		}

		[TestMethod]
		public async Task Arithmatic_Divide()
		{
			using (var queryResult = await DataProvider.ExecuteReaderAsync(
				QueryExpression.Select(QueryExpression.Divide(
					QueryExpression.Value(6), QueryExpression.Value(2)
					))
				))
			{
				Assert.IsTrue(queryResult.HasRows);
				Assert.IsTrue(await queryResult.ReadAsync());
				Assert.AreEqual(3, queryResult.GetInt32(0));
			}
		}
	}
}
