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
		public virtual async Task Transactions_CommitPersists()
		{
			using (var testTable = await DataTestHelpers.CreateAutoIncrementTable(DataProvider))
			{
				using (var transaction = await DataProvider.CreateTransactionAsync())
				{
					await transaction.ExecuteNonQueryAsync(QueryExpression.Insert(
						DataTestHelpers.AUTOINC_TABLE_NAME,
						new[] { "Data" },
						new object[] { 10 }
						));

					transaction.Commit();
				}

				using (var queryResult = await DataProvider.ExecuteReaderAsync(QueryExpression.Select(
					new[] { QueryExpression.All() },
					QueryExpression.Table(DataTestHelpers.AUTOINC_TABLE_NAME)
					)))
				{
					Assert.IsTrue(queryResult.HasRows);
				}
			}
		}

		[TestMethod]
		public virtual async Task Transactions_Rollback()
		{
			using (var testTable = await DataTestHelpers.CreateAutoIncrementTable(DataProvider))
			{
				using (var transaction = await DataProvider.CreateTransactionAsync())
				{
					await transaction.ExecuteNonQueryAsync(QueryExpression.Insert(
						DataTestHelpers.AUTOINC_TABLE_NAME,
						new[] { "Data" },
						new object[] { 10 }
						));

					transaction.Rollback();
				}

				using (var queryResult = await DataProvider.ExecuteReaderAsync(QueryExpression.Select(
					new[] { QueryExpression.All() },
					QueryExpression.Table(DataTestHelpers.AUTOINC_TABLE_NAME)
					)))
				{
					Assert.IsFalse(queryResult.HasRows);
				}
			}
		}
	}
}
