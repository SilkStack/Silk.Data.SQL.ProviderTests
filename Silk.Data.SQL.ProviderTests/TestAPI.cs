using Microsoft.VisualStudio.TestTools.UnitTesting;
using Silk.Data.SQL.Expressions;
using Silk.Data.SQL.Providers;
using System;
using System.Threading.Tasks;

namespace Silk.Data.SQL.ProviderTests
{
	[TestClass]
	public abstract partial class SqlProviderTests : IDisposable
	{
		public abstract IDataProvider DataProvider { get; }
		public abstract void Dispose();

		protected async Task<bool> TableExists(string tableName)
		{
			var query = QueryExpression.TableExists(tableName);
			using (var result = await DataProvider.ExecuteReaderAsync(query))
			{
				if (!result.HasRows || !await result.ReadAsync())
					return false;

				return result.GetInt32(0) == 1;
			}
		}

		protected Task DropTable(string tableName)
		{
			return DataProvider.ExecuteNonQueryAsync(
				QueryExpression.DropTable(tableName)
				);
		}
	}
}
