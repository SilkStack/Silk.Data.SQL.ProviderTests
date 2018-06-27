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
		private IDataProvider _dataProvider;

		public TestContext TestContext { get; set; }
		public IDataProvider DataProvider
		{
			get
			{
				if (_dataProvider == null)
					_dataProvider = CreateDataProvider(TestContext.Properties["ConnectionString"] as string);
				return _dataProvider;
			}
		}

		public abstract void Dispose();
		public abstract IDataProvider CreateDataProvider(string connectionString);

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
