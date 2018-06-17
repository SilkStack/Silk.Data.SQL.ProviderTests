using Silk.Data.SQL.Expressions;
using Silk.Data.SQL.Providers;
using System;

namespace Silk.Data.SQL.ProviderTests
{
	public class TemporaryTestTable : IDisposable
	{
		public TemporaryTestTable(string tableName, IDataProvider dataProvider)
		{
			TableName = tableName;
			_dataProvider = dataProvider;
		}

		public string TableName { get; }

		private readonly IDataProvider _dataProvider;

		public void Dispose()
		{
			_dataProvider.ExecuteNonQuery(
				QueryExpression.DropTable(TableName)
				);
		}
	}
}
