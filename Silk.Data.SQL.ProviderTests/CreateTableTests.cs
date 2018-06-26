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
		public virtual async Task CreateTable_CanCreateATable()
		{
			var tableName = "CreateTableTest";
			using (new TemporaryTestTable(tableName, DataProvider))
			{
				await DataProvider.ExecuteNonQueryAsync(
					QueryExpression.CreateTable(
						tableName,
						QueryExpression.DefineColumn("Column", SqlDataType.Int())
						)
					);
				Assert.IsTrue(await TableExists(tableName));
			}
		}

		[TestMethod]
		public virtual async Task CreateTable_CanCreateWithAutoIncrementPrimaryKey()
		{
			var tableName = "CreateTableTest";
			using (new TemporaryTestTable(tableName, DataProvider))
			{
				await DataProvider.ExecuteNonQueryAsync(
					QueryExpression.CreateTable(
						tableName,
						QueryExpression.DefineColumn("Column", SqlDataType.Int(), isAutoIncrement: true, isPrimaryKey: true)
						)
					);
				Assert.IsTrue(await TableExists(tableName));
			}
		}

		[TestMethod]
		public virtual async Task CreateTable_CanCreateCompositePrimaryKey()
		{
			var tableName = "CreateTableTest";
			using (new TemporaryTestTable(tableName, DataProvider))
			{
				await DataProvider.ExecuteNonQueryAsync(
					QueryExpression.CreateTable(
					tableName,
					QueryExpression.DefineColumn("AutoId", SqlDataType.Int(), isAutoIncrement: true, isPrimaryKey: true),
					QueryExpression.DefineColumn("RefId", SqlDataType.Guid(), isNullable: false, isPrimaryKey: true)
					));
				Assert.IsTrue(await TableExists(tableName));
			}
		}
	}
}
