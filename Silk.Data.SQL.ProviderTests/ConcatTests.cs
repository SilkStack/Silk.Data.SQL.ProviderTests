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
		public virtual async Task Concat_SelectStrings()
		{
			using (var queryResult = await DataProvider.ExecuteReaderAsync(
				QueryExpression.Select(QueryExpression.Concat(
					QueryExpression.Value("Hello"), QueryExpression.Value(" "), QueryExpression.Value("World")
					))
				))
			{
				Assert.IsTrue(queryResult.HasRows);
				Assert.IsTrue(await queryResult.ReadAsync());
				Assert.AreEqual("Hello World", queryResult.GetString(0));
			}
		}
	}
}
