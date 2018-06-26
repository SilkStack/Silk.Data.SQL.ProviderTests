using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Threading.Tasks;

namespace Silk.Data.SQL.ProviderTests
{
	public partial class SqlProviderTests
	{
		[TestMethod]
		public virtual async Task TableExists_CheckForTestTable()
		{
			Assert.IsTrue(await TableExists("TableExistsTest"));
		}
	}
}
