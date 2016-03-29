using System;
using System.Collections.Generic;
using System.Linq;
using NLog;
using NUnit.Framework;

namespace Dapper
{
	#region Entries

	public class REGISTRY_ENTRY
	{
		public string ID { get; set; }
		public DateTime INCOME { get; set; }
		public long GU_CODE { get; set; }
		public long NO { get; set; }
		public DateTime? SIGNED { get; set; }

		public List<PACK_ENTRY> PACKS { get; set; }
	}

	public class PACK_ENTRY
	{
		public string ID { get; set; }
		public long GU_CODE { get; set; }
		public long NO { get; set; }

		public string REGISTRY_ID { get; set; }

		public List<FILE_ENTRY> FILES { get; set; }
	}

	public class FILE_ENTRY
	{
		public string ID { get; set; }
		public string PACK_ID { get; set; }
		public long NO { get; set; }
		public string SNILS { get; set; }
	}

	#endregion

	[TestFixture]
	public class UnitTests
	{
		private static readonly Logger Log = LogManager.GetCurrentClassLogger();

		#region OneJoin

		private static List<PACK_ENTRY> GetOneJoin()
		{
			const string sql =
@"select
    p.ID,p.GU_CODE,p.NO

    ,f.PACK_ID
    ,f.ID,f.NO,f.SNILS
from PACK_ENTRY p
join FILE_ENTRY f on f.PACK_ID=p.ID";

			var lookup = new Dictionary<string, PACK_ENTRY>();
			using (var db = new DB2StorageData(Log))
			{
				db.Conn.Query<PACK_ENTRY, FILE_ENTRY, PACK_ENTRY>(sql, (p, f) =>
				{
					PACK_ENTRY pack;
					if (!lookup.TryGetValue(p.ID, out pack))
					{
						lookup.Add(p.ID, pack = p);
					}
					if (p.FILES == null)
						p.FILES = new List<FILE_ENTRY>();

					pack.FILES.Add(f);
					return pack;
				}, splitOn: "PACK_ID").AsQueryable();
			}
			return lookup.Values.ToList();
		}

		[Test]
		public void OneJoinWithPrimaryKey()
		{
			const string sql =
@"select
    p.ID,p.GU_CODE,p.NO

    ,f.PACK_ID
    ,f.ID,f.NO,f.SNILS
from PACK_ENTRY p
join FILE_ENTRY f on f.PACK_ID=p.ID";

			var expected = GetOneJoin();
			List<PACK_ENTRY> actual;
			using (var db = new DB2StorageData(Log))
			{
				actual = db.Conn.Query<PACK_ENTRY, FILE_ENTRY, string>(sql,

					p => p.ID,
					(pack, file) =>
					{
						if (pack.FILES == null)
							pack.FILES = new List<FILE_ENTRY>();
						pack.FILES.Add(file);
					},

					splitOn: "PACK_ID").ToList();
			}
			AreEquivalent(expected, actual);
		}

		[Test]
		public void OneJoinWithForeignKey()
		{
			const string sql =
@"select
    p.ID,p.GU_CODE,p.NO

    ,f.PACK_ID
    ,f.ID,f.NO,f.SNILS
from PACK_ENTRY p
join FILE_ENTRY f on f.PACK_ID=p.ID";

			var expected = GetOneJoin();
			List<PACK_ENTRY> actual;
			using (var db = new DB2StorageData(Log))
			{
				actual = db.Conn.Query<PACK_ENTRY, FILE_ENTRY, string>(sql,

					(pack, file) =>
					{
						if (pack.FILES == null)
							pack.FILES = new List<FILE_ENTRY>();
						pack.FILES.Add(file);
					}, f => f.PACK_ID,

					splitOn: "PACK_ID").ToList();
			}
			AreEquivalent(expected, actual);
		}

		#endregion

		#region TwoJoin

		private static List<REGISTRY_ENTRY> GetTwoJoin()
		{
			const string sql =
@"select
    r.ID,r.INCOME,r.GU_CODE,r.NO,r.SIGNED
    
    ,p.REGISTRY_ID
    ,p.ID,p.GU_CODE,p.NO

    ,f.PACK_ID
    ,f.ID,f.NO,f.SNILS
from REGISTRY_ENTRY r
join PACK_ENTRY p on p.REGISTRY_ID = r.ID
join FILE_ENTRY f on f.PACK_ID=p.ID";

			var registryLookup = new Dictionary<string, REGISTRY_ENTRY>();
			var packLookup = new Dictionary<REGISTRY_ENTRY, Dictionary<string, PACK_ENTRY>>();
			using (var db = new DB2StorageData(Log))
			{
				db.Conn.Query<REGISTRY_ENTRY, PACK_ENTRY, FILE_ENTRY, REGISTRY_ENTRY>(sql, (r, p, f) =>
				{
					REGISTRY_ENTRY registry;
					if (!registryLookup.TryGetValue(r.ID, out registry))
					{
						registryLookup.Add(r.ID, registry = r);
					}
					if (registry.PACKS == null)
						registry.PACKS = new List<PACK_ENTRY>();

					Dictionary<string, PACK_ENTRY> lookup;
					if (!packLookup.TryGetValue(registry, out lookup))
					{
						packLookup.Add(registry, lookup = new Dictionary<string, PACK_ENTRY>());
					}

					PACK_ENTRY pack;
					if (!lookup.TryGetValue(p.ID, out pack))
					{
						lookup.Add(p.ID, pack = p);
						registry.PACKS.Add(pack);
					}
					if (p.FILES == null)
						p.FILES = new List<FILE_ENTRY>();

					pack.FILES.Add(f);
					return registry;
				}, splitOn: "REGISTRY_ID,PACK_ID").AsQueryable();
			}
			return registryLookup.Values.ToList();
		}

		[Test]
		public void TwoJoinWithPrimaryKey()
		{
			const string sql =
@"select
    r.ID,r.INCOME,r.GU_CODE,r.NO,r.SIGNED
    
    ,p.REGISTRY_ID
    ,p.ID,p.GU_CODE,p.NO

    ,f.PACK_ID
    ,f.ID,f.NO,f.SNILS
from REGISTRY_ENTRY r
join PACK_ENTRY p on p.REGISTRY_ID = r.ID
join FILE_ENTRY f on f.PACK_ID=p.ID";

			var expected = GetTwoJoin();
			List<REGISTRY_ENTRY> actual;
			using (var db = new DB2StorageData(Log))
			{
				actual = db.Conn.Query<REGISTRY_ENTRY, PACK_ENTRY, FILE_ENTRY, string, string>(sql,

					r => r.ID, (registry, pack) =>
					{
						if (registry.PACKS == null)
							registry.PACKS = new List<PACK_ENTRY>();
						registry.PACKS.Add(pack);
					},

					p => p.ID, (pack, file) =>
					{
						if (pack.FILES == null)
							pack.FILES = new List<FILE_ENTRY>();
						pack.FILES.Add(file);
					},

					splitOn: "REGISTRY_ID,PACK_ID").ToList();
			}
			AreEquivalent(expected, actual);
		}

		[Test]
		public void TwoJoinWithForeignKey()
		{
			const string sql =
@"select
    r.ID,r.INCOME,r.GU_CODE,r.NO,r.SIGNED
    
    ,p.REGISTRY_ID
    ,p.ID,p.GU_CODE,p.NO

    ,f.PACK_ID
    ,f.ID,f.NO,f.SNILS
from REGISTRY_ENTRY r
join PACK_ENTRY p on p.REGISTRY_ID = r.ID
join FILE_ENTRY f on f.PACK_ID=p.ID";

			var expected = GetTwoJoin();
			List<REGISTRY_ENTRY> actual;
			using (var db = new DB2StorageData(Log))
			{
				actual = db.Conn.Query<REGISTRY_ENTRY, PACK_ENTRY, FILE_ENTRY, string, string>(sql,

					(registry, pack) =>
					{
						if (registry.PACKS == null)
							registry.PACKS = new List<PACK_ENTRY>();
						registry.PACKS.Add(pack);
					}, p => p.REGISTRY_ID,

					(pack, file) =>
					{
						if (pack.FILES == null)
							pack.FILES = new List<FILE_ENTRY>();
						pack.FILES.Add(file);
					}, f => f.PACK_ID,

					splitOn: "REGISTRY_ID,PACK_ID").ToList();
			}
			AreEquivalent(expected, actual);
		}

		#endregion

		#region QueryMultiple

		private static PACK_ENTRY GetPack(string id)
		{
			const string sql =
@"select
    p.ID,p.GU_CODE,p.NO

    ,f.PACK_ID
    ,f.ID,f.NO,f.SNILS
from PACK_ENTRY p
join FILE_ENTRY f on f.PACK_ID=p.ID
where p.ID=?";

			PACK_ENTRY entry;
			using (var db = new DB2StorageData(Log))
			{
				entry = db.Conn.Query<PACK_ENTRY, FILE_ENTRY, string>(sql,

					(pack, file) =>
					{
						if (pack.FILES == null)
							pack.FILES = new List<FILE_ENTRY>();
						pack.FILES.Add(file);
					}, f => f.PACK_ID,

					param: new { id },

					splitOn: "PACK_ID").FirstOrDefault();
			}
			return entry;
		}

		[Test]
		public void QueryMutiple()
		{
			const string sql =
@"select ID,GU_CODE,NO from PACK_ENTRY where ID='BBFDX';
select PACK_ID,ID,NO,SNILS from FILE_ENTRY where PACK_ID='BBFDX';";

			var expected = GetPack("BBFDX");
			PACK_ENTRY actual;
			using (var db = new DB2StorageData(Log))
			{
				using (var result = db.Conn.QueryMultiple(sql))
				{
					actual = result.Read<PACK_ENTRY>().Single();
					actual.FILES = result.Read<FILE_ENTRY>().ToList();
				}
			}
			AreEquivalent(expected, actual);
		}

		[Test]
		public void QueryOneMutipleWithChild()
		{
			const string sql =
@"select ID,GU_CODE,NO from PACK_ENTRY;
select PACK_ID,ID,NO,SNILS from FILE_ENTRY;";

			var expected = GetOneJoin();
			List<PACK_ENTRY> actual;
			using (var db = new DB2StorageData(Log))
			{
				using (var result = db.Conn.QueryMultiple(sql))
				{
					actual = result.Read<PACK_ENTRY, FILE_ENTRY, string>
					(
						p => p.ID, (pack, files) => { pack.FILES = files.ToList(); },
						f => f.PACK_ID
					).ToList();
				}
			}
			AreEquivalent(expected, actual);
		}

		[Test]
		public void QueryTwoMutipleWithChild()
		{
			const string sql =
@"select ID,INCOME,GU_CODE,NO,SIGNED from REGISTRY_ENTRY;
select REGISTRY_ID,ID,GU_CODE,NO from PACK_ENTRY where REGISTRY_ID is not null;
select PACK_ID,ID,NO,SNILS from FILE_ENTRY;";

			var expected = GetTwoJoin();
			List<REGISTRY_ENTRY> actual;
			using (var db = new DB2StorageData(Log))
			{
				using (var result = db.Conn.QueryMultiple(sql))
				{
					actual = result.Read<REGISTRY_ENTRY, PACK_ENTRY, FILE_ENTRY, string, string>
					(
						r => r.ID, (registry, packs) => { registry.PACKS = packs.ToList(); },
						p => p.REGISTRY_ID,

						p => p.ID, (pack, files) => { pack.FILES = files.ToList(); },
						f => f.PACK_ID
					).ToList();
				}
			}
			AreEquivalent(expected, actual);
		}

		#endregion

		#region AreEquivalent

		private static void AreEquivalent(ICollection<REGISTRY_ENTRY> expected, ICollection<REGISTRY_ENTRY> actual)
		{
			Assert.That(actual.Count, Is.EqualTo(expected.Count));
			foreach (var registry in actual)
			{
				var r = expected.FirstOrDefault(x => x.ID == registry.ID);
				if (r == null) Assert.Fail("The desired object can't be found");
				AreEquivalent(r, registry);
			}
		}

		private static void AreEquivalent(REGISTRY_ENTRY expected, REGISTRY_ENTRY actual)
		{
			Assert.IsNotNull(expected);
			Assert.IsNotNull(actual);
			Assert.That(actual.ID, Is.EqualTo(expected.ID));
			Assert.That(actual.INCOME, Is.EqualTo(expected.INCOME).Within(1).Seconds);
			Assert.That(actual.GU_CODE, Is.EqualTo(expected.GU_CODE));
			Assert.That(actual.NO, Is.EqualTo(expected.NO));
			if (actual.SIGNED.HasValue)
			{
				Assert.IsTrue(expected.SIGNED.HasValue);
				Assert.That(actual.SIGNED.Value, Is.EqualTo(expected.SIGNED.Value).Within(1).Seconds);
			}
			else
			{
				Assert.IsFalse(expected.SIGNED.HasValue);
			}
			AreEquivalent(expected.PACKS, actual.PACKS);
		}

		private static void AreEquivalent(ICollection<PACK_ENTRY> expected, ICollection<PACK_ENTRY> actual)
		{
			Assert.That(actual.Count, Is.EqualTo(expected.Count));
			foreach (var pack in actual)
			{
				var p = expected.FirstOrDefault(x => x.ID == pack.ID);
				if (p == null) Assert.Fail("The desired object can't be found");
				AreEquivalent(p, pack);
			}
		}

		private static void AreEquivalent(PACK_ENTRY expected, PACK_ENTRY actual)
		{
			Assert.IsNotNull(expected);
			Assert.IsNotNull(actual);
			Assert.That(actual.ID, Is.EqualTo(expected.ID));
			Assert.That(actual.GU_CODE, Is.EqualTo(expected.GU_CODE));
			Assert.That(actual.NO, Is.EqualTo(expected.NO));
			Assert.That(actual.REGISTRY_ID, Is.EqualTo(expected.REGISTRY_ID));
			AreEquivalent(expected.FILES, actual.FILES);
		}

		private static void AreEquivalent(ICollection<FILE_ENTRY> expected, ICollection<FILE_ENTRY> actual)
		{
			Assert.That(actual.Count, Is.EqualTo(expected.Count));
			foreach (var file in actual)
			{
				var f = expected.FirstOrDefault(x => x.ID == file.ID);
				if (f == null) Assert.Fail("The desired object can't be found");
				AreEquivalent(f, file);
			}
		}

		private static void AreEquivalent(FILE_ENTRY expected, FILE_ENTRY actual)
		{
			Assert.IsNotNull(expected);
			Assert.IsNotNull(actual);
			Assert.That(actual.ID, Is.EqualTo(expected.ID));
			Assert.That(actual.PACK_ID, Is.EqualTo(expected.PACK_ID));
			Assert.That(actual.NO, Is.EqualTo(expected.NO));
			Assert.That(actual.SNILS, Is.EqualTo(expected.SNILS));
		}

		#endregion
	}
}