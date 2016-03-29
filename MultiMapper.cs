using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Dapper
{
	public static class MultiMapper
	{
		public static IEnumerable<TFirst> Query<TFirst, TSecond, TFirstKey>(this IDbConnection cnn, 
			string sql,
			Func<TFirst, TFirstKey> firstKey,
			Action<TFirst, TSecond> addChildToFirst,
			object param = null, IDbTransaction transaction = null, bool buffered = true,
			string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
		{
			var lookup = new Dictionary<TFirstKey, TFirst>();
			Func<TFirst, TSecond, TFirst> map = delegate(TFirst f, TSecond second)
			{
				TFirst first;
				TFirstKey fKey = firstKey(f);
				if (!lookup.TryGetValue(fKey, out first))
				{
					lookup.Add(fKey, first = f);
				}
				addChildToFirst(first, second);
				return first;
			};
			cnn.Query<TFirst, TSecond, TFirst>(sql, map, param, transaction, buffered, splitOn, commandTimeout, commandType)
				.AsQueryable();
			return lookup.Values;
		}

		public static IEnumerable<TFirst> Query<TFirst, TSecond, TSecondKey>(this IDbConnection cnn,
			string sql,
			Action<TFirst, TSecond> addChildToFirst,
			Func<TSecond, TSecondKey> secondKey,
			object param = null, IDbTransaction transaction = null, bool buffered = true,
			string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
		{
			var lookup = new Dictionary<TSecondKey, TFirst>();
			Func<TFirst, TSecond, TFirst> map = delegate(TFirst f, TSecond second)
			{
				TFirst first;
				TSecondKey fKey = secondKey(second);
				if (!lookup.TryGetValue(fKey, out first))
				{
					lookup.Add(fKey, first = f);
				}
				addChildToFirst(first, second);
				return first;
			};
			cnn.Query<TFirst, TSecond, TFirst>(sql, map, param, transaction, buffered, splitOn, commandTimeout, commandType)
				.AsQueryable();
			return lookup.Values;
		}

		public static IEnumerable<TFirst> Query<TFirst, TSecond, TThird, TFirstKey, TSecondKey>(this IDbConnection cnn, 
			string sql,
			Func<TFirst, TFirstKey> firstKey,
			Action<TFirst, TSecond> addChildToFirst,
			Func<TSecond, TSecondKey> secondKey,
			Action<TSecond, TThird> addChildToSecond,
			object param = null, IDbTransaction transaction = null, bool buffered = true, 
			string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
		{
			var firstLookup = new Dictionary<TFirstKey, TFirst>();
			var secondLookup = new Dictionary<TFirstKey, Dictionary<TSecondKey, TSecond>>();

			Func<TFirst, TSecond, TThird, TFirst> map = delegate(TFirst f, TSecond s, TThird third)
			{
				TFirst first;
				TFirstKey fKey = firstKey(f);
				if (!firstLookup.TryGetValue(fKey, out first))
				{
					firstLookup.Add(fKey, first = f);
				}
				Dictionary<TSecondKey, TSecond> lookup;
				if (!secondLookup.TryGetValue(fKey, out lookup))
				{
					secondLookup.Add(fKey, lookup = new Dictionary<TSecondKey, TSecond>());
				}
				TSecond second;
				TSecondKey sKey = secondKey(s);
				if (!lookup.TryGetValue(sKey, out second))
				{
					lookup.Add(sKey, second = s);
					addChildToFirst(first, second);
				}
				addChildToSecond(second, third);
				return first;
			};
			cnn.Query<TFirst, TSecond, TThird, TFirst>(sql, map, param, transaction, buffered , splitOn, commandTimeout, commandType)
				.AsQueryable();
			return firstLookup.Values;
		}

		public static IEnumerable<TFirst> Query<TFirst, TSecond, TThird, TSecondKey, TThirdKey>(this IDbConnection cnn,
			string sql,
			Action<TFirst, TSecond> addChildToFirst,
			Func<TSecond, TSecondKey> secondKey,
			Action<TSecond, TThird> addChildToSecond,
			Func<TThird, TThirdKey> thirdKey,
			object param = null, IDbTransaction transaction = null, bool buffered = true,
			string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
		{
			var firstLookup = new Dictionary<TSecondKey, TFirst>();
			var secondLookup = new Dictionary<TSecondKey, Dictionary<TThirdKey, TSecond>>();

			Func<TFirst, TSecond, TThird, TFirst> map = delegate(TFirst f, TSecond s, TThird third)
			{
				TFirst first;
				TSecondKey fKey = secondKey(s);
				if (!firstLookup.TryGetValue(fKey, out first))
				{
					firstLookup.Add(fKey, first = f);
				}
				Dictionary<TThirdKey, TSecond> lookup;
				if (!secondLookup.TryGetValue(fKey, out lookup))
				{
					secondLookup.Add(fKey, lookup = new Dictionary<TThirdKey, TSecond>());
				}
				TSecond second;
				TThirdKey sKey = thirdKey(third);
				if (!lookup.TryGetValue(sKey, out second))
				{
					lookup.Add(sKey, second = s);
					addChildToFirst(first, second);
				}
				addChildToSecond(second, third);
				return first;
			};
			cnn.Query<TFirst, TSecond, TThird, TFirst>(sql, map, param, transaction, buffered, splitOn, commandTimeout, commandType)
				.AsQueryable();
			return firstLookup.Values;
		}




		public static IEnumerable<TFirst> Read<TFirst, TSecond, TFirstKey>(
			this SqlMapper.GridReader reader,
			Func<TFirst, TFirstKey> firstKey,
			Action<TFirst, IEnumerable<TSecond>> addChildrenToFirst,
			Func<TSecond, TFirstKey> secondKey)
		{
			var firstLookup = reader.Read<TFirst>().ToList();
			var secondLookup = reader
				.Read<TSecond>()
				.GroupBy(s => secondKey(s))
				.ToDictionary(g => g.Key, g => g.AsEnumerable());

			foreach (var first in firstLookup)
			{
				IEnumerable<TSecond> secondChildren;
				if (secondLookup.TryGetValue(firstKey(first), out secondChildren))
				{
					addChildrenToFirst(first, secondChildren);
				}
			}
			return firstLookup;
		}

		public static IEnumerable<TFirst> Read<TFirst, TSecond, TThird, TKeyFirst, TKeySecond>(
			this SqlMapper.GridReader reader,
			Func<TFirst, TKeyFirst> firstKey,
			Action<TFirst, IEnumerable<TSecond>> addChildrenIntoFirst,
			Func<TSecond, TKeyFirst> secondKey,
			Func<TSecond, TKeySecond> thirdKey,
			Action<TSecond, IEnumerable<TThird>> addChildrenIntoSecond,
			Func<TThird, TKeySecond> fourthKey)
		{
			var firstLookup = reader.Read<TFirst>().ToList();
			var secondLookup = reader
				.Read<TSecond>()
				.GroupBy(s => secondKey(s))
				.ToDictionary(g => g.Key, g => g.AsEnumerable());
			var thirdLookup = reader
				.Read<TThird>()
				.GroupBy(s => fourthKey(s))
				.ToDictionary(g => g.Key, g => g.AsEnumerable());

			foreach (var first in firstLookup)
			{
				IEnumerable<TSecond> secondChildren;
				if (secondLookup.TryGetValue(firstKey(first), out secondChildren))
				{
					foreach (var second in secondChildren)
					{
						IEnumerable<TThird> thirdChildren;
						if (thirdLookup.TryGetValue(thirdKey(second), out thirdChildren))
						{
							addChildrenIntoSecond(second, thirdChildren);
						}
					}
					addChildrenIntoFirst(first, secondChildren);
				}
			}
			return firstLookup;
		}
	}
}