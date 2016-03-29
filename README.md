Dapper Multi-Mapper - a simple object mapper for join
========================================

See also
-------------
[link](https://github.com/stackexchange/dapper-dot-net/)

Execute a query and map the results to a strongly typed List
------------------------------------------------------------

Note: all extension methods assume the connection is already open, they will fail if the connection is closed.

```csharp
public static IEnumerable<TFirst> Query<TFirst, TSecond, TSecondKey>(this IDbConnection cnn,
            string sql,
            Action<TFirst, TSecond> addChildToFirst,
            Func<TSecond, TSecondKey> secondKey, ...)
```
Example usage:

```csharp
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
            
var list = db.Conn.Query<PACK_ENTRY, FILE_ENTRY, string>(
    @"select
        p.ID,p.GU_CODE,p.NO

        ,f.PACK_ID
        ,f.ID,f.NO,f.SNILS
    from PACK_ENTRY p
        join FILE_ENTRY f on f.PACK_ID=p.ID",

    (pack, file) => {
        if (pack.FILES == null)
            pack.FILES = new List<FILE_ENTRY>();
        pack.FILES.Add(file);
    }, f => f.PACK_ID, 

    splitOn: "PACK_ID").ToList()
```