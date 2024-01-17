namespace DbFun.Core.Builders

open System
open System.Data
open System.Linq.Expressions
open System.Reflection
open FSharp.Quotations
open FSharp.Quotations.Patterns
open DbFun.Core
open System.Data.Common

type IResultReader<'Result> = 
    abstract member Read: IDataReader -> Async<'Result>

type ResultSpecifier<'Result> = IRowGetterProvider * IDataReader -> IResultReader<'Result>

module Helper = 

    let nextResultAsync(reader: IDataReader): Async<bool> = 
        async {
            match reader with 
            | :? DbDataReader as dbReader ->
                let! token = Async.CancellationToken
                let! result = dbReader.NextResultAsync(token) |> Async.AwaitTask
                return result
            | _ ->
                let reader = reader.NextResult()
                return reader
        }

    let advance (resultTypes: Type seq) (reader: IDataReader) = 
        if reader <> null && not (reader.NextResult()) then
            failwithf "Not enough results when reading %A" resultTypes

    let advanceAsync (resultTypes: Type seq) (reader: IDataReader) = 
        async {
            let! exists = nextResultAsync(reader)
            if not exists then
                failwithf "Not enough results when reading %A" resultTypes
        }

open Helper


type Results() = 

#if DOTNET_FRAMEWORK
    static let readOnlyDict (l: ('k * 'v) seq) = 
        let dict = System.Collections.Generic.Dictionary<'k, 'v>()
        for k, v in l do
            dict.[k] <- v
        dict
#endif
  

    static member private EmptyReader<'Result>() =
        { new IResultReader<'Result> with
                member __.Read(_: IDataReader): Async<'Result> = 
                    async.Return Unchecked.defaultof<'Result>
        }        

    /// <summary>
    /// The artificial result builder representing return type of command not returning any result.
    /// </summary>
    static member Unit: ResultSpecifier<unit> = 
        fun (_: IRowGetterProvider, _: IDataReader) -> Results.EmptyReader<unit>()
            
    /// <summary>
    /// Creates result builder retrieving exactly one row.
    /// </summary>
    /// <param name="rowBuilder">
    /// The row builder.
    /// </param>
    static member Single<'Result> (rowBuilder: IRowGetterProvider * IDataRecord -> IRowGetter<'Result>): ResultSpecifier<'Result> = 
        fun (provider: IRowGetterProvider, prototype: IDataReader) ->
            let getter = rowBuilder(provider, prototype)
            { new IResultReader<'Result> with
                member __.Read(reader: IDataReader) = 
                    if reader.Read() then
                        async.Return (getter.Get(reader))
                    else
                        failwithf "Cannot read %A object, resultset is empty. Use option type." typeof<'Result>
            }

    /// <summary>
    /// Creates result builder retrieving exactly one row.
    /// </summary>
    /// <param name="name">
    /// The result column name or record prefix.
    /// </param>
    static member Single<'Result> (?name: string): ResultSpecifier<'Result> = 
        fun (provider: IRowGetterProvider, prototype: IDataReader) ->
            let getter = provider.Getter(defaultArg name "", prototype)
            { new IResultReader<'Result> with
                member __.Read(reader: IDataReader) = 
                    if reader.Read() then
                        async.Return (getter.Get(reader))
                    else
                        failwithf "Cannot read %A object, resultset is empty. Use option type." typeof<'Result>
            }

    /// <summary>
    /// Creates result builder retrieving at most one row.
    /// </summary>
    /// <param name="rowBuilder">
    /// The underlying row builder.
    /// </param>
    static member Optional<'Result> (rowBuilder: RowSpecifier<'Result>): ResultSpecifier<'Result option> = 
        fun (provider: IRowGetterProvider, prototype: IDataReader)  ->
            let getter = rowBuilder(provider, prototype)
            { new IResultReader<'Result option> with
                member __.Read(reader: IDataReader) = 
                    if reader.Read() then
                        async.Return (Some <| getter.Get(reader))
                    else
                        async.Return None
            }

    /// <summary>
    /// Creates result builder retrieving at most one row.
    /// </summary>
    /// <param name="name">
    /// The result column name or record prefix.
    /// </param>
    static member Optional<'Result> (?name: string): ResultSpecifier<'Result option>  = 
        fun (provider: IRowGetterProvider, prototype: IDataReader)  ->
            let getter = provider.Getter(defaultArg name "", prototype)
            { new IResultReader<'Result option> with
                member __.Read(reader: IDataReader) = 
                    if reader.Read() then
                        async.Return (Some <| getter.Get(reader))
                    else
                        async.Return None
            }

    /// <summary>
    /// Creates result builder retrieving many rows as a sequence.
    /// </summary>
    /// <param name="rowBuilder">
    /// The underlying row builder.
    /// </param>
    static member Seq<'Result> (rowBuilder: RowSpecifier<'Result>): ResultSpecifier<'Result seq> = 
        fun (provider: IRowGetterProvider, prototype: IDataReader)  ->
            let getter = rowBuilder(provider, prototype)
            { new IResultReader<'Result seq> with
                member __.Read(reader: IDataReader) = 
                    async.Return 
                        [ while reader.Read() do
                            getter.Get(reader)
                        ]
            }

    /// <summary>
    /// Creates result builder retrieving many rows as a sequence.
    /// </summary>
    /// <param name="name">
    /// The result column name or record prefix.
    /// </param>
    static member Seq<'Result> (?name: string): ResultSpecifier<'Result seq> = 
        fun (provider: IRowGetterProvider, prototype: IDataReader)  ->
            let getter = provider.Getter<'Result>(defaultArg name "", prototype)
            { new IResultReader<'Result seq> with
                member __.Read(reader: IDataReader) = 
                    async.Return 
                        [ while reader.Read() do
                            getter.Get(reader)
                        ]
            }

    /// <summary>
    /// Creates result builder retrieving many rows as a list.
    /// </summary>
    /// <param name="rowBuilder">
    /// The underlying row builder.
    /// </param>
    static member List<'Result> (rowBuilder: RowSpecifier<'Result>): ResultSpecifier<'Result list> = 
        fun (provider: IRowGetterProvider, prototype: IDataReader)  ->
            let getter = rowBuilder(provider, prototype)
            { new IResultReader<'Result list> with
                member __.Read(reader: IDataReader) = 
                    async.Return 
                        [ while reader.Read() do
                            getter.Get(reader)
                        ]
            }

    /// <summary>
    /// Creates result builder retrieving many rows as a list.
    /// </summary>
    /// <param name="name">
    /// The result column name or record prefix.
    /// </param>
    static member List<'Result> (?name: string): ResultSpecifier<'Result list> = 
        fun (provider: IRowGetterProvider, prototype: IDataReader)  ->
            let getter = provider.Getter<'Result>(defaultArg name "", prototype)
            { new IResultReader<'Result list> with
                member __.Read(reader: IDataReader) = 
                    async.Return 
                        [ while reader.Read() do
                            getter.Get(reader)
                        ]
            }

    /// <summary>
    /// Creates result builder retrieving many rows as an array.
    /// </summary>
    /// <param name="rowBuilder">
    /// The underlying row builder.
    /// </param>
    static member Array<'Result> (rowBuilder: RowSpecifier<'Result>): ResultSpecifier<'Result array> = 
        fun (provider: IRowGetterProvider, prototype: IDataReader)  ->
            let getter = rowBuilder(provider, prototype)
            { new IResultReader<'Result array> with
                member __.Read(reader: IDataReader) = 
                    async.Return 
                        [| while reader.Read() do
                            getter.Get(reader)
                        |]
            }

    /// <summary>
    /// Creates result builder retrieving many rows as an array.
    /// </summary>
    /// <param name="name">
    /// The result column name or record prefix.
    /// </param>
    static member Array<'Result> (?name: string): ResultSpecifier<'Result array> = 
        fun (provider: IRowGetterProvider, prototype: IDataReader) ->
            let getter = provider.Getter<'Result>(defaultArg name "", prototype)
            { new IResultReader<'Result array> with
                member __.Read(reader: IDataReader) = 
                    async.Return 
                        [| while reader.Read() do
                            getter.Get(reader)
                        |]
            }

    static member Auto<'Result> (?name: string): ResultSpecifier<'Result> = 
        if Types.isOptionType typeof<'Result> then
            let optionalMethod = typeof<Results>.GetMethod("Optional", [| typeof<string option> |])
            let gOptionalMethod = optionalMethod.MakeGenericMethod(typeof<'Result>.GetGenericArguments().[0])
            let reader = gOptionalMethod.Invoke(null, [| name |]) :?> ResultSpecifier<'Result>
            reader
        elif Types.isCollectionType typeof<'Result> then
            let methodName = 
                if typedefof<'Result> = typedefof<List<_>> then "List"
                elif typedefof<'Result>.IsArray then "Array"
                else "Seq"
            let collectionMethod = typeof<Results>.GetMethod(methodName, [| typeof<string option> |]).MakeGenericMethod(Types.getElementType typeof<'Result>)
            let reader = collectionMethod.Invoke(null, [| name |]) :?> ResultSpecifier<'Result>
            reader
        elif typeof<'Result> = typeof<unit> then
            Results.Unit |> box :?> ResultSpecifier<'Result>
        else
            Results.Single<'Result>(?name = name)

    /// <summary>
    /// Creates a builder of result with primary and foreign key, that can be used in result joins as a master, as well as a detail result.
    /// </summary>
    /// <param name="primaryName">
    /// The name of the primary key column or record prefix for compound keys.
    /// </param>
    /// <param name="foreignName"></param>
    /// The name of the foreign key column or record prefix for compound keys.
    /// <param name="resultName">
    /// The name of the result column or record prefix.
    /// </param>
    static member Keyed<'Primary, 'Foreign, 'Result>(primaryName: string, foreignName: string, resultName: string) = 
        Results.Seq(Rows.Keyed<'Primary, 'Foreign, 'Result>(primaryName, foreignName, resultName))

    /// <summary>
    /// Creates a builder of result with primary and foreign key, that can be used in result joins as a master, as well as a detail result.
    /// </summary>
    /// <param name="primaryName">
    /// The name of the primary key column or record prefix for compound keys.
    /// </param>
    /// <param name="foreignName"></param>
    /// The name of the foreign key column or record prefix for compound keys.
    /// <param name="result">
    /// The result builder.
    /// </param>
    static member Keyed<'Primary, 'Foreign, 'Result>(primaryName: string, foreignName: string, result: RowSpecifier<'Result>) = 
        Results.Seq(Rows.Keyed<'Primary, 'Foreign, 'Result>(primaryName, foreignName, result))

    /// <summary>
    /// Creates a builder of result with primary and foreign key, that can be used in result joins as a master, as well as a detail result.
    /// </summary>
    /// <param name="primary">
    /// The primary key builder.
    /// </param>
    /// <param name="foreign">
    /// The foreign key builder.
    /// </param>
    /// <param name="result">
    /// The result builder.
    /// </param>
    static member Keyed<'Primary, 'Foreign, 'Result>(primary: RowSpecifier<'Primary>, foreign: RowSpecifier<'Foreign>, result: RowSpecifier<'Result>) = 
        Results.Seq(Rows.Keyed<'Primary, 'Foreign, 'Result>(primary, foreign, result))

    /// <summary>
    /// Creates a builder of result with primary key, that can be used in result joins as a master result.
    /// </summary>
    /// <param name="primaryName">
    /// The name of the primary key column or record prefix for compound keys.
    /// </param>
    /// <param name="resultName">
    /// The name of the result column or record prefix.
    /// </param>
    static member PKeyed<'Primary, 'Result>(primaryName: string, ?resultName: string) = 
        Results.Seq(Rows.PKeyed<'Primary, 'Result>(primaryName, ?resultName = resultName))

    /// <summary>
    /// Creates a builder of result with primary key, that can be used in result joins as a master result.
    /// </summary>
    /// <param name="primaryName">
    /// The name of the primary key column or record prefix for compound keys.
    /// </param>
    /// <param name="result">
    /// The result builder.
    /// </param>
    static member PKeyed<'Primary, 'Result>(primaryName: string, result: RowSpecifier<'Result>) = 
        Results.Seq(Rows.PKeyed<'Primary, 'Result>(primaryName, result))

    /// <summary>
    /// Creates a builder of result with primary key, that can be used in result joins as a master result.
    /// </summary>
    /// <param name="primary">
    /// The primary key builder.
    /// </param>
    /// <param name="result">
    /// The result builder.
    /// </param>
    static member PKeyed<'Primary, 'Result>(primary: RowSpecifier<'Primary>, result: RowSpecifier<'Result>) = 
        Results.Seq(Rows.PKeyed<'Primary, 'Result>(primary, result))

    /// <summary>
    /// Creates a builder of result with foreign key, that can be used in result joins as a detail result.
    /// </summary>
    /// <param name="foreignName">
    /// The name of the foreign key column or record prefix for compound keys.
    /// </param>
    /// <param name="resultName">
    /// The name of the result column or record prefix.
    /// </param>
    static member FKeyed<'Foreign, 'Result>(foreignName: string, ?resultName: string) = 
        Results.Seq(Rows.FKeyed<'Foreign, 'Result>(foreignName, ?resultName = resultName))

    /// <summary>
    /// Creates a builder of result with foreign key, that can be used in result joins as a detail result.
    /// </summary>
    /// <param name="foreignName">
    /// The name of the foreign key column or record prefix for compound keys.
    /// </param>
    /// <param name="result">
    /// The result builder.
    /// </param>
    static member FKeyed<'Foreign, 'Result>(foreignName: string, result: RowSpecifier<'Result>) = 
        Results.Seq(Rows.FKeyed<'Foreign, 'Result>(foreignName, result))

    /// <summary>
    /// Creates a builder of result with foreign key, that can be used in result joins as a detail result.
    /// </summary>
    /// <param name="foreign">
    /// The foreign key builder.
    /// </param>
    /// <param name="result">
    /// The result builder.
    /// </param>
    static member FKeyed<'Foreign, 'Result>(foreign: RowSpecifier<'Foreign>, result: RowSpecifier<'Result>) = 
        Results.Seq(Rows.FKeyed<'Foreign, 'Result>(foreign, result))

    /// <summary>
    /// Merges two result builders into one builder creating tuple of source results.
    /// </summary>
    /// <param name="builder1">
    /// First source result builder.
    /// </param>
    /// <param name="builder2">
    /// Second source result builder.
    /// </param>
    static member Multiple<'Result1, 'Result2>(builder1: ResultSpecifier<'Result1>, builder2: ResultSpecifier<'Result2>): ResultSpecifier<'Result1 * 'Result2> = 
        fun (provider: IRowGetterProvider, prototype: IDataReader)  ->
            let reader1 = builder1(provider, prototype)
            advance [ typeof<'Result1>; typeof<'Result2> ] prototype
            let reader2 = builder2(provider, prototype)
            { new IResultReader<'Result1 * 'Result2> with
                member __.Read(reader: IDataReader) = 
                    async {
                        let! result1 = reader1.Read(reader)
                        do! advanceAsync [ typeof<'Result1>; typeof<'Result2> ] reader
                        let! result2 = reader2.Read(reader)
                        return result1, result2
                    }
            }
            
    /// <summary>
    /// Merges three result builders into one builder creating tuple of source results.
    /// </summary>
    /// <param name="builder1">
    /// First source result builder.
    /// </param>
    /// <param name="builder2">
    /// Second source result builder.
    /// </param>
    /// <param name="builder3">
    /// Third source result builder.
    /// </param>
    static member Multiple<'Result1, 'Result2, 'Result3>(builder1: ResultSpecifier<'Result1>, builder2: ResultSpecifier<'Result2>, builder3: ResultSpecifier<'Result3>)
                    : ResultSpecifier<'Result1 * 'Result2 * 'Result3> = 
        fun (provider: IRowGetterProvider, prototype: IDataReader)  ->
            let advance = advance [ typeof<'Result1>; typeof<'Result2>; typeof<'Result3> ]
            let reader1 = builder1(provider, prototype)
            advance prototype
            let reader2 = builder2(provider, prototype)
            advance prototype
            let reader3 = builder3(provider, prototype)
            { new IResultReader<'Result1 * 'Result2 * 'Result3> with
                member __.Read(reader: IDataReader) = 
                    async {
                        let advanceAsync = advanceAsync [ typeof<'Result1>; typeof<'Result2>; typeof<'Result3> ]
                        let! result1 = reader1.Read(reader)
                        do! advanceAsync reader
                        let! result2 = reader2.Read(reader)
                        do! advanceAsync reader
                        let! result3 = reader3.Read(reader)
                        return result1, result2, result3
                    }
            }
            
    /// <summary>
    /// Merges four result builders into one builder creating tuple of source results.
    /// </summary>
    /// <param name="builder1">
    /// First source result builder.
    /// </param>
    /// <param name="builder2">
    /// Second source result builder.
    /// </param>
    /// <param name="builder3">
    /// Third source result builder.
    /// </param>
    /// <param name="builder4">
    /// Fourth source result builder.
    /// </param>
    static member Multiple<'Result1, 'Result2, 'Result3, 'Result4>(builder1: ResultSpecifier<'Result1>, builder2: ResultSpecifier<'Result2>, builder3: ResultSpecifier<'Result3>, builder4: ResultSpecifier<'Result4>)
                    : ResultSpecifier<'Result1 * 'Result2 * 'Result3 * 'Result4> = 
        fun (provider: IRowGetterProvider, prototype: IDataReader)  ->
            let advance = advance [ typeof<'Result1>; typeof<'Result2>; typeof<'Result3>; typeof<'Result4> ]
            let reader1 = builder1(provider, prototype)
            advance prototype
            let reader2 = builder2(provider, prototype)
            advance prototype
            let reader3 = builder3(provider, prototype)
            advance prototype
            let reader4 = builder4(provider, prototype)
            { new IResultReader<'Result1 * 'Result2 * 'Result3 * 'Result4> with
                member __.Read(reader: IDataReader) = 
                    async {
                        let advanceAsync = advanceAsync [ typeof<'Result1>; typeof<'Result2>; typeof<'Result3>; typeof<'Result4> ]
                        let! result1 = reader1.Read(reader)
                        do! advanceAsync reader
                        let! result2 = reader2.Read(reader)
                        do! advanceAsync reader
                        let! result3 = reader3.Read(reader)
                        do! advanceAsync reader
                        let! result4 = reader4.Read(reader)
                        return result1, result2, result3, result4
                    }
            }
            
    /// <summary>
    /// Merges five result builders into one builder creating tuple of source results.
    /// </summary>
    /// <param name="builder1">
    /// First source result builder.
    /// </param>
    /// <param name="builder2">
    /// Second source result builder.
    /// </param>
    /// <param name="builder3">
    /// Third source result builder.
    /// </param>
    /// <param name="builder4">
    /// Fourth source result builder.
    /// </param>
    /// <param name="builder5">
    /// Fifth source result builder.
    /// </param>
    static member Multiple<'Result1, 'Result2, 'Result3, 'Result4, 'Result5>(
            builder1: ResultSpecifier<'Result1>, 
            builder2: ResultSpecifier<'Result2>, 
            builder3: ResultSpecifier<'Result3>, 
            builder4: ResultSpecifier<'Result4>, 
            builder5: ResultSpecifier<'Result5>)
            : ResultSpecifier<'Result1 * 'Result2 * 'Result3 * 'Result4 * 'Result5> = 
        fun (provider: IRowGetterProvider, prototype: IDataReader)  ->
            let advance = advance [ typeof<'Result1>; typeof<'Result2>; typeof<'Result3>; typeof<'Result4>; typeof<'Result5> ]
            let reader1 = builder1(provider, prototype)
            advance prototype
            let reader2 = builder2(provider, prototype)
            advance prototype
            let reader3 = builder3(provider, prototype)
            advance prototype
            let reader4 = builder4(provider, prototype)
            advance prototype
            let reader5 = builder5(provider, prototype)
            { new IResultReader<'Result1 * 'Result2 * 'Result3 * 'Result4 * 'Result5> with
                member __.Read(reader: IDataReader) = 
                    async {
                        let advanceAsync = advanceAsync [ typeof<'Result1>; typeof<'Result2>; typeof<'Result3>; typeof<'Result4>; typeof<'Result5> ]
                        let! result1 = reader1.Read(reader)
                        do! advanceAsync reader
                        let! result2 = reader2.Read(reader)
                        do! advanceAsync reader
                        let! result3 = reader3.Read(reader)
                        do! advanceAsync reader
                        let! result4 = reader4.Read(reader)
                        do! advanceAsync reader
                        let! result5 = reader5.Read(reader)
                        return result1, result2, result3, result4, result5
                    }
            }
            
    /// <summary>
    /// Aplies on a builder function transforming source result to a target type.
    /// </summary>
    /// <param name="mapper">
    /// The function transforming the result.
    /// </param>
    /// <param name="source">
    /// The source result builder.
    /// </param>
    static member Map<'Source, 'Target>(mapper: 'Source -> 'Target) (source: ResultSpecifier<'Source>): ResultSpecifier<'Target> = 
        fun (provider: IRowGetterProvider, prototype: IDataReader) ->
            let srcReader = source(provider, prototype)
            { new IResultReader<'Target> with
                member __.Read(reader: IDataReader) = 
                    async {
                        let! result = srcReader.Read(reader)
                        return mapper(result)
                    }
            }


    /// <summary>
    /// Joins two results by a key using primary key of result1 and foreign key of result2 and merge function to 
    /// consolidate master ('Result1) values with sequences of detail ('Result2) values.
    /// </summary>
    /// <param name="merge">
    /// Function consolidating master value ('Result1) with sequence of detail values ('Result2).
    /// </param>
    static member Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2 when 'Key: comparison>(merge: 'Result1 * 'Result2 seq -> 'Result1): 
            ResultSpecifier<(RowsImpl.KeySpecifier<'PK2, 'Key> * 'Result2) seq> -> 
            ResultSpecifier<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq> -> 
            ResultSpecifier<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq> = 
        fun (builder2: ResultSpecifier<(RowsImpl.KeySpecifier<'PK2, 'Key> * 'Result2) seq>) 
            (builder1: ResultSpecifier<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq>) 
            (provider: IRowGetterProvider, prototype: IDataReader) ->
                let reader1 = builder1(provider, prototype)
                advance [ typeof<'Key * 'Result1>; typeof<'Key * 'Result2> ] prototype
                let reader2 = builder2(provider, prototype)
                { new IResultReader<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq> with
                    member __.Read(reader: IDataReader) = 
                        async {
                            let! result1 = reader1.Read(reader) 
                            do! advanceAsync [ typeof<'Key * 'Result1>; typeof<'Key * 'Result2> ] reader
                            let! result2 = reader2.Read(reader) 
                            let result2' = result2 |> Seq.groupBy (fun (k, r) -> k.Foreign) |> readOnlyDict
                            let merged = result1 |> Seq.map (fun (k, r1) -> k, merge(r1, result2'.TryGetValue k.Primary |> (function (true, r2s) -> r2s |> Seq.map snd | (false, _) -> Seq.empty)))
                            return merged
                        }
                }

    /// <summary>
    /// Joins two results by a key using primary key of result1 and foreign key of result2 and merge function to 
    /// consolidate master ('Result1) values with lists of detail ('Result2) values.
    /// </summary>
    /// <param name="merge">
    /// Function consolidating master value ('Result1) with list of detail values ('Result2).
    /// </param>
    static member Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2 when 'Key: comparison>(merge: 'Result1 * 'Result2 list -> 'Result1): 
            ResultSpecifier<(RowsImpl.KeySpecifier<'PK2, 'Key> * 'Result2) seq> -> 
            ResultSpecifier<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq> -> 
            ResultSpecifier<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq> = 
        Results.Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2>(fun (r1: 'Result1, r2s: 'Result2 seq) -> merge(r1, r2s |> Seq.toList))

    /// <summary>
    /// Joins two results by a key using primary key of result1 and foreign key of result2 and merge function to 
    /// consolidate master ('Result1) values with arrays of detail ('Result2) values.
    /// </summary>
    /// <param name="merge">
    /// Function consolidating master value ('Result1) with array of detail values ('Result2).
    /// </param>
    static member Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2 when 'Key: comparison>(merge: 'Result1 * 'Result2 array -> 'Result1): 
            ResultSpecifier<(RowsImpl.KeySpecifier<'PK2, 'Key> * 'Result2) seq> -> 
            ResultSpecifier<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq> -> 
            ResultSpecifier<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq> = 
        Results.Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2>(fun (r1: 'Result1, r2s: 'Result2 seq) -> merge(r1, r2s |> Seq.toArray))

    static member private GetPropChain(expr: Expr) = 
        match expr with
        | PropertyGet (Some inner, property, _) -> Results.GetPropChain(inner) @ [ property ]
        | _ -> []

    static member private GenerateMerge(propChain: PropertyInfo list, target: Expression, replacement: Expression) = 
        match propChain with
        | [] -> replacement
        | changed :: remaining ->
            let propValues = 
                [| for prop in target.Type.GetProperties() do
                    if prop.Name <> changed.Name then
                        Expression.Property(target, prop) :> Expression
                    else    
                        Results.GenerateMerge(remaining, Expression.Property(target, changed), replacement)
                |]
            let constructor = target.Type.GetConstructor(propValues |> Array.map (fun expr -> expr.Type))
            Expression.New(constructor, propValues)            

    static member private GenerateMerge (target: Expr<'Result2>): Func<'Result1, 'Result2, 'Result1> = 
        let result1Param = Expression.Parameter(typeof<'Result1>)
        let result2Param = Expression.Parameter(typeof<'Result2>)
        let propChain = Results.GetPropChain(target) 
        let construct = Results.GenerateMerge(propChain, result1Param, result2Param)
        Expression.Lambda<Func<'Result1, 'Result2, 'Result1>>(construct, result1Param, result2Param).Compile()

    /// <summary>
    /// Joins two results by a key using primary key of result1 and foreign key of result2 and merge function to 
    /// consolidate master ('Result1) values with lists of detail ('Result2) values.
    /// </summary>
    /// <param name="target">
    /// The expression specifying a path to a property that should be updated with details values.
    /// </param>
    static member Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2 when 'Key: comparison>([<ReflectedDefinition>] target: Expr<'Result2 list>): 
            ResultSpecifier<(RowsImpl.KeySpecifier<'PK2, 'Key> * 'Result2) seq> -> 
            ResultSpecifier<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq> -> 
            ResultSpecifier<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq> = 
        let merge = Results.GenerateMerge(target)
        Results.Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2>(fun (r1: 'Result1, r2s: 'Result2 seq) -> merge.Invoke(r1, r2s |> Seq.toList))

    /// <summary>
    /// Joins two results by a key using primary key of result1 and foreign key of result2 and merge function to 
    /// consolidate master ('Result1) values with arrays of detail ('Result2) values.
    /// </summary>
    /// <param name="target">
    /// The expression specifying a path to a property that should be updated with details values.
    /// </param>
    static member Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2 when 'Key: comparison>([<ReflectedDefinition>] target: Expr<'Result2 array>): 
            ResultSpecifier<(RowsImpl.KeySpecifier<'PK2, 'Key> * 'Result2) seq> -> 
            ResultSpecifier<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq> -> 
            ResultSpecifier<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq> = 
        let merge = Results.GenerateMerge(target)
        Results.Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2>(fun (r1: 'Result1, r2s: 'Result2 seq) -> merge.Invoke(r1, r2s |> Seq.toArray))

    /// <summary>
    /// Joins two results by a key using primary key of result1 and foreign key of result2 and merge function to 
    /// consolidate master ('Result1) values with sequences of detail ('Result2) values.
    /// </summary>
    /// <param name="target">
    /// The expression specifying a path to a property that should be updated with details values.
    /// </param>
    static member Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2 when 'Key: comparison>(target: Expr<'Result2 seq>): 
            ResultSpecifier<(RowsImpl.KeySpecifier<'PK2, 'Key> * 'Result2) seq> -> 
            ResultSpecifier<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq> -> 
            ResultSpecifier<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq> = 
        let merge = Results.GenerateMerge(target)
        Results.Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2>(fun (r1: 'Result1, r2s: 'Result2 seq) -> merge.Invoke(r1, r2s))

    /// <summary>
    /// Removes a key specifier from a result.
    /// </summary>
    /// <param name="keyedResult">
    /// The source result builder.
    /// </param>
    static member Unkeyed<'PK, 'FK, 'Result>(keyedResult: IRowGetterProvider * IDataReader -> IResultReader<(RowsImpl.KeySpecifier<'PK, 'FK> * 'Result) seq>) = 
        Results.Map (Seq.map snd) keyedResult

    /// <summary>
    /// Groups list of tuples by first item and consilidates this item with list of corresponding second items using merge function.
    /// </summary>
    /// <param name="merge">
    /// Function merging first tuple element with list of corresponding second items.
    /// </param>
    static member Group (merge: 'Result1 -> 'Result2 list -> 'Result1) =
        fun (sourceResult: IRowGetterProvider * IDataReader -> IResultReader<('Result1 * 'Result2 option) list>) ->
            Results.Map (List.groupBy fst >> List.map (fun (r1, r1r2s) ->  merge r1 (r1r2s |> List.map snd |> List.choose id))) sourceResult

    /// <summary>
    /// Groups array of tuples by first item and consilidates this item with list of corresponding second items using merge function.
    /// </summary>
    /// <param name="merge">
    /// Function merging first tuple element with array of corresponding second items.
    /// </param>
    static member Group (merge: 'Result1 -> 'Result2 array -> 'Result1) =
        fun (sourceResult: IRowGetterProvider * IDataReader -> IResultReader<('Result1 * 'Result2 option) array>) ->
            Results.Map (Array.groupBy fst >> Array.map (fun (r1, r1r2s) -> merge r1 (r1r2s |> Array.map snd |> Array.choose id))) sourceResult

    /// <summary>
    /// Groups seq of tuples by first item and consilidates this item with list of corresponding second items using merge function.
    /// </summary>
    /// <param name="merge">
    /// Function merging first tuple element with seq of corresponding second items
    /// </param>
    static member Group (merge: 'Result1 -> 'Result2 seq -> 'Result1) =
        fun (sourceResult: IRowGetterProvider * IDataReader -> IResultReader<('Result1 * 'Result2 option) seq>) ->
            Results.Map (Seq.groupBy fst >> Seq.map (fun (r1, r1r2s) ->  merge r1 (r1r2s |> Seq.map snd |> Seq.choose id))) sourceResult

    /// <summary>
    /// Groups list of tuples by first item and consilidates this item with list of corresponding second items by assigning them to a specified property.
    /// </summary>
    /// <param name="target">
    /// The expression specifying a path to a property that should be updated with details values.
    /// </param>
    static member Group ([<ReflectedDefinition>] target: Expr<'Result2 list>): 
            ResultSpecifier<('Result1 * 'Result2 option) list> -> ResultSpecifier<'Result1 list> =
        let merge = Results.GenerateMerge(target)
        fun (sourceResult: IRowGetterProvider * IDataReader -> IResultReader<('Result1 * 'Result2 option) list>) ->
            Results.Group (fun  (r1: 'Result1) (r2l: 'Result2 list) -> merge.Invoke(r1, r2l)) sourceResult

    /// <summary>
    /// Groups array of tuples by first item and consilidates this item with an array of corresponding second items by assigning them to a specified property.
    /// </summary>
    /// <param name="target">
    /// The expression specifying a path to a property that should be updated with details values.
    /// </param>
    static member Group ([<ReflectedDefinition>] target: Expr<'Result2 array>):
            ResultSpecifier<('Result1 * 'Result2 option) array> -> ResultSpecifier<'Result1 array> =
        let merge = Results.GenerateMerge(target)
        fun (sourceResult: IRowGetterProvider * IDataReader -> IResultReader<('Result1 * 'Result2 option) array>) ->
            Results.Group (fun  (r1: 'Result1) (r2l: 'Result2 array) -> merge.Invoke(r1, r2l)) sourceResult

    /// <summary>
    /// Groups seq of tuples by first item and consilidates this item with seq of corresponding second items by assigning them to a specified property.
    /// </summary>
    /// <param name="target">
    /// The expression specifying a path to a property that should be updated with details values.
    /// </param>
    static member Group (target: Expr<'Result2 seq>):
            ResultSpecifier<('Result1 * 'Result2 option) seq> -> ResultSpecifier<'Result1 seq> =
        let merge = Results.GenerateMerge<'Result2 seq, 'Result1>(target)
        fun (sourceResult: IRowGetterProvider * IDataReader -> IResultReader<('Result1 * 'Result2 option) seq>) ->
            Results.Group (fun (r1: 'Result1) (r2l: 'Result2 seq) -> merge.Invoke(r1, r2l)) sourceResult

/// <summary>
/// Definitions allowing for applicative functor-like result combining.
/// </summary>
module MultipleResults = 

    type IAdvancer = 
        abstract member Advance: IDataReader -> unit
        abstract member AdvanceAsync: IDataReader -> Async<unit>

    /// <summary>
    /// Applies result builder containing ordinary result to result builder containing combiner function.
    /// </summary>
    /// <param name="multiple">
    /// The result builder of function type.
    /// </param>
    /// <param name="resultBuilder">
    /// The ordinary result builder to be appliied.
    /// </param>
    /// <param name="provider">
    /// The row getter provider
    /// </param>
    /// <param name="prototype">
    /// The prototype data reader.
    /// </param>
    let (<*>) (multiple: ResultSpecifier<'Result -> 'Next>) (resultBuilder: ResultSpecifier<'Result>): ResultSpecifier<'Next> =        

        fun (provider: IRowGetterProvider, prototype: IDataReader) ->
            let combiner = multiple(provider, prototype)
            (combiner :?> IAdvancer).Advance(prototype)
            let resultReader = resultBuilder(provider, prototype)
            { new IResultReader<'Next> with
                member __.Read(reader: IDataReader) = 
                    async {
                        let! combine = combiner.Read(reader)
                        do! (combiner :?> IAdvancer).AdvanceAsync(reader)
                        let! result = resultReader.Read(reader)
                        return combine(result)                    
                    }
              interface IAdvancer with
                member __.Advance(reader: IDataReader) = advance [typeof<'Next>] reader
                member __.AdvanceAsync(reader: IDataReader) = advanceAsync [typeof<'Next>] reader
            }
        
    type Results with  
                
        /// <summary>
        /// Creates an initial result builder containing combiner function.
        /// </summary>
        /// <param name="combiner">
        /// The combiner function.
        /// </param>
        static member Combine(combiner: 'ResultBuilder) =         
            fun (_: IRowGetterProvider, _: IDataReader)  ->
                { new IResultReader<'ResultBuilder> with
                    member __.Read(_: IDataReader) = async.Return combiner
                  interface IAdvancer with
                    member __.Advance(_: IDataReader) = ()
                    member __.AdvanceAsync(_: IDataReader) = async.Return ()
                } 
