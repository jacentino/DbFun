namespace DbFun.Core.Builders

open System
open System.Data
open System.Linq.Expressions
open System.Reflection
open FSharp.Quotations
open FSharp.Quotations.Patterns
open DbFun.Core

type IResultReader<'Result> = 
    abstract member Read: IDataReader -> Async<'Result>

type BuildResultReader<'Result> = IRowGetterProvider * IDataReader -> IResultReader<'Result>

module MultipleResults = 

    type IAdvancer = 
        abstract member Advance: IDataReader -> unit
        abstract member AdvanceAsync: IDataReader -> Async<unit>

    let (<*>) (multiple: BuildResultReader<'Result -> 'Next>) (resultBuilder: BuildResultReader<'Result>): BuildResultReader<'Next> =        
        
        let advance(combiner: IResultReader<'Result -> 'Next>, reader: IDataReader) = 
            (combiner :?> IAdvancer).Advance(reader)

        let advanceAsync(combiner: IResultReader<'Result -> 'Next>, reader: IDataReader) = 
            (combiner :?> IAdvancer).AdvanceAsync(reader)

        fun (provider: IRowGetterProvider, prototype: IDataReader) ->
            let combiner = multiple(provider, prototype)
            advance(combiner, prototype)
            let resultReader = resultBuilder(provider, prototype)
            { new IResultReader<'Next> with
                member __.Read(reader: IDataReader) = 
                    async {
                        let! combine = combiner.Read(reader)
                        do! advanceAsync(combiner, reader)
                        let! result = resultReader.Read(reader)
                        return combine(result)                    
                    }
              interface IAdvancer with
                member __.Advance(reader: IDataReader) = 
                    if not(reader.NextResult()) then
                        failwith "Not enough results"                         
                member __.AdvanceAsync(reader: IDataReader) = 
                    async {
                        let! exists = Executor.nextResultAsync(reader)
                        if not exists then
                            failwith "Not enough results"                         
                    }
            }
        
    type Results() = 
                
        static member Combine(combiner: 'ResultBuilder) =         
            fun (_: IRowGetterProvider, _: IDataReader)  ->
                { new IResultReader<'ResultBuilder> with
                    member __.Read(_: IDataReader) = async.Return combiner
                  interface IAdvancer with
                    member __.Advance(_: IDataReader) = ()
                    member __.AdvanceAsync(_: IDataReader) = async.Return ()
                } 


type Results() = 

    static let advance (resultTypes: Type seq) (reader: IDataReader) = 
        if not (reader.NextResult()) then
            failwithf "Not enough results when reading %A" resultTypes

    static let advanceAsync (resultTypes: Type seq) (reader: IDataReader) = 
        async {
            let! exists = Executor.nextResultAsync(reader)
            if not exists then
                failwithf "Not enough results when reading %A" resultTypes
        }

    static member private EmptyReader<'Result>() =
        { new IResultReader<'Result> with
                member __.Read(_: IDataReader): Async<'Result> = 
                    async.Return Unchecked.defaultof<'Result>
        }        

    static member Unit: BuildResultReader<unit> = 
        fun (_: IRowGetterProvider, _: IDataReader) -> Results.EmptyReader<unit>()
            
    static member Single<'Result> (rowBuilder: IRowGetterProvider * IDataRecord -> IRowGetter<'Result>): IRowGetterProvider * IDataReader -> IResultReader<'Result> = 
        fun (provider: IRowGetterProvider, prototype: IDataReader) ->
            let getter = rowBuilder(provider, prototype)
            { new IResultReader<'Result> with
                member __.Read(reader: IDataReader) = 
                    if reader.Read() then
                        async.Return (getter.Get(reader))
                    else
                        failwithf "Cannot read %A object, resultset is empty. Use option type." typeof<'Result>
            }

    static member Single<'Result> (name: string): BuildResultReader<'Result> = 
        fun (provider: IRowGetterProvider, prototype: IDataReader) ->
            let getter = provider.Getter(name, prototype)
            { new IResultReader<'Result> with
                member __.Read(reader: IDataReader) = 
                    if reader.Read() then
                        async.Return (getter.Get(reader))
                    else
                        failwithf "Cannot read %A object, resultset is empty. Use option type." typeof<'Result>
            }

    static member Optional<'Result> (rowBuilder: BuildRowGetter<'Result>): BuildResultReader<'Result option> = 
        fun (provider: IRowGetterProvider, prototype: IDataReader)  ->
            let getter = rowBuilder(provider, prototype)
            { new IResultReader<'Result option> with
                member __.Read(reader: IDataReader) = 
                    if reader.Read() then
                        async.Return (Some <| getter.Get(reader))
                    else
                        async.Return None
            }

    static member Optional<'Result> (name: string): BuildResultReader<'Result option>  = 
        fun (provider: IRowGetterProvider, prototype: IDataReader)  ->
            let getter = provider.Getter(name, prototype)
            { new IResultReader<'Result option> with
                member __.Read(reader: IDataReader) = 
                    if reader.Read() then
                        async.Return (Some <| getter.Get(reader))
                    else
                        async.Return None
            }

    static member Seq<'Result> (rowBuilder: BuildRowGetter<'Result>): BuildResultReader<'Result seq> = 
        fun (provider: IRowGetterProvider, prototype: IDataReader)  ->
            let getter = rowBuilder(provider, prototype)
            { new IResultReader<'Result seq> with
                member __.Read(reader: IDataReader) = 
                    async.Return 
                        [ while reader.Read() do
                            getter.Get(reader)
                        ]
            }

    static member Seq<'Result> (name: string): BuildResultReader<'Result seq> = 
        fun (provider: IRowGetterProvider, prototype: IDataReader)  ->
            let getter = provider.Getter<'Result>(name, prototype)
            { new IResultReader<'Result seq> with
                member __.Read(reader: IDataReader) = 
                    async.Return 
                        [ while reader.Read() do
                            getter.Get(reader)
                        ]
            }

    static member List<'Result> (rowBuilder: BuildRowGetter<'Result>): BuildResultReader<'Result list> = 
        fun (provider: IRowGetterProvider, prototype: IDataReader)  ->
            let getter = rowBuilder(provider, prototype)
            { new IResultReader<'Result list> with
                member __.Read(reader: IDataReader) = 
                    async.Return 
                        [ while reader.Read() do
                            getter.Get(reader)
                        ]
            }

    static member List<'Result> (name: string): BuildResultReader<'Result list> = 
        fun (provider: IRowGetterProvider, prototype: IDataReader)  ->
            let getter = provider.Getter<'Result>(name, prototype)
            { new IResultReader<'Result list> with
                member __.Read(reader: IDataReader) = 
                    async.Return 
                        [ while reader.Read() do
                            getter.Get(reader)
                        ]
            }

    static member Array<'Result> (rowBuilder: BuildRowGetter<'Result>): BuildResultReader<'Result array> = 
        fun (provider: IRowGetterProvider, prototype: IDataReader)  ->
            let getter = rowBuilder(provider, prototype)
            { new IResultReader<'Result array> with
                member __.Read(reader: IDataReader) = 
                    async.Return 
                        [| while reader.Read() do
                            getter.Get(reader)
                        |]
            }

    static member Array<'Result> (name: string): BuildResultReader<'Result array> = 
        fun (provider: IRowGetterProvider, prototype: IDataReader)  ->
            let getter = provider.Getter<'Result>(name, prototype)
            { new IResultReader<'Result array> with
                member __.Read(reader: IDataReader) = 
                    async.Return 
                        [| while reader.Read() do
                            getter.Get(reader)
                        |]
            }

    static member Keyed<'Primary, 'Foreign, 'Result>(primaryName: string, foreignName: string, resultName: string) = 
        Results.Seq(Rows.Keyed<'Primary, 'Foreign, 'Result>(primaryName, foreignName, resultName))

    static member Keyed<'Primary, 'Foreign, 'Result>(primaryName: string, foreignName: string, result: BuildRowGetter<'Result>) = 
        Results.Seq(Rows.Keyed<'Primary, 'Foreign, 'Result>(primaryName, foreignName, result))

    static member Keyed<'Primary, 'Foreign, 'Result>(primary: BuildRowGetter<'Primary>, foreign: BuildRowGetter<'Foreign>, result: BuildRowGetter<'Result>) = 
        Results.Seq(Rows.Keyed<'Primary, 'Foreign, 'Result>(primary, foreign, result))

    static member PKeyed<'Primary, 'Result>(primaryName: string, resultName: string) = 
        Results.Seq(Rows.PKeyed<'Primary, 'Result>(primaryName, resultName))

    static member PKeyed<'Primary, 'Result>(primaryName: string, result: BuildRowGetter<'Result>) = 
        Results.Seq(Rows.PKeyed<'Primary, 'Result>(primaryName, result))

    static member PKeyed<'Primary, 'Result>(primary: BuildRowGetter<'Primary>, result: BuildRowGetter<'Result>) = 
        Results.Seq(Rows.PKeyed<'Primary, 'Result>(primary, result))

    static member FKeyed<'Foreign, 'Result>(foreignName: string, resultName: string) = 
        Results.Seq(Rows.FKeyed<'Foreign, 'Result>(foreignName, resultName))

    static member FKeyed<'Foreign, 'Result>(foreignName: string, result: BuildRowGetter<'Result>) = 
        Results.Seq(Rows.FKeyed<'Foreign, 'Result>(foreignName, result))

    static member FKeyed<'Foreign, 'Result>(foreign: BuildRowGetter<'Foreign>, result: BuildRowGetter<'Result>) = 
        Results.Seq(Rows.FKeyed<'Foreign, 'Result>(foreign, result))

    static member Multiple<'Result1, 'Result2>(builder1: BuildResultReader<'Result1>, builder2: BuildResultReader<'Result2>): BuildResultReader<'Result1 * 'Result2> = 
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
            
    static member Multiple<'Result1, 'Result2, 'Result3>(builder1: BuildResultReader<'Result1>, builder2: BuildResultReader<'Result2>, builder3: BuildResultReader<'Result3>)
                    : BuildResultReader<'Result1 * 'Result2 * 'Result3> = 
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
            
    static member Multiple<'Result1, 'Result2, 'Result3, 'Result4>(builder1: BuildResultReader<'Result1>, builder2: BuildResultReader<'Result2>, builder3: BuildResultReader<'Result3>, builder4: BuildResultReader<'Result4>)
                    : BuildResultReader<'Result1 * 'Result2 * 'Result3 * 'Result4> = 
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
            
    static member Multiple<'Result1, 'Result2, 'Result3, 'Result4, 'Result5>(
            builder1: BuildResultReader<'Result1>, 
            builder2: BuildResultReader<'Result2>, 
            builder3: BuildResultReader<'Result3>, 
            builder4: BuildResultReader<'Result4>, 
            builder5: BuildResultReader<'Result5>)
            : BuildResultReader<'Result1 * 'Result2 * 'Result3 * 'Result4 * 'Result5> = 
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
            
    static member Map<'Source, 'Target>(mapper: 'Source -> 'Target) (source: BuildResultReader<'Source>): BuildResultReader<'Target> = 
        fun (provider: IRowGetterProvider, prototype: IDataReader) ->
            let srcReader = source(provider, prototype)
            { new IResultReader<'Target> with
                member __.Read(reader: IDataReader) = 
                    async {
                        let! result = srcReader.Read(reader)
                        return mapper(result)
                    }
            }

    static member Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2 when 'Key: comparison>(merge: 'Result1 * 'Result2 seq -> 'Result1): 
            BuildResultReader<(RowsImpl.KeySpecifier<'PK2, 'Key> * 'Result2) seq> -> 
            BuildResultReader<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq> -> 
            BuildResultReader<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq> = 
        fun (builder2: BuildResultReader<(RowsImpl.KeySpecifier<'PK2, 'Key> * 'Result2) seq>) 
            (builder1: BuildResultReader<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq>) 
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

    static member Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2 when 'Key: comparison>(merge: 'Result1 * 'Result2 list -> 'Result1): 
            BuildResultReader<(RowsImpl.KeySpecifier<'PK2, 'Key> * 'Result2) seq> -> 
            BuildResultReader<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq> -> 
            BuildResultReader<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq> = 
        Results.Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2>(fun (r1: 'Result1, r2s: 'Result2 seq) -> merge(r1, r2s |> Seq.toList))

    static member Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2 when 'Key: comparison>(merge: 'Result1 * 'Result2 array -> 'Result1): 
            BuildResultReader<(RowsImpl.KeySpecifier<'PK2, 'Key> * 'Result2) seq> -> 
            BuildResultReader<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq> -> 
            BuildResultReader<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq> = 
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

    static member Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2 when 'Key: comparison>([<ReflectedDefinition>] target: Expr<'Result2 list>): 
            BuildResultReader<(RowsImpl.KeySpecifier<'PK2, 'Key> * 'Result2) seq> -> 
            BuildResultReader<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq> -> 
            BuildResultReader<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq> = 
        let merge = Results.GenerateMerge(target)
        Results.Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2>(fun (r1: 'Result1, r2s: 'Result2 seq) -> merge.Invoke(r1, r2s |> Seq.toList))

    static member Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2 when 'Key: comparison>([<ReflectedDefinition>] target: Expr<'Result2 array>): 
            BuildResultReader<(RowsImpl.KeySpecifier<'PK2, 'Key> * 'Result2) seq> -> 
            BuildResultReader<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq> -> 
            BuildResultReader<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq> = 
        let merge = Results.GenerateMerge(target)
        Results.Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2>(fun (r1: 'Result1, r2s: 'Result2 seq) -> merge.Invoke(r1, r2s |> Seq.toArray))

    static member Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2 when 'Key: comparison>(target: Expr<'Result2 seq>): 
            BuildResultReader<(RowsImpl.KeySpecifier<'PK2, 'Key> * 'Result2) seq> -> 
            BuildResultReader<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq> -> 
            BuildResultReader<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq> = 
        let merge = Results.GenerateMerge(target)
        Results.Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2>(fun (r1: 'Result1, r2s: 'Result2 seq) -> merge.Invoke(r1, r2s))

    static member Unkeyed<'PK, 'FK, 'Result>(keyedResult: IRowGetterProvider * IDataReader -> IResultReader<(RowsImpl.KeySpecifier<'PK, 'FK> * 'Result) seq>) = 
        Results.Map (Seq.map snd) keyedResult
