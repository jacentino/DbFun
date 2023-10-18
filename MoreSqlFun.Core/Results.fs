namespace MoreSqlFun.Core.Builders

open System
open System.Data
open System.Linq.Expressions
open System.Reflection
open FSharp.Quotations
open FSharp.Quotations.Patterns


type IResultReader<'Result> = 
    abstract member Read: IDataReader -> 'Result

module MultipleResults = 

    type IAdvancer = 
        abstract member Advance: IDataReader -> unit

    let (<*>) (multiple: IRowGetterProvider * IDataReader -> IResultReader<'Result -> 'Next>) (resultBuilder: IRowGetterProvider * IDataReader -> IResultReader<'Result>): IRowGetterProvider * IDataReader -> IResultReader<'Next> =        
        
        let advance(combiner: IResultReader<'Result -> 'Next>, reader: IDataReader) = 
            (combiner :?> IAdvancer).Advance(reader)

        fun (provider: IRowGetterProvider, prototype: IDataReader) ->
            let combiner = multiple(provider, prototype)
            advance(combiner, prototype)
            let resultReader = resultBuilder(provider, prototype)
            { new IResultReader<'Next> with
                member __.Read(reader: IDataReader) = 
                    let combine = combiner.Read(reader)
                    advance(combiner, reader)
                    let result = resultReader.Read(reader)
                    combine(result)                    
              interface IAdvancer with
                member __.Advance(reader: IDataReader) = 
                    if not(reader.NextResult()) then
                        failwith "Not enough results"                         
            }
        
    type Results() = 
                
        static member Combine(combiner: 'ResultBuilder) =         
            fun (_: IRowGetterProvider, _: IDataReader)  ->
                { new IResultReader<'ResultBuilder> with
                    member __.Read(_: IDataReader) = combiner
                  interface IAdvancer with
                    member __.Advance(_: IDataReader) = ()
                } 


type Results() = 

    static let advance (resultTypes: Type seq) (reader: IDataReader) = 
        if not (reader.NextResult()) then
            failwithf "Not enough results when reading %A" resultTypes

    static member private EmptyReader<'Result>() =
        { new IResultReader<'Result> with
                member __.Read(_: IDataReader): 'Result = 
                    Unchecked.defaultof<'Result>
        }        

    static member Unit: IRowGetterProvider * IDataReader -> IResultReader<unit> = 
        fun (_: IRowGetterProvider, _: IDataReader) -> Results.EmptyReader<unit>()
            
    static member One<'Result> (rowBuilder: IRowGetterProvider * IDataRecord -> IRowGetter<'Result>): IRowGetterProvider * IDataReader -> IResultReader<'Result> = 
        fun (provider: IRowGetterProvider, prototype: IDataReader) ->
            let getter = rowBuilder(provider, prototype)
            { new IResultReader<'Result> with
                member __.Read(reader: IDataReader) = 
                    if reader.Read() then
                        getter.Get(reader)    
                    else
                        failwithf "Cannot read %A object, resultset is empty. Use option type." typeof<'Result>
            }

    static member One<'Result> (name: string): IRowGetterProvider * IDataReader -> IResultReader<'Result> = 
        fun (provider: IRowGetterProvider, prototype: IDataReader) ->
            let getter = provider.Getter(name, prototype)
            { new IResultReader<'Result> with
                member __.Read(reader: IDataReader) = 
                    if reader.Read() then
                        getter.Get(reader)    
                    else
                        failwithf "Cannot read %A object, resultset is empty. Use option type." typeof<'Result>
            }

    static member TryOne<'Result> (rowBuilder: IRowGetterProvider * IDataRecord -> IRowGetter<'Result>): IRowGetterProvider * IDataReader -> IResultReader<'Result option> = 
        fun (provider: IRowGetterProvider, prototype: IDataReader)  ->
            let getter = rowBuilder(provider, prototype)
            { new IResultReader<'Result option> with
                member __.Read(reader: IDataReader) = 
                    if reader.Read() then
                        Some <| getter.Get(reader)    
                    else
                        None
            }

    static member TryOne<'Result> (name: string): IRowGetterProvider * IDataReader -> IResultReader<'Result option>  = 
        fun (provider: IRowGetterProvider, prototype: IDataReader)  ->
            let getter = provider.Getter(name, prototype)
            { new IResultReader<'Result option> with
                member __.Read(reader: IDataReader) = 
                    if reader.Read() then
                        Some <| getter.Get(reader)    
                    else
                        None
            }

    static member Many<'Result> (rowBuilder: IRowGetterProvider * IDataRecord -> IRowGetter<'Result>): IRowGetterProvider * IDataReader -> IResultReader<'Result seq> = 
        fun (provider: IRowGetterProvider, prototype: IDataReader)  ->
            let getter = rowBuilder(provider, prototype)
            { new IResultReader<'Result seq> with
                member __.Read(reader: IDataReader) = 
                    [ while reader.Read() do
                        getter.Get(reader)
                    ]
            }

    static member Many<'Result> (name: string): IRowGetterProvider * IDataReader -> IResultReader<'Result seq> = 
        fun (provider: IRowGetterProvider, prototype: IDataReader)  ->
            let getter = provider.Getter<'Result>(name, prototype)
            { new IResultReader<'Result seq> with
                member __.Read(reader: IDataReader) = 
                    [ while reader.Read() do
                        getter.Get(reader)
                    ]
            }

    static member Keyed<'Primary, 'Foreign, 'Result>(primaryName: string, foreignName: string, resultName: string) = 
        Results.Many(Rows.Keyed<'Primary, 'Foreign, 'Result>(primaryName, foreignName, resultName))

    static member Keyed<'Primary, 'Foreign, 'Result>(primaryName: string, foreignName: string, result: IRowGetterProvider * IDataRecord -> IRowGetter<'Result>) = 
        Results.Many(Rows.Keyed<'Primary, 'Foreign, 'Result>(primaryName, foreignName, result))

    static member Keyed<'Primary, 'Foreign, 'Result>(
            primary: IRowGetterProvider * IDataRecord -> IRowGetter<'Primary>, 
            foreign: IRowGetterProvider * IDataRecord -> IRowGetter<'Foreign>, 
            result: IRowGetterProvider * IDataRecord -> IRowGetter<'Result>) = 
        Results.Many(Rows.Keyed<'Primary, 'Foreign, 'Result>(primary, foreign, result))

    static member PKeyed<'Primary, 'Result>(primaryName: string, resultName: string) = 
        Results.Many(Rows.PKeyed<'Primary, 'Result>(primaryName, resultName))

    static member PKeyed<'Primary, 'Result>(primaryName: string, result: IRowGetterProvider * IDataRecord -> IRowGetter<'Result>) = 
        Results.Many(Rows.PKeyed<'Primary, 'Result>(primaryName, result))

    static member PKeyed<'Primary, 'Result>(
            primary: IRowGetterProvider * IDataRecord -> IRowGetter<'Primary>, 
            result: IRowGetterProvider * IDataRecord -> IRowGetter<'Result>) = 
        Results.Many(Rows.PKeyed<'Primary, 'Result>(primary, result))

    static member FKeyed<'Foreign, 'Result>(foreignName: string, resultName: string) = 
        Results.Many(Rows.FKeyed<'Foreign, 'Result>(foreignName, resultName))

    static member FKeyed<'Foreign, 'Result>(foreignName: string, result: IRowGetterProvider * IDataRecord -> IRowGetter<'Result>) = 
        Results.Many(Rows.FKeyed<'Foreign, 'Result>(foreignName, result))

    static member FKeyed<'Foreign, 'Result>(
            foreign: IRowGetterProvider * IDataRecord -> IRowGetter<'Foreign>, 
            result: IRowGetterProvider * IDataRecord -> IRowGetter<'Result>) = 
        Results.Many(Rows.FKeyed<'Foreign, 'Result>(foreign, result))

    static member Multiple<'Result1, 'Result2>(
            builder1: IRowGetterProvider * IDataReader -> IResultReader<'Result1>, 
            builder2: IRowGetterProvider * IDataReader -> IResultReader<'Result2>)
            : IRowGetterProvider * IDataReader -> IResultReader<'Result1 * 'Result2> = 
        fun (provider: IRowGetterProvider, prototype: IDataReader)  ->
            let advance = advance [ typeof<'Result1>; typeof<'Result2> ]
            let reader1 = builder1(provider, prototype)
            advance(prototype)
            let reader2 = builder2(provider, prototype)
            { new IResultReader<'Result1 * 'Result2> with
                member __.Read(reader: IDataReader) = 
                    let result1 = reader1.Read(reader)
                    advance(reader)
                    let result2 = reader2.Read(reader)
                    result1, result2
            }
            
    static member Map<'Source, 'Target>(mapper: 'Source -> 'Target) (source: IRowGetterProvider * IDataReader -> IResultReader<'Source>): IRowGetterProvider * IDataReader -> IResultReader<'Target> = 
        fun (provider: IRowGetterProvider, prototype: IDataReader) ->
            let srcReader = source(provider, prototype)
            { new IResultReader<'Target> with
                member __.Read(reader: IDataReader) = 
                    let result = srcReader.Read(reader)
                    mapper(result)
            }

    static member Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2 when 'Key: comparison>(merge: 'Result1 * 'Result2 seq -> 'Result1): 
            (IRowGetterProvider * IDataReader -> IResultReader<(RowsImpl.KeySpecifier<'PK2, 'Key> * 'Result2) seq>) -> 
            (IRowGetterProvider * IDataReader -> IResultReader<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq>) -> 
            IRowGetterProvider * IDataReader -> IResultReader<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq> = 
        fun (builder2: IRowGetterProvider * IDataReader -> IResultReader<(RowsImpl.KeySpecifier<'PK2, 'Key> * 'Result2) seq>) 
            (builder1: IRowGetterProvider * IDataReader -> IResultReader<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq>) 
            (provider: IRowGetterProvider, prototype: IDataReader) ->
                let advance = advance [ typeof<'Key * 'Result1>; typeof<'Key * 'Result2> ]
                let reader1 = builder1(provider, prototype)
                advance(prototype)
                let reader2 = builder2(provider, prototype)
                { new IResultReader<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq> with
                    member __.Read(reader: IDataReader) = 
                        let result1 = reader1.Read(reader) 
                        advance(reader)
                        let result2 = reader2.Read(reader) |> Seq.groupBy (fun (k, r) -> k.Foreign) |> readOnlyDict
                        let merged = result1 |> Seq.map (fun (k, r1) -> k, merge(r1, result2.TryGetValue k.Primary |> (function (true, r2s) -> r2s |> Seq.map snd | (false, _) -> Seq.empty)))
                        merged
                }

    static member Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2 when 'Key: comparison>(merge: 'Result1 * 'Result2 list -> 'Result1): 
            (IRowGetterProvider * IDataReader -> IResultReader<(RowsImpl.KeySpecifier<'PK2, 'Key> * 'Result2) seq>) -> 
            (IRowGetterProvider * IDataReader -> IResultReader<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq>) -> 
            IRowGetterProvider * IDataReader -> IResultReader<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq> = 
        Results.Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2>(fun (r1: 'Result1, r2s: 'Result2 seq) -> merge(r1, r2s |> Seq.toList))

    static member Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2 when 'Key: comparison>(merge: 'Result1 * 'Result2 array -> 'Result1): 
            (IRowGetterProvider * IDataReader -> IResultReader<(RowsImpl.KeySpecifier<'PK2, 'Key> * 'Result2) seq>) -> 
            (IRowGetterProvider * IDataReader -> IResultReader<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq>) -> 
            IRowGetterProvider * IDataReader -> IResultReader<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq> = 
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
            (IRowGetterProvider * IDataReader -> IResultReader<(RowsImpl.KeySpecifier<'PK2, 'Key> * 'Result2) seq>) -> 
            (IRowGetterProvider * IDataReader -> IResultReader<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq>) -> 
            IRowGetterProvider * IDataReader -> IResultReader<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq> = 
        let merge = Results.GenerateMerge(target)
        Results.Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2>(fun (r1: 'Result1, r2s: 'Result2 seq) -> merge.Invoke(r1, r2s |> Seq.toList))

    static member Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2 when 'Key: comparison>([<ReflectedDefinition>] target: Expr<'Result2 array>): 
            (IRowGetterProvider * IDataReader -> IResultReader<(RowsImpl.KeySpecifier<'PK2, 'Key> * 'Result2) seq>) -> 
            (IRowGetterProvider * IDataReader -> IResultReader<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq>) -> 
            IRowGetterProvider * IDataReader -> IResultReader<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq> = 
        let merge = Results.GenerateMerge(target)
        Results.Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2>(fun (r1: 'Result1, r2s: 'Result2 seq) -> merge.Invoke(r1, r2s |> Seq.toArray))

    static member Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2 when 'Key: comparison>(target: Expr<'Result2 seq>): 
            (IRowGetterProvider * IDataReader -> IResultReader<(RowsImpl.KeySpecifier<'PK2, 'Key> * 'Result2) seq>) -> 
            (IRowGetterProvider * IDataReader -> IResultReader<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq>) -> 
            IRowGetterProvider * IDataReader -> IResultReader<(RowsImpl.KeySpecifier<'Key, 'FK1> * 'Result1) seq> = 
        let merge = Results.GenerateMerge(target)
        Results.Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2>(fun (r1: 'Result1, r2s: 'Result2 seq) -> merge.Invoke(r1, r2s))

    static member Unkeyed<'PK, 'FK, 'Result>(keyedResult: IRowGetterProvider * IDataReader -> IResultReader<(RowsImpl.KeySpecifier<'PK, 'FK> * 'Result) seq>) = 
        Results.Map (Seq.map snd) keyedResult
