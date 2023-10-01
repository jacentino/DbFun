namespace MoreSqlFun.Core.Builders

open System
open System.Data
open System.Linq.Expressions
open System.Reflection
open FSharp.Quotations
open FSharp.Quotations.Patterns


type IResultReader<'Result> = 
    abstract member Read: IDataReader -> 'Result


type ResultBuilder(rowGetterProvider: IRowGetterProvider) = 

    member __.Unit: IDataReader -> IResultReader<unit seq> = 
        fun (_: IDataReader) ->
            { new IResultReader<unit seq> with
                  member __.Read(_: IDataReader): unit seq = 
                      Seq.empty
            }

    member __.One<'Result> (rowBuilder: IDataRecord -> IRowGetter<'Result>): IDataReader -> IResultReader<'Result> = 
        fun (prototype: IDataReader) ->
            let getter = rowBuilder(prototype)
            { new IResultReader<'Result> with
                member __.Read(reader: IDataReader) = 
                    if reader.Read() then
                        getter.Get(reader)    
                    else
                        failwithf "Cannot read %A object, resultset is empty. Use option type." typeof<'Result>
            }

    member __.One<'Result> (name: string): IDataReader -> IResultReader<'Result> = 
        fun (prototype: IDataReader) ->
            let getter = rowGetterProvider.Getter(name, prototype)
            { new IResultReader<'Result> with
                member __.Read(reader: IDataReader) = 
                    if reader.Read() then
                        getter.Get(reader)    
                    else
                        failwithf "Cannot read %A object, resultset is empty. Use option type." typeof<'Result>
            }

    member __.TryOne<'Result> (rowBuilder: IDataRecord -> IRowGetter<'Result>): IDataReader -> IResultReader<'Result option> = 
        fun (prototype: IDataReader)  ->
            let getter = rowBuilder(prototype)
            { new IResultReader<'Result option> with
                member __.Read(reader: IDataReader) = 
                    if reader.Read() then
                        Some <| getter.Get(reader)    
                    else
                        None
            }

    member __.TryOne<'Result> (name: string): IDataReader -> IResultReader<'Result option>  = 
        fun (prototype: IDataReader)  ->
            let getter = rowGetterProvider.Getter(name, prototype)
            { new IResultReader<'Result option> with
                member __.Read(reader: IDataReader) = 
                    if reader.Read() then
                        Some <| getter.Get(reader)    
                    else
                        None
            }

    member __.Many<'Result> (rowBuilder: IDataRecord -> IRowGetter<'Result>): IDataReader -> IResultReader<'Result seq> = 
        fun (prototype: IDataReader)  ->
            let getter = rowBuilder(prototype)
            { new IResultReader<'Result seq> with
                member __.Read(reader: IDataReader) = 
                    [ while reader.Read() do
                        getter.Get(reader)
                    ]
            }

    member __.Many<'Result> (name: string): IDataReader -> IResultReader<'Result seq> = 
        fun (prototype: IDataReader)  ->
            let getter = rowGetterProvider.Getter<'Result>(name, prototype)
            { new IResultReader<'Result seq> with
                member __.Read(reader: IDataReader) = 
                    [ while reader.Read() do
                        getter.Get(reader)
                    ]
            }

    member __.Multiple<'Result1, 'Result2>(builder1: IDataReader -> IResultReader<'Result1>, builder2: IDataReader -> IResultReader<'Result2>): IDataReader -> IResultReader<'Result1 * 'Result2> = 
        fun (prototype: IDataReader)  ->
            let reader1 = builder1(prototype)
            if not (prototype.NextResult()) then
                failwithf "Not enough results when reading %A, %A" typeof<'Result1> typeof<'Result2>
            let reader2 = builder2(prototype)
            { new IResultReader<'Result1 * 'Result2> with
                member __.Read(reader: IDataReader) = 
                    let result1 = reader1.Read(reader)
                    if not (reader.NextResult()) then
                        failwithf "Not enough results when reading %A, %A" typeof<'Result1> typeof<'Result2>
                    let result2 = reader2.Read(reader)
                    result1, result2
            }
            
    member __.Map<'Source, 'Target>(mapper: 'Source -> 'Target) (source: IDataReader -> IResultReader<'Source>): IDataReader -> IResultReader<'Target> = 
        fun (prototype: IDataReader) ->
            let srcReader = source(prototype)
            { new IResultReader<'Target> with
                member __.Read(reader: IDataReader) = 
                    let result = srcReader.Read(reader)
                    mapper(result)
            }

    member __.Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2 when 'Key: comparison>(merge: 'Result1 * 'Result2 seq -> 'Result1): 
            (IDataReader -> IResultReader<(Rows.KeySpecifier<'PK2, 'Key> * 'Result2) seq>) -> 
            (IDataReader -> IResultReader<(Rows.KeySpecifier<'Key, 'FK1> * 'Result1) seq>) -> 
            IDataReader -> 
            IResultReader<(Rows.KeySpecifier<'Key, 'FK1> * 'Result1) seq> = 
        fun (builder2: IDataReader -> IResultReader<(Rows.KeySpecifier<'PK2, 'Key> * 'Result2) seq>) 
            (builder1: IDataReader -> IResultReader<(Rows.KeySpecifier<'Key, 'FK1> * 'Result1) seq>) 
            (prototype: IDataReader) ->
                let reader1 = builder1(prototype)
                if not (prototype.NextResult()) then
                    failwithf "Not enough results when reading %A, %A" typeof<'Key * 'Result1> typeof<'Key * 'Result2>
                let reader2 = builder2(prototype)
                { new IResultReader<(Rows.KeySpecifier<'Key, 'FK1> * 'Result1) seq> with
                    member __.Read(reader: IDataReader) = 
                        let result1 = reader1.Read(reader) 
                        if not (reader.NextResult()) then
                            failwithf "Not enough results when reading %A, %A" typeof<'Result1> typeof<'Result2>
                        let result2 = reader2.Read(reader) |> Seq.groupBy (fun (k, r) -> k.Foreign) |> readOnlyDict
                        let merged = result1 |> Seq.map (fun (k, r1) -> k, merge(r1, result2.TryGetValue k.Primary |> (function (true, r2s) -> r2s |> Seq.map snd | (false, _) -> Seq.empty)))
                        merged
                }

    member this.Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2 when 'Key: comparison>(merge: 'Result1 * 'Result2 list -> 'Result1): 
            (IDataReader -> IResultReader<(Rows.KeySpecifier<'PK2, 'Key> * 'Result2) seq>) -> 
            (IDataReader -> IResultReader<(Rows.KeySpecifier<'Key, 'FK1> * 'Result1) seq>) -> 
            IDataReader -> 
            IResultReader<(Rows.KeySpecifier<'Key, 'FK1> * 'Result1) seq> = 
        this.Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2>(fun (r1: 'Result1, r2s: 'Result2 seq) -> merge(r1, r2s |> Seq.toList))

    member this.Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2 when 'Key: comparison>(merge: 'Result1 * 'Result2 array -> 'Result1): 
            (IDataReader -> IResultReader<(Rows.KeySpecifier<'PK2, 'Key> * 'Result2) seq>) -> 
            (IDataReader -> IResultReader<(Rows.KeySpecifier<'Key, 'FK1> * 'Result1) seq>) -> 
            IDataReader -> 
            IResultReader<(Rows.KeySpecifier<'Key, 'FK1> * 'Result1) seq> = 
        this.Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2>(fun (r1: 'Result1, r2s: 'Result2 seq) -> merge(r1, r2s |> Seq.toArray))

    member private this.GetPropChain(expr: Expr) = 
        match expr with
        | PropertyGet (Some inner, property, _) -> property :: this.GetPropChain(inner)
        | _ -> []

    member private this.GenerateMerge(propChain: PropertyInfo list, target: Expression, replacement: Expression) = 
        match propChain with
        | [] -> replacement
        | changed :: remaining ->
            let propValues = 
                [| for prop in target.Type.GetProperties() do
                    if prop.Name <> changed.Name then
                        Expression.Property(target, prop) :> Expression
                    else    
                        this.GenerateMerge(remaining, Expression.Property(target, changed), replacement)
                |]
            let constructor = target.Type.GetConstructor(propValues |> Array.map (fun expr -> expr.Type))
            Expression.New(constructor, propValues)            

    member private this.GenerateMerge (target: Expr<'Result2>): Func<'Result1, 'Result2, 'Result1> = 
        let result1Param = Expression.Parameter(typeof<'Result1>)
        let result2Param = Expression.Parameter(typeof<'Result2>)
        let propChain = this.GetPropChain(target) 
        let construct = this.GenerateMerge(propChain, result1Param, result2Param)
        Expression.Lambda<Func<'Result1, 'Result2, 'Result1>>(construct, result1Param, result2Param).Compile()

    member this.Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2 when 'Key: comparison>(target: Expr<'Result2 list>): 
            (IDataReader -> IResultReader<(Rows.KeySpecifier<'PK2, 'Key> * 'Result2) seq>) -> 
            (IDataReader -> IResultReader<(Rows.KeySpecifier<'Key, 'FK1> * 'Result1) seq>) -> 
            IDataReader -> 
            IResultReader<(Rows.KeySpecifier<'Key, 'FK1> * 'Result1) seq> = 
        let merge = this.GenerateMerge(target)
        this.Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2>(fun (r1: 'Result1, r2s: 'Result2 seq) -> merge.Invoke(r1, r2s |> Seq.toList))

    member this.Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2 when 'Key: comparison>(target: Expr<'Result2 array>): 
            (IDataReader -> IResultReader<(Rows.KeySpecifier<'PK2, 'Key> * 'Result2) seq>) -> 
            (IDataReader -> IResultReader<(Rows.KeySpecifier<'Key, 'FK1> * 'Result1) seq>) -> 
            IDataReader -> 
            IResultReader<(Rows.KeySpecifier<'Key, 'FK1> * 'Result1) seq> = 
        let merge = this.GenerateMerge(target)
        this.Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2>(fun (r1: 'Result1, r2s: 'Result2 seq) -> merge.Invoke(r1, r2s |> Seq.toArray))

    member this.Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2 when 'Key: comparison>(target: Expr<'Result2 seq>): 
            (IDataReader -> IResultReader<(Rows.KeySpecifier<'PK2, 'Key> * 'Result2) seq>) -> 
            (IDataReader -> IResultReader<(Rows.KeySpecifier<'Key, 'FK1> * 'Result1) seq>) -> 
            IDataReader -> 
            IResultReader<(Rows.KeySpecifier<'Key, 'FK1> * 'Result1) seq> = 
        let merge = this.GenerateMerge(target)
        this.Join<'Key, 'FK1, 'PK2, 'Result1, 'Result2>(fun (r1: 'Result1, r2s: 'Result2 seq) -> merge.Invoke(r1, r2s))
