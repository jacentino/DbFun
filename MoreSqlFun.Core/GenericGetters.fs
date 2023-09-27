namespace MoreSqlFun.Core.Builders

open System
open System.Reflection
open System.Linq.Expressions
open Microsoft.FSharp.Reflection
open MoreSqlFun.Core

module GenericGetters = 

    type IGetter<'DbObject, 'Result> = 
        abstract member IsNull: 'DbObject -> bool
        abstract member Get: 'DbObject -> 'Result
        abstract member Create: 'DbObject -> unit

    type IGetterProvider<'Prototype, 'DbObject> = 
        abstract member Getter: Type * string * 'Prototype -> obj
        abstract member Getter: string * 'Prototype -> IGetter<'DbObject, 'Result>

    type IBuilder<'Prototype, 'DbObject> = 
        abstract member CanBuild: Type -> bool
        abstract member Build: IGetterProvider<'Prototype, 'DbObject> * string -> 'Prototype -> IGetter<'DbObject, 'Result>


    type UnitBuilder<'Prototype, 'DbObject>() =

        interface IBuilder<'Prototype, 'DbObject> with

            member __.CanBuild (resType: Type) = resType = typeof<unit>

            member __.Build<'Result> (_: IGetterProvider<'Prototype, 'DbObject>, _: string) (_: 'Prototype): IGetter<'DbObject, 'Result> = 
                { new IGetter<'DbObject, 'Result> with
                    member __.Get (_: 'DbObject) = 
                        Unchecked.defaultof<'Result>
                    member __.IsNull(_: 'DbObject) = 
                        true
                    member __.Create(_: 'DbObject) = 
                        ()
                }


    type SequenceBuilder<'Prototype, 'DbObject>() =

        interface IBuilder<'Prototype, 'DbObject> with

            member __.CanBuild (resType: Type) = Types.isCollectionType resType 

            member __.Build<'Result> (_: IGetterProvider<'Prototype, 'DbObject>, _: string) (_: 'Prototype): IGetter<'DbObject, 'Result> = 
                let newCall = 
                    if typeof<'Result>.IsAbstract then 
                        Expression.New(typedefof<list<_>>.MakeGenericType(typeof<'Result>.GetGenericArguments().[0])) :> Expression
                    elif typeof<'Result>.IsArray then
                        Expression.NewArrayInit(typeof<'Result>.GetElementType()) :> Expression
                    else
                        Expression.New(typeof<'Result>) :> Expression
                let constructor = Expression.Lambda<Func<'Result>>(newCall).Compile()
                { new IGetter<'DbObject, 'Result> with
                    member __.Get (_: 'DbObject) = 
                        constructor.Invoke()
                    member __.IsNull(_: 'DbObject) = 
                        true
                    member __.Create(_: 'DbObject) = 
                        ()
                }


    type OptionBuilder<'Prototype, 'DbObject>() = 

        member __.CreateGetter(provider: IGetterProvider<'Prototype, 'DbObject>, name: string, prototype: 'Prototype): IGetter<'DbObject, 'Underlying option> = 
            let underlying = provider.Getter<'Underlying>(name, prototype)
            { new IGetter<'DbObject, 'Underlying option> with
                member __.IsNull(record: 'DbObject): bool = underlying.IsNull(record)
                member __.Get(record: 'DbObject): 'Underlying option = 
                    if underlying.IsNull(record) then 
                        None
                    else
                        Some (underlying.Get(record))
                member __.Create(record: 'DbObject) = 
                    underlying.Create(record)
            }

        interface IBuilder<'Prototype, 'DbObject> with

            member __.CanBuild(resType: Type): bool = Types.isOptionType resType
                    
            member this.Build(provider: IGetterProvider<'Prototype, 'DbObject>, name: string) (prototype: 'Prototype): IGetter<'DbObject, 'Result> = 
                let method = this.GetType().GetMethod("CreateGetter")
                let gmethod = method.MakeGenericMethod(typeof<'Result>.GetGenericArguments().[0])
                let getter = gmethod.Invoke(this, [| provider; name; prototype |]) 
                getter :?> IGetter<'DbObject, 'Result>


    type Converter<'Prototype, 'DbObject, 'Source, 'Target>(convert: 'Source -> 'Target) =

        interface IBuilder<'Prototype, 'DbObject> with

            member __.CanBuild (resType: Type) = typeof<'Target>.IsAssignableFrom resType

            member __.Build<'Result> (provider: IGetterProvider<'Prototype, 'DbObject>, name: string) (prototype: 'Prototype): IGetter<'DbObject, 'Result> = 
                let getter = provider.Getter<'Source>(name, prototype) 
                let convert' = box convert :?> 'Source -> 'Result
                { new IGetter<'DbObject, 'Result> with
                    member __.Get (record: 'DbObject) = 
                        convert'(getter.Get(record))
                    member __.IsNull(record: 'DbObject) = 
                        getter.IsNull(record)
                    member __.Create(record: 'DbObject) = 
                        getter.Create(record)
                }

                
    type EnumConverter<'Prototype, 'DbObject, 'Underlying>() = 

        interface IBuilder<'Prototype, 'DbObject> with

            member __.CanBuild(resType: Type): bool = resType.IsEnum && resType.GetEnumUnderlyingType() = typeof<'Underlying>

            member __.Build<'Result> (provider: IGetterProvider<'Prototype, 'DbObject>, name: string) (prototype: 'Prototype): IGetter<'DbObject, 'Result> = 
                let getter = provider.Getter<'Underlying>(name, prototype)   
                let underlyingParam = Expression.Parameter(typeof<'Underlying>)
                let convert = Expression.Lambda<Func<'Underlying, 'Result>>(Expression.Convert(underlyingParam, typeof<'Result>), underlyingParam).Compile()                    
                { new IGetter<'DbObject, 'Result> with
                    member __.Get (record: 'DbObject) = 
                        convert.Invoke(getter.Get(record))
                    member __.IsNull(record: 'DbObject) = 
                        getter.IsNull(record)
                    member __.Create(record: 'DbObject) = 
                        getter.Create(record)
                }


    type AttrEnumConverter<'Prototype, 'DbObject>() = 

        interface IBuilder<'Prototype, 'DbObject> with

            member __.CanBuild(resType: Type): bool = 
                resType.IsEnum 
                    && 
                resType.GetFields() 
                |> Seq.filter (fun f -> f.IsStatic) 
                |> Seq.forall (fun f -> not (Seq.isEmpty (f.GetCustomAttributes<Models.EnumValueAttribute>())))

            member __.Build<'Result> (provider: IGetterProvider<'Prototype, 'DbObject>, name: string) (prototype: 'Prototype): IGetter<'DbObject, 'Result> = 
                let getter = provider.Getter<string>(name, prototype)   
                let underlyingValues = 
                    [ for f in typeof<'Result>.GetFields() do
                        if f.IsStatic then
                            f.GetCustomAttribute<Models.EnumValueAttribute>().Value, f.GetValue(null) :?> 'Result
                    ] 
                let convert (x: string): 'Result = underlyingValues |> List.find (fst >> (=) x) |> snd
                { new IGetter<'DbObject, 'Result> with
                    member __.Get (record: 'DbObject) = 
                        convert(getter.Get(record))
                    member __.IsNull(record: 'DbObject) = 
                        getter.IsNull(record)
                    member __.Create(record: 'DbObject) = 
                        getter.Create(record)
                }


    module FieldListBuilder = 
            
        let private callGetValue(getter: Expression, record: Expression) = 
            Expression.Call(getter, getter.Type.GetInterface("IGetter`2").GetMethod("Get"), record) :> Expression

        let private callIsNull(getter: Expression, record: Expression) = 
            Expression.Call(getter, getter.Type.GetInterface("IGetter`2").GetMethod("IsNull"), record) :> Expression


        let private callCreate(getter: Expression, record: Expression) = 
            Expression.Call(getter, getter.Type.GetInterface("IGetter`2").GetMethod("Create"), record) :> Expression

        let private multiAnd (operands: Expression seq) = 
            Seq.fold (fun expr op -> Expression.And(expr, op) :> Expression) (operands |> Seq.head) (operands |> Seq.skip 1)

        let build(provider: IGetterProvider<'Prototype, 'DbObject>, fields: (Type * string) seq, constructor: Expression seq -> Expression, prototype: 'Prototype): IGetter<'DbObject, 'Result> = 
                    
                let recordParam = Expression.Parameter(typeof<'DbObject>)

                let gettersAndCheckers = 
                    [ for fieldType, name in fields do
                        let getter = Expression.Constant(provider.Getter(fieldType, name, prototype))
                        callGetValue(getter, recordParam), callIsNull(getter, recordParam), callCreate(getter, recordParam)
                    ]

                let getters = gettersAndCheckers |> Seq.map (fun (g, _, _) -> g) |> constructor
                let getFunction = Expression.Lambda<Func<'DbObject, 'Result>>(getters, recordParam).Compile()

                let nullCheckers = gettersAndCheckers |> Seq.map (fun (_, n, _) -> n) |> multiAnd
                let isNullFunction = Expression.Lambda<Func<'DbObject, bool>>(nullCheckers, recordParam).Compile()

                let creators = gettersAndCheckers |> Seq.map (fun (_, _, c) -> c) |> Expression.Block
                let createFunction = Expression.Lambda<Action<'DbObject>>(creators , recordParam).Compile()

                { new IGetter<'DbObject, 'Result> with
                    member __.Get (record: 'DbObject) = 
                        getFunction.Invoke(record)
                    member __.IsNull(record: 'DbObject) = 
                        isNullFunction.Invoke(record)
                    member __.Create(record: 'DbObject) = 
                        createFunction.Invoke(record)
                }


    type RecordBuilder<'Prototype, 'DbObject>() = 
            
        let newRecord (recordType: Type) (fieldTypes: Type array) (elements: Expression seq) = 
            let construct = recordType.GetConstructor(fieldTypes)
            Expression.New(construct, elements) :> Expression

        interface IBuilder<'Prototype, 'DbObject> with

            member __.CanBuild(resType: Type): bool = FSharpType.IsRecord(resType)

            member __.Build<'Result> (provider: IGetterProvider<'Prototype, 'DbObject>, _) (prototype: 'Prototype): IGetter<'DbObject, 'Result> = 
                let fields = FSharpType.GetRecordFields typeof<'Result> |> Array.map (fun f -> f.PropertyType, f.Name)
                FieldListBuilder.build(provider, fields, newRecord typeof<'Result> (fields |> Array.map fst), prototype)
                        

    type TupleBuilder<'Prototype, 'DbObject>() = 

        let newTuple (elementTypes: Type array) (elements: Expression seq) : Expression = 
            let creator = typeof<Tuple>.GetMethods() |> Array.find (fun m -> m.Name = "Create" && m.GetGenericArguments().Length = Array.length elementTypes)
            Expression.Call(creator.MakeGenericMethod(elementTypes), elements) :> Expression

        interface IBuilder<'Prototype, 'DbObject> with

            member __.CanBuild(resType: Type): bool = FSharpType.IsTuple(resType)

            member __.Build<'Result> (provider: IGetterProvider<'Prototype, 'DbObject>, name: string) (prototype: 'Prototype): IGetter<'DbObject, 'Result> =                
                let fields = typeof<'Result>.GetProperties() |> Array.mapi (fun i f -> f.PropertyType, sprintf "%s%d" name (i + 1))
                FieldListBuilder.build(provider, fields, newTuple (fields |> Array.map fst), prototype)


    type GenericGetterBuilder<'Prototype, 'DbObject>(builders: IBuilder<'Prototype, 'DbObject> seq) =

        let builders = 
            [
                yield! builders
                UnitBuilder<'Prototype, 'DbObject>()
                SequenceBuilder<'Prototype, 'DbObject>()
                RecordBuilder<'Prototype, 'DbObject>()
                TupleBuilder<'Prototype, 'DbObject>()
                OptionBuilder<'Prototype, 'DbObject>()
                Converter<'Prototype, 'DbObject, DateTime, DateOnly>(fun (dateTime: DateTime) -> DateOnly.FromDateTime(dateTime))
                Converter<'Prototype, 'DbObject, TimeSpan, TimeOnly>(fun (timeSpan: TimeSpan) -> TimeOnly.FromTimeSpan(timeSpan))
                AttrEnumConverter<'Prototype, 'DbObject>()
                EnumConverter<'Prototype, 'DbObject, char>()
                EnumConverter<'Prototype, 'DbObject, int>()
            ]

        member this.Unit (prototype: 'Prototype): IGetter<'DbObject, unit> = 
            this.CreateGetter<unit>("", prototype)

        member this.Simple<'Result>(name: string) (prototype: 'Prototype): IGetter<'DbObject, 'Result> = 
            this.CreateGetter<'Result>(name, prototype)

        member this.Optional<'Result>(name: string):  'Prototype -> IGetter<'DbObject, 'Result option> =            
            fun (prototype: 'Prototype) -> this.CreateGetter<'Result option>(name, prototype)

        member __.Optional<'Result>(underlying: 'Prototype -> IGetter<'DbObject, 'Result>): 'Prototype -> IGetter<'DbObject, 'Result option> = 
            fun (prototype: 'Prototype) ->           
                let getter = underlying(prototype)
                { new IGetter<'DbObject, 'Result option> with
                    member __.IsNull(record: 'DbObject) = 
                        getter.IsNull(record)
                    member __.Get(record: 'DbObject) = 
                        if getter.IsNull(record) then 
                            None
                        else
                            Some (getter.Get(record))
                    member __.Create(record: 'DbObject): unit = 
                        getter.Create(record)
                }


        member this.Tuple<'Result1, 'Result2>(name1: string, name2: string): 'Prototype -> IGetter<'DbObject, 'Result1 * 'Result2> =
            fun (prototype: 'Prototype) ->
                let getter1 = this.CreateGetter<'Result1>(name1, prototype)
                let getter2 = this.CreateGetter<'Result2>(name2, prototype)
                { new IGetter<'DbObject, 'Result1 * 'Result2> with
                    member __.IsNull(record: 'DbObject): bool = 
                        getter1.IsNull(record) && getter2.IsNull(record)
                    member __.Get(record: 'DbObject): 'Result1 * 'Result2 = 
                        getter1.Get(record), getter2.Get(record)
                    member __.Create(record: 'DbObject): unit = 
                        getter1.Create(record)
                        getter2.Create(record)
                }

        member __.Tuple<'Result1, 'Result2>(provider1: 'Prototype -> IGetter<'DbObject, 'Result1>, provider2: 'Prototype -> IGetter<'DbObject, 'Result2>): 'Prototype -> IGetter<'DbObject, 'Result1 * 'Result2> = 
            fun (prototype: 'Prototype) ->
                let getter1 = provider1(prototype)
                let getter2 = provider2(prototype)
                { new IGetter<'DbObject, 'Result1 * 'Result2> with
                    member __.IsNull(record: 'DbObject): bool = 
                        getter1.IsNull(record) && getter2.IsNull(record)
                    member __.Get(record: 'DbObject): 'Result1 * 'Result2 = 
                        getter1.Get(record), getter2.Get(record)
                    member __.Create(record: 'DbObject): unit = 
                        getter1.Create(record)
                        getter2.Create(record)
                }

        member this.Record<'Result>(name: string) (prototype: 'Prototype): IGetter<'DbObject, 'Result> = 
            this.CreateGetter<'Result>(name, prototype)


        member this.CreateGetter<'Result>(name: string, record: 'Prototype): IGetter<'DbObject, 'Result> = 
            match builders |> Seq.tryFind (fun b -> b.CanBuild typeof<'Result>) with
            | Some builder -> builder.Build<'Result>(this, name) record
            | None -> failwithf "Could not found row/column getter for type: %A" typeof<'Result>

        member this.CreateGetter(argType: Type, name: string, record: 'Prototype): obj = 
            let method = this.GetType().GetMethods() |> Seq.find (fun m -> m.Name = "CreateGetter" && m.IsGenericMethod && m.GetGenericArguments().Length = 1)
            let gmethod = method.MakeGenericMethod(argType)
            gmethod.Invoke(this, [| name; record |])

        interface IGetterProvider<'Prototype, 'DbObject> with
            member this.Getter(resType: Type, name: string, record: 'Prototype): obj = 
                this.CreateGetter(resType, name, record)
            member this.Getter<'Result>(name: string, record: 'Prototype): IGetter<'DbObject, 'Result> =                
                this.CreateGetter<'Result>(name, record)
