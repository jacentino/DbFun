namespace DbFun.Core.Builders

open System
open System.Reflection
open System.Linq.Expressions
open Microsoft.FSharp.Reflection
open DbFun.Core
open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Quotations.Patterns

module GenericGetters = 

    type IGetter<'DbObject, 'Result> = 
        abstract member IsNull: 'DbObject -> bool
        abstract member Get: 'DbObject -> 'Result
        abstract member Create: 'DbObject -> unit

    type IGetterProvider<'Prototype, 'DbObject> = 
        abstract member Getter: Type * string * 'Prototype -> obj
        abstract member Getter: string * 'Prototype -> IGetter<'DbObject, 'Result>
        abstract member Builder: Type -> IBuilder<'Prototype, 'DbObject> option
    and
        IBuilder<'Prototype, 'DbObject> = 
        abstract member CanBuild: Type -> bool
        abstract member Build: string * IGetterProvider<'Prototype, 'DbObject> * 'Prototype -> IGetter<'DbObject, 'Result>

    type IBuilderEx<'Prototype, 'DbObject> = 
        abstract member Build: string * IGetterProvider<'Prototype, 'DbObject> * 'Prototype -> IGetter<'DbObject, 'Result>

    type BuildGetter<'Prototype, 'DbObject, 'Result> = IGetterProvider<'Prototype, 'DbObject> * 'Prototype -> IGetter<'DbObject, 'Result>

    type IOverride<'Prototype, 'DbObject> = 
        abstract member IsRelevant: string -> bool
        abstract member IsFinal: bool
        abstract member Shift: unit -> IOverride<'Prototype, 'DbObject>
        abstract member Build: IGetterProvider<'Prototype, 'DbObject> * 'Prototype -> IGetter<'DbObject, 'Result>

    /// <summary>
    /// The specification of field mapping override.
    /// </summary>
    type Override<'Prototype, 'DbObject, 'Result>(propNames: string list, builder: IGetterProvider<'Prototype, 'DbObject> * 'Prototype -> IGetter<'DbObject, 'Result>) =         

        static let rec getPropChain(expr: Expr) = 
            match expr with
            | PropertyGet (Some inner, property, _) -> getPropChain(inner) @ [ property.Name ]
            | _ -> []

        /// <summary>
        /// Creates an override for a field specified by a given property path by replacing it's default builder.
        /// </summary>
        /// <param name="path">
        /// The property path of the overriden field.
        /// </param>
        /// <param name="setter"></param>
        new ([<ReflectedDefinition>] path: Expr<'Result>, builder: IGetterProvider<'Prototype, 'DbObject> * 'Prototype -> IGetter<'DbObject, 'Result>) = 
            Override(getPropChain(path), builder)

        interface IOverride<'Prototype, 'DbObject> with
            member __.IsRelevant (propertyName: string) = propNames |> List.tryHead |> Option.map ((=) propertyName) |> Option.defaultValue false
            member __.IsFinal = propNames |> List.isEmpty
            member __.Shift() = Override(propNames |> List.tail, builder)
            member __.Build<'Result2>(provider: IGetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype) = 
                builder(provider, prototype) :?> IGetter<'DbObject, 'Result2>

    type BaseGetterProvider<'Prototype, 'DbObject>(builders: IBuilder<'Prototype, 'DbObject> seq) = 

        member this.GetGetter(argType: Type, name: string, prototype: 'Prototype): obj = 
            let method = this.GetType().GetMethods() |> Seq.find (fun m -> m.Name = "GetGetter" && m.IsGenericMethod && m.GetGenericArguments().Length = 1)
            let gmethod = method.MakeGenericMethod(argType)
            gmethod.Invoke(this, [| name; prototype |])

        member this.GetGetter<'Result>(name: string, prototype: 'Prototype): IGetter<'DbObject, 'Result> = 
            match builders |> Seq.tryFind (fun b -> b.CanBuild typeof<'Result>) with
            | Some builder -> builder.Build(name, this, prototype)
            | None -> failwithf "Could not find a getter builder for type: %A" typeof<'Result>

        interface IGetterProvider<'Prototype, 'DbObject> with
            member this.Getter<'Result>(name: string, prototype: 'Prototype): IGetter<'DbObject, 'Result> = 
                this.GetGetter<'Result>(name, prototype)
            member this.Getter(argType: Type, name: string, prototype: 'Prototype): obj = 
                this.GetGetter(argType, name, prototype)
            member __.Builder(argType: Type) = 
                builders |> Seq.tryFind (fun b -> b.CanBuild argType)


    type DerivedGetterProvider<'Prototype, 'DbObject>(baseProvider: IGetterProvider<'Prototype, 'DbObject>, overrides: IOverride<'Prototype, 'DbObject> seq) =

        member this.GetGetter(argType: Type, name: string, prototype: 'Prototype): obj = 
            let method = this.GetType().GetMethods() |> Seq.find (fun m -> m.Name = "GetSetter" && m.IsGenericMethod && m.GetGenericArguments().Length = 1)
            let gmethod = method.MakeGenericMethod(argType)
            gmethod.Invoke(this, [| name; prototype |])

        member __.GetSetter<'Arg>(name: string, prototype: 'Prototype): IGetter<'DbObject, 'Arg> = 
            let relevant = overrides |> Seq.filter (fun x -> x.IsRelevant name) |> Seq.map (fun x -> x.Shift()) |> Seq.toList
            match relevant |> List.tryFind (fun x -> x.IsFinal) with
            | Some ov -> ov.Build(DerivedGetterProvider(baseProvider, relevant), prototype)
            | None -> baseProvider.Getter(name, prototype)

        member __.GetBuilder(argType: Type) = 
            baseProvider.Builder(argType)

        interface IGetterProvider<'Prototype, 'DbObject> with
            member this.Getter<'Arg>(name: string, prototype: 'Prototype): IGetter<'DbObject, 'Arg> = 
                this.GetSetter<'Arg>(name, prototype)
            member this.Getter(argType: Type, name: string, prototype: 'Prototype): obj = 
                this.GetGetter(argType, name, prototype)
            member this.Builder(argType: Type) = 
                this.GetBuilder(argType)


    type UnitBuilder<'Prototype, 'DbObject>() =

        interface IBuilder<'Prototype, 'DbObject> with

            member __.CanBuild (resType: Type) = resType = typeof<unit>

            member __.Build<'Result> (_: string, _: IGetterProvider<'Prototype, 'DbObject>, _: 'Prototype): IGetter<'DbObject, 'Result> = 
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

            member __.Build<'Result> (string, _: IGetterProvider<'Prototype, 'DbObject>, _: _: 'Prototype): IGetter<'DbObject, 'Result> = 
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
                    
            member this.Build(name: string, provider: IGetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype): IGetter<'DbObject, 'Result> = 
                let method = this.GetType().GetMethod("CreateGetter")
                let gmethod = method.MakeGenericMethod(typeof<'Result>.GetGenericArguments().[0])
                let getter = gmethod.Invoke(this, [| provider; name; prototype |]) 
                getter :?> IGetter<'DbObject, 'Result>


    type Converter<'Prototype, 'DbObject, 'Source, 'Target>(convert: 'Source -> 'Target) =

        interface IBuilder<'Prototype, 'DbObject> with

            member __.CanBuild (resType: Type) = typeof<'Target>.IsAssignableFrom resType

            member __.Build<'Result> (name: string, provider: IGetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype): IGetter<'DbObject, 'Result> = 
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

            member __.Build<'Result> (name: string, provider: IGetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype): IGetter<'DbObject, 'Result> = 
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
                FSharpType.IsUnion resType
                    && 
                resType
                |> FSharpType.GetUnionCases
                |> Seq.forall (fun uc -> not (Seq.isEmpty (uc.GetCustomAttributes(typeof<Models.EnumValueAttribute>))))

            member __.Build<'Result> (name: string, provider: IGetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype): IGetter<'DbObject, 'Result> = 
                let getter = provider.Getter<string>(name, prototype)   
                let properties = typeof<'Result>.GetProperties()
                let underlyingValues = 
                    [ for uc in FSharpType.GetUnionCases typeof<'Result> do
                        (uc.GetCustomAttributes(typeof<Models.EnumValueAttribute>)[0] :?> Models.EnumValueAttribute).Value,
                        (properties |> Array.find(fun p -> p.Name = uc.Name)).GetValue(null) :?> 'Result
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

            member __.Build<'Result> (_, provider: IGetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype): IGetter<'DbObject, 'Result> = 
                let fields = FSharpType.GetRecordFields typeof<'Result> |> Array.map (fun f -> f.PropertyType, f.Name)
                FieldListBuilder.build(provider, fields, newRecord typeof<'Result> (fields |> Array.map fst), prototype)
                        
        interface IBuilderEx<'Prototype, 'DbObject> with

            member __.Build<'Result> (name: string, provider: IGetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype): IGetter<'DbObject, 'Result> = 
                let fields = FSharpType.GetRecordFields typeof<'Result> |> Array.map (fun f -> f.PropertyType, sprintf "%s%s" name f.Name)
                FieldListBuilder.build(provider, fields, newRecord typeof<'Result> (fields |> Array.map fst), prototype)
                        

    type TupleBuilder<'Prototype, 'DbObject>() = 

        let newTuple (elementTypes: Type array) (elements: Expression seq) : Expression = 
            let creator = typeof<Tuple>.GetMethods() |> Array.find (fun m -> m.Name = "Create" && m.GetGenericArguments().Length = Array.length elementTypes)
            Expression.Call(creator.MakeGenericMethod(elementTypes), elements) :> Expression

        interface IBuilder<'Prototype, 'DbObject> with

            member __.CanBuild(resType: Type): bool = FSharpType.IsTuple(resType)

            member __.Build<'Result> (name: string, provider: IGetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype): IGetter<'DbObject, 'Result> =                
                let fields = typeof<'Result>.GetProperties() |> Array.mapi (fun i f -> f.PropertyType, sprintf "%s%d" name (i + 1))
                FieldListBuilder.build(provider, fields, newTuple (fields |> Array.map fst), prototype)

    let getDefaultBuilders(): IBuilder<'Prototype, 'DbObject> list = 
        [
            UnitBuilder<'Prototype, 'DbObject>()
            Converter<'Prototype, 'DbObject, DateTime, DateOnly>(fun (dateTime: DateTime) -> DateOnly.FromDateTime(dateTime))
            Converter<'Prototype, 'DbObject, TimeSpan, TimeOnly>(fun (timeSpan: TimeSpan) -> TimeOnly.FromTimeSpan(timeSpan))
            AttrEnumConverter<'Prototype, 'DbObject>()
            EnumConverter<'Prototype, 'DbObject, char>()
            EnumConverter<'Prototype, 'DbObject, int>()
            RecordBuilder<'Prototype, 'DbObject>()
            TupleBuilder<'Prototype, 'DbObject>()
            OptionBuilder<'Prototype, 'DbObject>()
            SequenceBuilder<'Prototype, 'DbObject>()
        ]


    type GenericGetterBuilder<'Prototype, 'DbObject>() =

        /// <summary>
        /// Creates a builder handling result without any value.
        /// </summary>
        static member Unit: BuildGetter<'Prototype, 'DbObject, Unit> = 
            fun (provider, prototype) -> provider.Getter<unit>("", prototype)

        /// <summary>
        /// Creates a builder handling simple values.
        /// </summary>
        /// <param name="name">
        /// The column name or prefix (for indirect results).
        /// </param>
        static member Simple<'Result>(name: string): BuildGetter<'Prototype, 'DbObject, 'Result> = 
            fun (provider, prototype) -> provider.Getter<'Result>(name, prototype)

        /// <summary>
        /// Creates a builder handling integer values.
        /// </summary>
        /// <param name="name">
        /// The column name.
        /// </param>
        static member Int(name: string): BuildGetter<'Prototype, 'DbObject, int> = 
            fun (provider, prototype) -> provider.Getter<int>(name, prototype)

        /// <summary>
        /// Creates a builder handling 64-bit integer values.
        /// </summary>
        /// <param name="name">
        /// The column name.
        /// </param>
        static member Int64(name: string): BuildGetter<'Prototype, 'DbObject, int64> = 
            fun (provider, prototype) -> provider.Getter<int64>(name, prototype)

        /// <summary>
        /// Creates a builder handling byte values.
        /// </summary>
        /// <param name="name">
        /// The column name.
        /// </param>
        static member Byte(name: string): BuildGetter<'Prototype, 'DbObject, byte> = 
            fun (provider, prototype) -> provider.Getter<byte>(name, prototype)

        /// <summary>
        /// Creates a builder handling char values.
        /// </summary>
        /// <param name="name">
        /// The column name.
        /// </param>
        static member Char(name: string): BuildGetter<'Prototype, 'DbObject, char> = 
            fun (provider, prototype) -> provider.Getter<char>(name, prototype)

        /// <summary>
        /// Creates a builder handling string values.
        /// </summary>
        /// <param name="name">
        /// The column name.
        /// </param>
        static member String(name: string): BuildGetter<'Prototype, 'DbObject, string> = 
            fun (provider, prototype) -> provider.Getter<string>(name, prototype)

        /// <summary>
        /// Creates a builder handling DateTime values.
        /// </summary>
        /// <param name="name">
        /// The column name.
        /// </param>
        static member DateTime(name: string): BuildGetter<'Prototype, 'DbObject, DateTime> = 
            fun (provider, prototype) -> provider.Getter<DateTime>(name, prototype)

        /// <summary>
        /// Creates a builder handling DateOnly values.
        /// </summary>
        /// <param name="name">
        /// The column name.
        /// </param>
        static member DateOnly(name: string): BuildGetter<'Prototype, 'DbObject, DateOnly> = 
            fun (provider, prototype) -> provider.Getter<DateOnly>(name, prototype)

        /// <summary>
        /// Creates a builder handling TimeOnly values.
        /// </summary>
        /// <param name="name">
        /// The column name.
        /// </param>
        static member TimeOnly(name: string): BuildGetter<'Prototype, 'DbObject, TimeOnly> = 
            fun (provider, prototype) -> provider.Getter<TimeOnly>(name, prototype)

        /// <summary>
        /// Creates a builder handling Decimal values.
        /// </summary>
        /// <param name="name">
        /// The column name.
        /// </param>
        static member Decimal(name: string): BuildGetter<'Prototype, 'DbObject, decimal> = 
            fun (provider, prototype) -> provider.Getter<decimal>(name, prototype)

        /// <summary>
        /// Creates a builder handling float values.
        /// </summary>
        /// <param name="name">
        /// The column name.
        /// </param>
        static member Float(name: string): BuildGetter<'Prototype, 'DbObject, float> = 
            fun (provider, prototype) -> provider.Getter<float>(name, prototype)

        /// <summary>
        /// Creates a builder handling double values.
        /// </summary>
        /// <param name="name">
        /// The column name.
        /// </param>
        static member Double(name: string): BuildGetter<'Prototype, 'DbObject, double> = 
            fun (provider, prototype) -> provider.Getter<double>(name, prototype)

        /// <summary>
        /// Creates a builder handling bool values.
        /// </summary>
        /// <param name="name">
        /// The column name.
        /// </param>
        static member Bool(name: string): BuildGetter<'Prototype, 'DbObject, bool> = 
            fun (provider, prototype) -> provider.Getter<bool>(name, prototype)

        /// <summary>
        /// Creates a builder handling Guid values.
        /// </summary>
        /// <param name="name">
        /// The column name.
        /// </param>
        static member Guid(name: string): BuildGetter<'Prototype, 'DbObject, Guid> = 
            fun (provider, prototype) -> provider.Getter<Guid>(name, prototype)

        /// <summary>
        /// Creates a builder handling byte array values.
        /// </summary>
        /// <param name="name">
        /// The column name.
        /// </param>
        static member ByteArray(name: string): BuildGetter<'Prototype, 'DbObject, byte array> = 
            fun (provider, prototype) -> provider.Getter<byte array>(name, prototype)

        /// <summary>
        /// Creates a builder handling optional types.
        /// </summary>
        /// <param name="name">
        /// The column or prefix (for indirect results) name.
        /// </param>
        static member Optional<'Result>(name: string):  BuildGetter<'Prototype, 'DbObject, 'Result option> =            
            fun (provider: IGetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype) -> 
                provider.Getter<'Result option>(name, prototype)

        /// <summary>
        /// Creates a builder handling optional types.
        /// </summary>
        /// <param name="underlying">
        /// The option underlying type builder.
        /// </param>
        static member Optional<'Result>(underlying: BuildGetter<'Prototype, 'DbObject, 'Result>): BuildGetter<'Prototype, 'DbObject, 'Result option> = 
            fun (provider: IGetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype) ->           
                let getter = underlying(provider, prototype)
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

            
        /// <summary>
        /// Creates a builder handling 2-element tuple types.
        /// </summary>
        /// <param name="name1">
        /// The column or prefix (for indirect results) name of the first tuple element.
        /// </param>
        /// <param name="name2">
        /// The column or prefix (for indirect results) name of the second tuple element.
        /// </param>
        static member Tuple<'Result1, 'Result2>(name1: string, name2: string): BuildGetter<'Prototype, 'DbObject, 'Result1 * 'Result2> =
            fun (provider: IGetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype) ->
                let getter1 = provider.Getter<'Result1>(name1, prototype)
                let getter2 = provider.Getter<'Result2>(name2, prototype)
                { new IGetter<'DbObject, 'Result1 * 'Result2> with
                    member __.IsNull(record: 'DbObject): bool = 
                        getter1.IsNull(record) && getter2.IsNull(record)
                    member __.Get(record: 'DbObject): 'Result1 * 'Result2 = 
                        getter1.Get(record), getter2.Get(record)
                    member __.Create(record: 'DbObject): unit = 
                        getter1.Create(record)
                        getter2.Create(record)
                }

        /// <summary>
        /// Creates a builder handling 2-element tuple types.
        /// </summary>
        /// <param name="createGetter1">
        /// The builder of the first tuple element.
        /// </param>
        /// <param name="createGetter2">
        /// The builder of the second tuple element.
        /// </param>
        static member Tuple<'Result1, 'Result2>(
                createGetter1: BuildGetter<'Prototype, 'DbObject, 'Result1>, 
                createGetter2: BuildGetter<'Prototype, 'DbObject, 'Result2>)
                : BuildGetter<'Prototype, 'DbObject, 'Result1 * 'Result2> = 
            fun (provider: IGetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype) ->
                let getter1 = createGetter1(provider, prototype)
                let getter2 = createGetter2(provider, prototype)
                { new IGetter<'DbObject, 'Result1 * 'Result2> with
                    member __.IsNull(record: 'DbObject): bool = 
                        getter1.IsNull(record) && getter2.IsNull(record)
                    member __.Get(record: 'DbObject): 'Result1 * 'Result2 = 
                        getter1.Get(record), getter2.Get(record)
                    member __.Create(record: 'DbObject): unit = 
                        getter1.Create(record)
                        getter2.Create(record)
                }

        /// <summary>
        /// Creates a builder handling 3-element tuple types.
        /// </summary>
        /// <param name="name1">
        /// The column or prefix (for indirect results) name of the first tuple element.
        /// </param>
        /// <param name="name2">
        /// The column or prefix (for indirect results) name of the second tuple element.
        /// </param>
        /// <param name="name3">
        /// The column or prefix (for indirect results) name of the third tuple element.
        /// </param>
        static member Tuple<'Result1, 'Result2, 'Result3>(name1: string, name2: string, name3: string): BuildGetter<'Prototype, 'DbObject, 'Result1 * 'Result2 * 'Result3> =
            fun (provider: IGetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype) ->
                let getter1 = provider.Getter<'Result1>(name1, prototype)
                let getter2 = provider.Getter<'Result2>(name2, prototype)
                let getter3 = provider.Getter<'Result3>(name3, prototype)
                { new IGetter<'DbObject, 'Result1 * 'Result2 * 'Result3> with
                    member __.IsNull(record: 'DbObject): bool = 
                        getter1.IsNull(record) && getter2.IsNull(record) && getter3.IsNull(record)
                    member __.Get(record: 'DbObject): 'Result1 * 'Result2 * 'Result3 = 
                        getter1.Get(record), getter2.Get(record), getter3.Get(record)
                    member __.Create(record: 'DbObject): unit = 
                        getter1.Create(record)
                        getter2.Create(record)
                        getter3.Create(record)
                }

        /// <summary>
        /// Creates a builder handling 3-element tuple types.
        /// </summary>
        /// <param name="createGetter1">
        /// The builder of the first tuple element.
        /// </param>
        /// <param name="createGetter2">
        /// The builder of the second tuple element.
        /// </param>
        /// <param name="createGetter3">
        /// The builder of the third tuple element.
        /// </param>
        static member Tuple<'Result1, 'Result2, 'Result3>(
                createGetter1: BuildGetter<'Prototype, 'DbObject, 'Result1>, 
                createGetter2: BuildGetter<'Prototype, 'DbObject, 'Result2>, 
                createGetter3: BuildGetter<'Prototype, 'DbObject, 'Result3>)
                : BuildGetter<'Prototype, 'DbObject, 'Result1 * 'Result2 * 'Result3> = 
            fun (provider: IGetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype) ->
                let getter1 = createGetter1(provider, prototype)
                let getter2 = createGetter2(provider, prototype)
                let getter3 = createGetter3(provider, prototype)
                { new IGetter<'DbObject, 'Result1 * 'Result2 * 'Result3> with
                    member __.IsNull(record: 'DbObject): bool = 
                        getter1.IsNull(record) && getter2.IsNull(record) && getter3.IsNull(record)
                    member __.Get(record: 'DbObject): 'Result1 * 'Result2 * 'Result3 = 
                        getter1.Get(record), getter2.Get(record), getter3.Get(record)
                    member __.Create(record: 'DbObject): unit = 
                        getter1.Create(record)
                        getter2.Create(record)
                        getter3.Create(record)
                }

        /// <summary>
        /// Creates a builder handling 4-element tuple types.
        /// </summary>
        /// <param name="name1">
        /// The column or prefix (for indirect results) name of the first tuple element.
        /// </param>
        /// <param name="name2">
        /// The column or prefix (for indirect results) name of the second tuple element.
        /// </param>
        /// <param name="name3">
        /// The column or prefix (for indirect results) name of the third tuple element.
        /// </param>
        /// <param name="name4">
        /// The column or prefix (for indirect results) name of the fourth tuple element.
        /// </param>
        static member Tuple<'Result1, 'Result2, 'Result3, 'Result4>(name1: string, name2: string, name3: string, name4: string): BuildGetter<'Prototype, 'DbObject, 'Result1 * 'Result2 * 'Result3 * 'Result4> =
            fun (provider: IGetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype) ->
                let getter1 = provider.Getter<'Result1>(name1, prototype)
                let getter2 = provider.Getter<'Result2>(name2, prototype)
                let getter3 = provider.Getter<'Result3>(name3, prototype)
                let getter4 = provider.Getter<'Result4>(name4, prototype)
                { new IGetter<'DbObject, 'Result1 * 'Result2 * 'Result3 * 'Result4> with
                    member __.IsNull(record: 'DbObject): bool = 
                        getter1.IsNull(record) && getter2.IsNull(record) && getter3.IsNull(record) && getter4.IsNull(record)
                    member __.Get(record: 'DbObject): 'Result1 * 'Result2 * 'Result3 * 'Result4 = 
                        getter1.Get(record), getter2.Get(record), getter3.Get(record), getter4.Get(record)
                    member __.Create(record: 'DbObject): unit = 
                        getter1.Create(record)
                        getter2.Create(record)
                        getter3.Create(record)
                        getter4.Create(record)
                }

        /// <summary>
        /// Creates a builder handling 4-element tuple types.
        /// </summary>
        /// <param name="createGetter1">
        /// The builder of the first tuple element.
        /// </param>
        /// <param name="createGetter2">
        /// The builder of the second tuple element.
        /// </param>
        /// <param name="createGetter3">
        /// The builder of the third tuple element.
        /// </param>
        /// <param name="createGetter4">
        /// The builder of the fourth tuple element.
        /// </param>
        static member Tuple<'Result1, 'Result2, 'Result3, 'Result4>(
                createGetter1: BuildGetter<'Prototype, 'DbObject, 'Result1>, 
                createGetter2: BuildGetter<'Prototype, 'DbObject, 'Result2>, 
                createGetter3: BuildGetter<'Prototype, 'DbObject, 'Result3>, 
                createGetter4: BuildGetter<'Prototype, 'DbObject, 'Result4>)
                : BuildGetter<'Prototype, 'DbObject, 'Result1 * 'Result2 * 'Result3 * 'Result4> = 
            fun (provider: IGetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype) ->
                let getter1 = createGetter1(provider, prototype)
                let getter2 = createGetter2(provider, prototype)
                let getter3 = createGetter3(provider, prototype)
                let getter4 = createGetter4(provider, prototype)
                { new IGetter<'DbObject, 'Result1 * 'Result2 * 'Result3 * 'Result4> with
                    member __.IsNull(record: 'DbObject): bool = 
                        getter1.IsNull(record) && getter2.IsNull(record) && getter3.IsNull(record) && getter4.IsNull(record)
                    member __.Get(record: 'DbObject): 'Result1 * 'Result2 * 'Result3 * 'Result4 = 
                        getter1.Get(record), getter2.Get(record), getter3.Get(record), getter4.Get(record)
                    member __.Create(record: 'DbObject): unit = 
                        getter1.Create(record)
                        getter2.Create(record)
                        getter3.Create(record)
                        getter4.Create(record)
                }

        /// <summary>
        /// Creates a builder handling 5-element tuple types.
        /// </summary>
        /// <param name="name1">
        /// The column or prefix (for indirect results) name of the first tuple element.
        /// </param>
        /// <param name="name2">
        /// The column or prefix (for indirect results) name of the second tuple element.
        /// </param>
        /// <param name="name3">
        /// The column or prefix (for indirect results) name of the third tuple element.
        /// </param>
        /// <param name="name4">
        /// The column or prefix (for indirect results) name of the fourth tuple element.
        /// </param>
        /// <param name="name5">
        /// The column or prefix (for indirect results) name of the fifth tuple element.
        /// </param>
        static member Tuple<'Result1, 'Result2, 'Result3, 'Result4, 'Result5>(name1: string, name2: string, name3: string, name4: string, name5: string): BuildGetter<'Prototype, 'DbObject, 'Result1 * 'Result2 * 'Result3 * 'Result4 * 'Result5> =
            fun (provider: IGetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype) ->
                let getter1 = provider.Getter<'Result1>(name1, prototype)
                let getter2 = provider.Getter<'Result2>(name2, prototype)
                let getter3 = provider.Getter<'Result3>(name3, prototype)
                let getter4 = provider.Getter<'Result4>(name4, prototype)
                let getter5 = provider.Getter<'Result5>(name5, prototype)
                { new IGetter<'DbObject, 'Result1 * 'Result2 * 'Result3 * 'Result4 * 'Result5> with
                    member __.IsNull(record: 'DbObject): bool = 
                        getter1.IsNull(record) && getter2.IsNull(record) && getter3.IsNull(record) && getter4.IsNull(record) && getter5.IsNull(record)
                    member __.Get(record: 'DbObject): 'Result1 * 'Result2 * 'Result3 * 'Result4 * 'Result5 = 
                        getter1.Get(record), getter2.Get(record), getter3.Get(record), getter4.Get(record), getter5.Get(record)
                    member __.Create(record: 'DbObject): unit = 
                        getter1.Create(record)
                        getter2.Create(record)
                        getter3.Create(record)
                        getter4.Create(record)
                        getter5.Create(record)
                }

        /// <summary>
        /// Creates a builder handling 5-element tuple types.
        /// </summary>
        /// <param name="createGetter1">
        /// The builder of the first tuple element.
        /// </param>
        /// <param name="createGetter2">
        /// The builder of the second tuple element.
        /// </param>
        /// <param name="createGetter3">
        /// The builder of the third tuple element.
        /// </param>
        /// <param name="createGetter4">
        /// The builder of the fourth tuple element.
        /// </param>
        /// <param name="createGetter5">
        /// The builder of the fourth tuple element.
        /// </param>
        static member Tuple<'Result1, 'Result2, 'Result3, 'Result4, 'Result5>(
                createGetter1: BuildGetter<'Prototype, 'DbObject, 'Result1>, 
                createGetter2: BuildGetter<'Prototype, 'DbObject, 'Result2>, 
                createGetter3: BuildGetter<'Prototype, 'DbObject, 'Result3>, 
                createGetter4: BuildGetter<'Prototype, 'DbObject, 'Result4>, 
                createGetter5: BuildGetter<'Prototype, 'DbObject, 'Result5>)
                : BuildGetter<'Prototype, 'DbObject, 'Result1 * 'Result2 * 'Result3 * 'Result4 * 'Result5> = 
            fun (provider: IGetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype) ->
                let getter1 = createGetter1(provider, prototype)
                let getter2 = createGetter2(provider, prototype)
                let getter3 = createGetter3(provider, prototype)
                let getter4 = createGetter4(provider, prototype)
                let getter5 = createGetter5(provider, prototype)
                { new IGetter<'DbObject, 'Result1 * 'Result2 * 'Result3 * 'Result4 * 'Result5> with
                    member __.IsNull(record: 'DbObject): bool = 
                        getter1.IsNull(record) && getter2.IsNull(record) && getter3.IsNull(record) && getter4.IsNull(record) && getter5.IsNull(record)
                    member __.Get(record: 'DbObject): 'Result1 * 'Result2 * 'Result3 * 'Result4 * 'Result5 = 
                        getter1.Get(record), getter2.Get(record), getter3.Get(record), getter4.Get(record), getter5.Get(record) 
                    member __.Create(record: 'DbObject): unit = 
                        getter1.Create(record)
                        getter2.Create(record)
                        getter3.Create(record)
                        getter4.Create(record)
                        getter5.Create(record)
                }

        /// <summary>
        /// Creates a builder handling record types.
        /// </summary>
        /// <param name="name">
        /// The prefix of column names representing record fields in a dtatbase.
        /// </param>
        /// <param name="overrides">
        /// Objects allowing to override default mappings of particular fields.
        /// </param>
        static member Record<'Result>(prefix: string, [<ParamArray>] overrides: IOverride<'Prototype, 'DbObject> array): BuildGetter<'Prototype, 'DbObject, 'Result> = 
            fun (provider: IGetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype) ->
                let provider = DerivedGetterProvider<'Prototype, 'DbObject>(provider, overrides)
                match provider.GetBuilder(typeof<'Result>) with
                | Some builder ->
                    match builder with
                    | :? IBuilderEx<'Prototype, 'DbObject> as builderEx -> builderEx.Build<'Result>(prefix, provider, prototype)
                    | _ -> builder.Build<'Result>(prefix, provider, prototype)
                | None -> failwithf "Could not find row/column getter for type: %A" typeof<'Result>

        /// <summary>
        /// Creates a builder handling record types.
        /// </summary>
        /// <param name="name">
        /// The prefix of column names representing record fields in a dtatabase.
        /// </param>
        static member Record<'Result>(?prefix: string): IGetterProvider<'Prototype, 'DbObject> * 'Prototype -> IGetter<'DbObject, 'Result> = 
            GenericGetterBuilder<'Prototype, 'DbObject>.Record<'Result>(defaultArg prefix "", [||])

        /// <summary>
        /// Creates a builder handling record types.
        /// </summary>
        /// <param name="overrides">
        /// Objects allowing to override default mappings of particular fields.
        /// </param>
        static member Record<'Result>([<ParamArray>] overrides: IOverride<'Prototype, 'DbObject> array): BuildGetter<'Prototype, 'DbObject, 'Result> = 
            GenericGetterBuilder<'Prototype, 'DbObject>.Record<'Result>("", overrides)
