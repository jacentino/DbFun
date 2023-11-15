namespace DbFun.Core.Builders

open System
open System.Reflection
open System.Linq.Expressions
open Microsoft.FSharp.Reflection
open DbFun.Core
open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Quotations.Patterns
open System.Text.RegularExpressions

module GenericGetters = 

    type IGetter<'DbObject, 'Result> = 
        abstract member IsNull: 'DbObject -> bool
        abstract member Get: 'DbObject -> 'Result
        abstract member Create: 'DbObject -> unit

    type IGetterProvider<'Prototype, 'DbObject> = 
        abstract member Getter: Type * string * 'Prototype -> obj
        abstract member Getter: string * 'Prototype -> IGetter<'DbObject, 'Result>
        abstract member Builder: Type -> IBuilder<'Prototype, 'DbObject> option
        abstract member AllBuilders: Type -> IBuilder<'Prototype, 'DbObject> seq
    and
        IBuilder<'Prototype, 'DbObject> = 
            abstract member CanBuild: Type -> bool
            abstract member Build: string * IGetterProvider<'Prototype, 'DbObject> * 'Prototype -> IGetter<'DbObject, 'Result>
    
    
    type IConfigurableBuilder<'Prototype, 'DbObject, 'Config> = 
        abstract member Build: string * IGetterProvider<'Prototype, 'DbObject> * 'Config * 'Prototype -> IGetter<'DbObject, 'Result>

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
            member __.AllBuilders(argType: Type) = 
                builders |> Seq.filter (fun b -> b.CanBuild argType)

    type InitialDerivedGetterProvider<'Prototype, 'DbObject, 'Config>(baseProvider: IGetterProvider<'Prototype, 'DbObject>, config: 'Config, overrides: IOverride<'Prototype, 'DbObject> seq) =

        member this.GetGetter(argType: Type, name: string, prototype: 'Prototype): obj = 
            let method = this.GetType().GetMethods() |> Seq.find (fun m -> m.Name = "GetGetter" && m.IsGenericMethod && m.GetGenericArguments().Length = 1)
            let gmethod = method.MakeGenericMethod(argType)
            gmethod.Invoke(this, [| name; prototype |])

        member __.GetGetter<'Result>(name: string, prototype: 'Prototype): IGetter<'DbObject, 'Result> = 
            match baseProvider.Builder(typeof<'Result>) with
            | Some builder -> 
                match builder with 
                | :? IConfigurableBuilder<'Prototype, 'DbObject, 'Config> as cfgBuilder ->
                    let nextCfgDerived = DerivedGetterProvider(baseProvider, config, overrides)
                    cfgBuilder.Build(name, nextCfgDerived, config, prototype)
                | _ ->
                    let nextDerived = DerivedGetterProvider(baseProvider, (), overrides)
                    builder.Build(name, nextDerived, prototype)
            | None -> failwithf "Could not find a getter builder for type: %A" typeof<'Result>

        interface IGetterProvider<'Prototype, 'DbObject> with
            member this.Getter<'Result>(name: string, prototype: 'Prototype): IGetter<'DbObject, 'Result> = 
                this.GetGetter<'Result>(name, prototype)
            member this.Getter(argType: Type, name: string, prototype: 'Prototype): obj = 
                this.GetGetter(argType, name, prototype)
            member __.Builder(argType: Type) = 
                baseProvider.Builder(argType)
            member __.AllBuilders(argType: Type) = 
                baseProvider.AllBuilders(argType)

    and DerivedGetterProvider<'Prototype, 'DbObject, 'Config>(baseProvider: IGetterProvider<'Prototype, 'DbObject>, config: 'Config, overrides: IOverride<'Prototype, 'DbObject> seq) =

        member this.GetGetter(argType: Type, name: string, prototype: 'Prototype): obj = 
            let method = this.GetType().GetMethods() |> Seq.find (fun m -> m.Name = "GetGetter" && m.IsGenericMethod && m.GetGenericArguments().Length = 1)
            let gmethod = method.MakeGenericMethod(argType)
            gmethod.Invoke(this, [| name; prototype |])

        member __.GetGetter<'Result>(name: string, prototype: 'Prototype): IGetter<'DbObject, 'Result> = 
            let relevant = overrides |> Seq.filter (fun x -> x.IsRelevant name) |> Seq.map (fun x -> x.Shift()) |> Seq.toList
            let nextDerived = DerivedGetterProvider(baseProvider, (), relevant)
            match relevant |> List.tryFind (fun x -> x.IsFinal) with
            | Some ov -> ov.Build(nextDerived, prototype)
            | None -> 
                match baseProvider.Builder(typeof<'Result>) with
                | Some builder -> 
                    match builder with 
                    | :? IConfigurableBuilder<'Prototype, 'DbObject, 'Config> as cfgBuilder ->
                        let nextCfgDerived = DerivedGetterProvider(baseProvider, config, relevant)
                        cfgBuilder.Build(name, nextCfgDerived, config, prototype)
                    | _ ->
                        builder.Build(name, nextDerived, prototype)
                | None -> failwithf "Could not find a getter builder for type: %A" typeof<'Result>

        interface IGetterProvider<'Prototype, 'DbObject> with
            member this.Getter<'Result>(name: string, prototype: 'Prototype): IGetter<'DbObject, 'Result> = 
                this.GetGetter<'Result>(name, prototype)
            member this.Getter(argType: Type, name: string, prototype: 'Prototype): obj = 
                this.GetGetter(argType, name, prototype)
            member __.Builder(argType: Type) = 
                baseProvider.Builder(argType)
            member __.AllBuilders(argType: Type) = 
                baseProvider.AllBuilders(argType)


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
                        
        interface IConfigurableBuilder<'Prototype, 'DbObject, string * RecordNaming> with

            member __.Build<'Result> (name: string, provider: IGetterProvider<'Prototype, 'DbObject>, (prefix: string, naming: RecordNaming), prototype: 'Prototype): IGetter<'DbObject, 'Result> = 
                let fields = 
                    [| for f in FSharpType.GetRecordFields typeof<'Result> do
                        match naming with
                        | RecordNaming.Fields -> f.PropertyType, f.Name
                        | RecordNaming.Prefix -> f.PropertyType, sprintf "%s%s" prefix f.Name
                        | RecordNaming.Path   -> f.PropertyType, sprintf "%s%s" name f.Name
                    |]
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


    type UnionBuilder<'Prototype, 'DbObject>() = 

        let newUnionCase (unionType: Type, caseName: string) (elements: Expression seq) = 
            let construct = unionType.GetMethod("New" + caseName)
            Expression.Call(construct, elements) :> Expression

        member __.CreateUnionCaseGetter<'Union, 'UnionCase>(provider: IGetterProvider<'Prototype, 'DbObject>, name: string, prefix: string, caseName: string, naming: UnionNaming, fields: PropertyInfo array, prototype: 'Prototype): IGetter<'DbObject, 'Union> =             
            let fieldsHaveNames = fields |> Array.exists (fun f -> not (Regex.Match(f.Name, "Item[0-9]*").Success))
            let named = 
                [ for f in fields do
                    let fieldName1 = if fieldsHaveNames then f.Name else f.Name.Replace("Item", caseName)
                    let fieldName2 = if naming &&& UnionNaming.CaseNames <> UnionNaming.Fields && fieldsHaveNames then caseName + fieldName1 else fieldName1
                    let fieldName3 = if naming &&& UnionNaming.Prefix <> UnionNaming.Fields && naming &&& UnionNaming.Path = UnionNaming.Fields then prefix + fieldName2 else fieldName2
                    let fieldName4 = if naming &&& UnionNaming.Path <> UnionNaming.Fields then name + fieldName3 else fieldName3
                    f.PropertyType, fieldName4
                ]
            let unionCaseBuilder: IGetter<'DbObject, 'Union> = FieldListBuilder.build(provider, named, newUnionCase(typeof<'Union>, caseName), prototype)  
            unionCaseBuilder

        member this.Build<'Result> (name: string, prefix: string, naming: UnionNaming, provider: IGetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype): IGetter<'DbObject, 'Result> = 
            let tagGetter = provider.Getter<string>(name, prototype)   
            let createDirectValGetterMethod = this.GetType().GetMethod("CreateDirectValueGetter")
            let createUCGetterMethod = this.GetType().GetMethod("CreateUnionCaseGetter")
            let caseGetters = 
                [ for uc in FSharpType.GetUnionCases typeof<'Result> do                         
                    (uc.GetCustomAttributes(typeof<Models.UnionCaseTagAttribute>)[0] :?> Models.UnionCaseTagAttribute).Value, 
                    let fields = uc.GetFields()
                    if Array.isEmpty fields then
                        let gmethod = createDirectValGetterMethod.MakeGenericMethod(typeof<'Result>)
                        gmethod.Invoke(this, [| uc.Name |]) :?> IGetter<'DbObject, 'Result>
                    else
                        let gmethod = createUCGetterMethod.MakeGenericMethod(typeof<'Result>, typeof<'Result>.GetNestedType(uc.Name))
                        gmethod.Invoke(this, [| provider; name; prefix; uc.Name; naming; fields; prototype |]) :?> IGetter<'DbObject, 'Result>
                ] 
            let getCurrentUCGetter (record: 'DbObject): IGetter<'DbObject, 'Result> = caseGetters |> List.find (fst >> (=) (tagGetter.Get(record))) |> snd
            { new IGetter<'DbObject, 'Result> with
                member __.Get (record: 'DbObject) = 
                    getCurrentUCGetter(record).Get(record)
                member __.IsNull(record: 'DbObject) = 
                    getCurrentUCGetter(record).IsNull(record)
                member __.Create(record: 'DbObject) = 
                    tagGetter.Create(record)
                    for _, caseGetter in caseGetters do
                        caseGetter.Create(record)
            }


        member __.CreateDirectValueGetter<'Union>(name: string) = 
            let prop = typeof<'Union>.GetProperty(name)
            let value = prop.GetValue(null) :?> 'Union
            { new IGetter<'DbObject, 'Union> with
                member __.Get (record: 'DbObject) = value
                member __.IsNull(record: 'DbObject) = false
                member __.Create(record: 'DbObject) = ()
            }            

        interface IBuilder<'Prototype, 'DbObject> with

            member __.CanBuild(resType: Type): bool = 
                FSharpType.IsUnion resType
                    && 
                resType
                |> FSharpType.GetUnionCases
                |> Seq.forall (fun uc -> not (Seq.isEmpty (uc.GetCustomAttributes(typeof<Models.UnionCaseTagAttribute>))))

            member this.Build<'Result> (name: string, provider: IGetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype): IGetter<'DbObject, 'Result> = 
                this.Build(name, name, UnionNaming.Fields, provider, prototype)

        interface IConfigurableBuilder<'Prototype, 'DbObject, string * UnionNaming> with

            member this.Build<'Result> (name: string, provider: IGetterProvider<'Prototype, 'DbObject>, (prefix: string, naming: UnionNaming), prototype: 'Prototype): IGetter<'DbObject, 'Result> = 
                this.Build(name, prefix, naming, provider, prototype)


    type Configurator<'Prototype, 'DbObject, 'Config>(getConfig: string -> 'Config, canBuild: Type -> bool) = 

        member this.Build(name: string, provider: IGetterProvider<'Prototype,'DbObject>, config: 'Config, prototype: 'Prototype): IGetter<'DbObject, 'Result> = 
            provider.AllBuilders(typeof<'Result>) 
            |> Seq.tryPick (function | :? IConfigurableBuilder<'Prototype, 'DbObject, 'Config> as builder when builder <> this -> Some builder | _ -> None)   
            |> Option.map (fun builder -> builder.Build<'Result>(name, provider, config, prototype))
            |> Option.defaultWith(fun () ->  failwithf "Could not find a configurable getter builder for type: %A and config: %A" typeof<'Result> typeof<'Config>)

        interface IBuilder<'Prototype, 'DbObject> with

            member __.CanBuild(argType: Type) = canBuild(argType)
        
            member this.Build(name: string, provider: IGetterProvider<'Prototype,'DbObject>, prototype: 'Prototype): IGetter<'DbObject, 'Result> = 
                this.Build(name, provider, getConfig(name), prototype)

        interface IConfigurableBuilder<'Prototype, 'DbObject, 'Config> with

            member this.Build(name: string, provider: IGetterProvider<'Prototype,'DbObject>, config: 'Config, prototype: 'Prototype): IGetter<'DbObject, 'Result> = 
                this.Build(name, provider, config, prototype)


    let getDefaultBuilders(): IBuilder<'Prototype, 'DbObject> list = 
        [
            UnitBuilder<'Prototype, 'DbObject>()
            Converter<'Prototype, 'DbObject, DateTime, DateOnly>(fun (dateTime: DateTime) -> DateOnly.FromDateTime(dateTime))
            Converter<'Prototype, 'DbObject, TimeSpan, TimeOnly>(fun (timeSpan: TimeSpan) -> TimeOnly.FromTimeSpan(timeSpan))
            UnionBuilder<'Prototype, 'DbObject>()
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
        /// <param name="prefix">
        /// The prefix of column names representing record fields in a dtatbase.
        /// </param>
        /// <param name="overrides">
        /// Objects allowing to override default mappings of particular fields.
        /// </param>
        static member Record<'Result>(?prefix: string, ?naming: RecordNaming, ?overrides: IOverride<'Prototype, 'DbObject> seq): BuildGetter<'Prototype, 'DbObject, 'Result> = 
            fun (provider: IGetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype) ->
                let provider: IGetterProvider<'Prototype, 'DbObject> = 
                    match naming with
                    | Some naming ->
                        InitialDerivedGetterProvider<'Prototype, 'DbObject, string * RecordNaming>(provider, (defaultArg prefix "", naming), defaultArg overrides []) 
                    | None -> 
                        InitialDerivedGetterProvider<'Prototype, 'DbObject, unit>(provider, (), defaultArg overrides []) 
                provider.Getter<'Result>(defaultArg prefix "", prototype)

        /// <summary>
        /// Creates a builder handling discriminated union types.
        /// </summary>
        /// <param name="name">
        /// The union tag name column name.
        /// </param>
        /// <param name="overrides">
        /// Objects allowing to override default mappings of particular fields.
        /// </param>
        static member Union<'Result>(?name: string, ?naming: UnionNaming, ?overrides: IOverride<'Prototype, 'DbObject> seq): BuildGetter<'Prototype, 'DbObject, 'Result> = 
            fun (provider: IGetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype) ->
                let provider: IGetterProvider<'Prototype, 'DbObject> = 
                    match naming with
                    | Some naming ->
                        InitialDerivedGetterProvider<'Prototype, 'DbObject, string * UnionNaming>(provider, (defaultArg name "", naming), defaultArg overrides []) 
                    | None -> 
                        InitialDerivedGetterProvider<'Prototype, 'DbObject, unit>(provider, (), defaultArg overrides []) 
                provider.Getter<'Result>(defaultArg name "", prototype)
