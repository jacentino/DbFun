namespace DbFun.Core.Builders

open System
open System.Reflection
open FSharp.Reflection
open System.Linq.Expressions
open DbFun.Core
open Microsoft.FSharp.Quotations
open Microsoft.FSharp.Quotations.Patterns
open System.Text.RegularExpressions

module GenericSetters =

    type ISetter<'DbObject, 'Arg> = 
        abstract member SetValue: 'Arg * 'DbObject -> unit
        abstract member SetNull: 'DbObject -> unit
        abstract member SetArtificial: 'DbObject -> unit

    type ISetterProvider<'Prototype, 'DbObject> = 
        abstract member Setter: Type * string * 'Prototype -> obj
        abstract member Setter: string * 'Prototype -> ISetter<'DbObject, 'Arg>
        abstract member Builder: Type -> IBuilder<'Prototype, 'DbObject> option
        abstract member AllBuilders: Type -> IBuilder<'Prototype, 'DbObject> seq
    and
        IBuilder<'Prototype, 'DbObject> = 
        abstract member CanBuild: Type -> bool
        abstract member Build: string * ISetterProvider<'Prototype, 'DbObject> * 'Prototype -> ISetter<'DbObject, 'Arg>

    type IConfigurableBuilder<'Prototype, 'DbObject, 'Config> = 
        abstract member Build: string * ISetterProvider<'Prototype, 'DbObject> * 'Config * 'Prototype -> ISetter<'DbObject, 'Arg>

    type BuildSetter<'Prototype, 'DbObject, 'Arg> = ISetterProvider<'Prototype, 'DbObject> * 'Prototype -> ISetter<'DbObject, 'Arg>

    type IOverride<'Prototype, 'DbObject> = 
        abstract member IsRelevant: string -> bool
        abstract member IsFinal: bool
        abstract member Shift: unit -> IOverride<'Prototype, 'DbObject>
        abstract member Build: ISetterProvider<'Prototype, 'DbObject> * 'Prototype -> ISetter<'DbObject, 'Arg>

    /// <summary>
    /// The specification of field mapping override.
    /// </summary>
    type Override<'Prototype, 'DbObject, 'Arg>(propNames: string list, setter: ISetterProvider<'Prototype, 'DbObject> * 'Prototype -> ISetter<'DbObject, 'Arg>) =         

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
        new ([<ReflectedDefinition>] path: Expr<'Arg>, setter: ISetterProvider<'Prototype, 'DbObject> * 'Prototype -> ISetter<'DbObject, 'Arg>) = 
            Override(getPropChain(path), setter)

        interface IOverride<'Prototype, 'DbObject> with
            member __.IsRelevant (propertyName: string) = propNames |> List.tryHead |> Option.map ((=) propertyName) |> Option.defaultValue false
            member __.IsFinal = propNames |> List.isEmpty
            member __.Shift() = Override(propNames |> List.tail, setter)
            member __.Build<'Arg2>(provider: ISetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype) = 
                setter(provider, prototype) :?> ISetter<'DbObject, 'Arg2>


    type BaseSetterProvider<'Prototype, 'DbObject>(builders: IBuilder<'Prototype, 'DbObject> seq) = 

        member this.GetSetter(argType: Type, name: string, prototype: 'Prototype): obj = 
            let method = this.GetType().GetMethods() |> Seq.find (fun m -> m.Name = "GetSetter" && m.IsGenericMethod && m.GetGenericArguments().Length = 1)
            let gmethod = method.MakeGenericMethod(argType)
            gmethod.Invoke(this, [| name; prototype |])

        member this.GetSetter<'Arg>(name: string, prototype: 'Prototype): ISetter<'DbObject, 'Arg> = 
            match builders |> Seq.tryFind (fun b -> b.CanBuild typeof<'Arg>) with
            | Some builder -> builder.Build(name, this, prototype)
            | None -> failwithf "Could not find a setter builder for type: %A" typeof<'Arg>

        interface ISetterProvider<'Prototype, 'DbObject> with
            member this.Setter<'Arg>(name: string, prototype: 'Prototype): ISetter<'DbObject, 'Arg> = 
                this.GetSetter<'Arg>(name, prototype)
            member this.Setter(argType: Type, name: string, prototype: 'Prototype): obj = 
                this.GetSetter(argType, name, prototype)
            member __.Builder(argType: Type) = 
                builders |> Seq.tryFind (fun b -> b.CanBuild argType)
            member __.AllBuilders(argType: Type) = 
                builders |> Seq.filter (fun b -> b.CanBuild argType)

    type InitialDerivedSetterProvider<'Prototype, 'DbObject, 'Config>(baseProvider: ISetterProvider<'Prototype, 'DbObject>, config: 'Config, overrides: IOverride<'Prototype, 'DbObject> seq) =

        member this.GetSetter(argType: Type, name: string, prototype: 'Prototype): obj = 
            let method = this.GetType().GetMethods() |> Seq.find (fun m -> m.Name = "GetSetter" && m.IsGenericMethod && m.GetGenericArguments().Length = 1)
            let gmethod = method.MakeGenericMethod(argType)
            gmethod.Invoke(this, [| name; prototype |])

        member __.GetSetter<'Arg>(name: string, prototype: 'Prototype): ISetter<'DbObject, 'Arg> = 
            match baseProvider.Builder(typeof<'Arg>) with
            | Some builder -> 
                match builder with
                | :? IConfigurableBuilder<'Prototype, 'DbObject, 'Config> as cfgBuilder ->
                    let nextCfgDerived = DerivedSetterProvider(baseProvider, config, overrides)
                    cfgBuilder.Build(name, nextCfgDerived, config, prototype)
                | _ ->
                    let nextDerived = DerivedSetterProvider(baseProvider, (), overrides)
                    builder.Build(name, nextDerived, prototype)
            | None -> failwithf "Could not find a setter builder for type: %A" typeof<'Arg>

        interface ISetterProvider<'Prototype, 'DbObject> with
            member this.Setter<'Arg>(name: string, prototype: 'Prototype): ISetter<'DbObject, 'Arg> = 
                this.GetSetter<'Arg>(name, prototype)
            member this.Setter(argType: Type, name: string, prototype: 'Prototype): obj = 
                this.GetSetter(argType, name, prototype)
            member __.Builder(argType: Type) = 
                baseProvider.Builder(argType)
            member __.AllBuilders(argType: Type) = 
                baseProvider.AllBuilders(argType)

    and DerivedSetterProvider<'Prototype, 'DbObject, 'Config>(baseProvider: ISetterProvider<'Prototype, 'DbObject>, config: 'Config, overrides: IOverride<'Prototype, 'DbObject> seq) =

        member this.GetSetter(argType: Type, name: string, prototype: 'Prototype): obj = 
            let method = this.GetType().GetMethods() |> Seq.find (fun m -> m.Name = "GetSetter" && m.IsGenericMethod && m.GetGenericArguments().Length = 1)
            let gmethod = method.MakeGenericMethod(argType)
            gmethod.Invoke(this, [| name; prototype |])

        member __.GetSetter<'Arg>(name: string, prototype: 'Prototype): ISetter<'DbObject, 'Arg> = 
            let relevant = overrides |> Seq.filter (fun x -> x.IsRelevant name) |> Seq.map (fun x -> x.Shift()) |> Seq.toList
            let nextDerived = DerivedSetterProvider(baseProvider, (), relevant)
            match relevant |> List.tryFind (fun x -> x.IsFinal) with
            | Some ov -> ov.Build(nextDerived, prototype)
            | None -> 
                match baseProvider.Builder(typeof<'Arg>) with
                | Some builder -> 
                    match builder with
                    | :? IConfigurableBuilder<'Prototype, 'DbObject, 'Config> as cfgBuilder ->
                        let nextCfgDerived = DerivedSetterProvider(baseProvider, config, relevant)
                        cfgBuilder.Build(name, nextCfgDerived, config, prototype)
                    | _ ->
                        builder.Build(name, nextDerived, prototype)
                | None -> failwithf "Could not find a setter builder for type: %A" typeof<'Arg>

        interface ISetterProvider<'Prototype, 'DbObject> with
            member this.Setter<'Arg>(name: string, prototype: 'Prototype): ISetter<'DbObject, 'Arg> = 
                this.GetSetter<'Arg>(name, prototype)
            member this.Setter(argType: Type, name: string, prototype: 'Prototype): obj = 
                this.GetSetter(argType, name, prototype)
            member __.Builder(argType: Type) = 
                baseProvider.Builder(argType)
            member __.AllBuilders(argType: Type) = 
                baseProvider.AllBuilders(argType)

    type UnitBuilder<'Prototype, 'DbObject>() =

        interface IBuilder<'Prototype, 'DbObject> with

            member __.CanBuild (argType: Type) = argType = typeof<unit>

            member __.Build<'Arg> (_: string, _: ISetterProvider<'Prototype, 'DbObject>, _: 'Prototype) = 
                { new ISetter<'DbObject, 'Arg> with
                    member __.SetValue (_: 'Arg, _: 'DbObject) = 
                        ()
                    member __.SetNull(_: 'DbObject) = 
                        ()
                    member __.SetArtificial(_: 'DbObject) = 
                        ()
                }

                
    type SequenceBuilder<'Prototype, 'DbObject>() =

        interface IBuilder<'Prototype, 'DbObject> with

            member __.CanBuild (resType: Type) = Types.isCollectionType resType 

            member __.Build<'Arg> (_: string, _: ISetterProvider<'Prototype, 'DbObject>, _: 'Prototype) = 
                { new ISetter<'DbObject, 'Arg> with
                    member __.SetValue (_: 'Arg, _: 'DbObject) = 
                        ()
                    member __.SetNull(_: 'DbObject) = 
                        ()
                    member __.SetArtificial(_: 'DbObject) = 
                        ()
                }


    type Converter<'Prototype, 'DbObject, 'Source, 'Target>(convert: 'Source -> 'Target) =

        interface IBuilder<'Prototype, 'DbObject> with

            member __.CanBuild (argType: Type) = typeof<'Source> = argType

            member __.Build<'Arg> (name: string, provider: ISetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype) = 
                let setter = provider.Setter<'Target>(name, prototype) 
                let convert' = box convert :?> 'Arg -> 'Target
                { new ISetter<'DbObject, 'Arg> with
                    member __.SetValue (value: 'Arg, command: 'DbObject) = 
                        setter.SetValue(convert'(value), command)
                    member __.SetNull(command: 'DbObject) = 
                        setter.SetNull(command)
                    member __.SetArtificial(command: 'DbObject) = 
                        setter.SetArtificial(command)
                }

    type SeqItemConverter<'Prototype, 'DbObject, 'Source, 'Target>(convert: 'Source -> 'Target) =

        member __.MapSeq(source: 'Source seq) = source |> Seq.map convert

        interface IBuilder<'Prototype, 'DbObject> with

            member __.CanBuild (argType: Type) = 
                if not (Types.isCollectionType argType) then
                    false
                else
                    let elemType = Types.getElementType argType                        
                    typeof<'Source>.IsAssignableFrom(elemType)

            member this.Build<'Arg> (name: string, provider: ISetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype) = 
                let setter = provider.Setter<'Target seq>(name, prototype)                
                let sourceParam = Expression.Parameter(typeof<'Arg>)
                let seqMapMethod = this.GetType().GetMethod("MapSeq")
                let conversion = Expression.Call(Expression.Constant(this), seqMapMethod, sourceParam)
                let convert' = Expression.Lambda<Func<'Arg, 'Target seq>>(conversion, sourceParam).Compile()
                { new ISetter<'DbObject, 'Arg> with
                    member __.SetValue (value: 'Arg, command: 'DbObject) = 
                        setter.SetValue(convert'.Invoke(value), command)
                    member __.SetNull(command: 'DbObject) = 
                        setter.SetNull(command)
                    member __.SetArtificial(command: 'DbObject) = 
                        setter.SetArtificial(command)
                }

                
    type EnumConverter<'Prototype, 'DbObject, 'Underlying>() = 

        interface IBuilder<'Prototype, 'DbObject> with

            member __.CanBuild(argType: Type): bool = argType.IsEnum && argType.GetEnumUnderlyingType() = typeof<'Underlying>

            member __.Build(name: string, provider: ISetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype): ISetter<'DbObject, 'Arg> = 
                let setter = provider.Setter<'Underlying>(name, prototype)   
                let enumParam = Expression.Parameter(typeof<'Arg>)
                let convert = Expression.Lambda<Func<'Arg, 'Underlying>>(Expression.Convert(enumParam, typeof<'Underlying>), enumParam).Compile()                    
                { new ISetter<'DbObject, 'Arg> with
                    member __.SetValue (value: 'Arg, command: 'DbObject) = 
                        setter.SetValue(convert.Invoke(value), command)
                    member __.SetNull(command: 'DbObject) = 
                        setter.SetNull(command)
                    member __.SetArtificial(command: 'DbObject) = 
                        setter.SetArtificial(command)
                }

                
    type EnumSeqConverter<'Prototype, 'DbObject, 'Underlying>() = 

        member __.MapSeq<'Enum, 'Underlying>(mapper: Func<'Enum, 'Underlying>, sequence: 'Enum seq) = 
            sequence |> Seq.map mapper.Invoke

        interface IBuilder<'Prototype, 'DbObject> with

            member __.CanBuild(argType: Type): bool = 
                if not (Types.isCollectionType argType) then
                    false
                else
                    let elemType = Types.getElementType argType
                    elemType.IsEnum && elemType.GetEnumUnderlyingType() = typeof<'Underlying>

            member this.Build(name: string, provider: ISetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype): ISetter<'DbObject, 'Arg> = 
                let setter = provider.Setter<'Underlying seq>(name, prototype)   
                let enumType = Types.getElementType typeof<'Arg>
                let enumParam = Expression.Parameter(enumType)
                let itemConvert = Expression.Lambda(Expression.Convert(enumParam, typeof<'Underlying>), enumParam)
                let seqMapMethod = this.GetType().GetMethod("MapSeq").MakeGenericMethod(enumType, typeof<'Underlying>)
                let seqParam = Expression.Parameter(typeof<'Arg>)
                let conversion = Expression.Call(Expression.Constant(this), seqMapMethod, itemConvert, seqParam)
                let seqConvert = Expression.Lambda<Func<'Arg, 'Underlying seq>>(conversion, seqParam).Compile()
                { new ISetter<'DbObject, 'Arg> with
                    member __.SetValue (value: 'Arg, command: 'DbObject) = 
                        setter.SetValue(seqConvert.Invoke(value), command)
                    member __.SetNull(command: 'DbObject) = 
                        setter.SetNull(command)
                    member __.SetArtificial(command: 'DbObject) = 
                        setter.SetArtificial(command)
                }
        

    type UnionSeqBuilder<'Prototype, 'DbObject>() = 

        member __.GetUnderlyingValues<'Enum>() = 
            let properties = typeof<'Enum>.GetProperties()
            let underlyingValues = 
                [ for uc in FSharpType.GetUnionCases typeof<'Enum> do
                    (properties |> Array.find(fun p -> p.Name = uc.Name)).GetValue(null) :?> 'Enum, 
                    (uc.GetCustomAttributes(typeof<Models.UnionCaseTagAttribute>)[0] :?> Models.UnionCaseTagAttribute).Value
                ] 
            underlyingValues

        member __.MapSeq<'Enum>(eq: Func<'Enum, 'Enum, bool>, underlyingValues: ('Enum * string) list, sequence: 'Enum seq) = 
            let convert (x: 'Enum): string = underlyingValues |> List.find (fun (k, _) -> eq.Invoke(x, k)) |> snd
            sequence |> Seq.map convert

        interface IBuilder<'Prototype, 'DbObject> with

            member __.CanBuild(argType: Type): bool = 
                if not (Types.isCollectionType argType) then
                    false
                else
                    let elemType = Types.getElementType argType
                    FSharpType.IsUnion elemType
                        && 
                    elemType
                    |> FSharpType.GetUnionCases
                    |> Seq.forall (fun uc -> not (Seq.isEmpty (uc.GetCustomAttributes(typeof<Models.UnionCaseTagAttribute>))))

            member this.Build(name: string, provider: ISetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype): ISetter<'DbObject, 'Arg> = 
                let setter = provider.Setter<string seq>(name, prototype)   
                let enumType = Types.getElementType typeof<'Arg>
                let op1 = Expression.Parameter(enumType)
                let op2 = Expression.Parameter(enumType)
                let eq = Expression.Lambda(Expression.Equal(op1, op2), op1, op2)
                let seqParam = Expression.Parameter(typeof<'Arg>)
                let seqMapMethod = this.GetType().GetMethod("MapSeq").MakeGenericMethod(enumType)
                let underlyingValsMethod = this.GetType().GetMethod("GetUnderlyingValues").MakeGenericMethod(enumType)
                let underlyingValues = underlyingValsMethod.Invoke(this, [||])
                let conversion = Expression.Call(Expression.Constant(this), seqMapMethod, eq, Expression.Constant(underlyingValues), seqParam)
                let convert = Expression.Lambda<Func<'Arg, string seq>>(conversion, seqParam).Compile()
                { new ISetter<'DbObject, 'Arg> with
                    member __.SetValue (value: 'Arg, command: 'DbObject) = 
                        setter.SetValue(convert.Invoke(value), command)
                    member __.SetNull(command: 'DbObject) = 
                        setter.SetNull(command)
                    member __.SetArtificial(command: 'DbObject) = 
                        setter.SetArtificial(command)
                }

        
    type OptionBuilder<'Prototype, 'DbObject>() =

        member __.GetSetter(provider: ISetterProvider<'Prototype, 'DbObject>, name: string) (prototype: 'Prototype): ISetter<'DbObject, 'Underlying option> = 
            let underlying = provider.Setter<'Underlying>(name, prototype)
            { new ISetter<'DbObject, 'Underlying option> with
                member __.SetValue (value: 'Underlying option, command: 'DbObject) = 
                    match value with
                    | Some value -> underlying.SetValue(value, command)
                    | None -> underlying.SetNull(command)
                member __.SetNull(command: 'DbObject) = 
                    underlying.SetNull(command)
                member __.SetArtificial(command: 'DbObject) = 
                    underlying.SetArtificial(command)
            }

        interface IBuilder<'Prototype, 'DbObject> with

            member __.CanBuild(argType: Type): bool = Types.isOptionType argType

            member this.Build<'Arg>(name: string, provider: ISetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype): ISetter<'DbObject, 'Arg> = 
                let method = this.GetType().GetMethod("GetSetter")
                let gmethod = method.MakeGenericMethod(typeof<'Arg>.GetGenericArguments().[0])
                let assigner = gmethod.Invoke(this, [| provider; name; prototype |]) 
                assigner :?> ISetter<'DbObject, 'Arg>


    module FieldListAssigner = 
            
        let private callSetValue(assigner: Expression, value: Expression, command: Expression) = 
            Expression.Call(assigner, assigner.Type.GetInterface("ISetter`2").GetMethod("SetValue"), value, command) :> Expression

        let private callSetNull(assigner: Expression, command: Expression) = 
            Expression.Call(assigner, assigner.Type.GetInterface("ISetter`2").GetMethod("SetNull"), command) :> Expression

        let private callSetArtificial(assigner: Expression, command: Expression) = 
            Expression.Call(assigner, assigner.Type.GetInterface("ISetter`2").GetMethod("SetArtificial"), command) :> Expression

        let build(provider: ISetterProvider<'Prototype, 'DbObject>, fields: (PropertyInfo * string) seq, prototype: 'Prototype): ISetter<'DbObject, 'Arg> = 
                    
                let valueParam = Expression.Parameter(typeof<'Arg>)
                let commandParam = Expression.Parameter(typeof<'DbObject>)

                let assignments = 
                    [ for field, name in fields do
                        let fieldValue = Expression.Property(valueParam, field)
                        let assigner = Expression.Constant(provider.Setter(field.PropertyType, name, prototype))
                        callSetValue(assigner, fieldValue, commandParam), callSetNull(assigner, commandParam), callSetArtificial(assigner, commandParam)
                    ]

                let valueAssignments = assignments |> Seq.map (fun (setVal, _, _) -> setVal) |> Expression.Block
                let setValueFunction = Expression.Lambda<Action<'Arg, 'DbObject>>(valueAssignments, valueParam, commandParam).Compile()

                let nullAssignments = assignments |> Seq.map (fun (_, setNull, _) -> setNull) |> Expression.Block
                let setNullFunction = Expression.Lambda<Action<'DbObject>>(nullAssignments, commandParam).Compile()

                let artifAssignments = assignments |> Seq.map (fun (_, _, setArtif) -> setArtif) |> Expression.Block
                let setArtifFunction = Expression.Lambda<Action<'DbObject>>(artifAssignments, commandParam).Compile()

                { new ISetter<'DbObject, 'Arg> with
                    member __.SetValue (value: 'Arg, command: 'DbObject) = 
                        setValueFunction.Invoke(value, command)
                    member __.SetNull(command: 'DbObject) = 
                        setNullFunction.Invoke(command)
                    member __.SetArtificial(command: 'DbObject) = 
                        setArtifFunction.Invoke(command)
                }


    type RecordBuilder<'Prototype, 'DbObject>() = 
            
        interface IBuilder<'Prototype, 'DbObject> with

            member __.CanBuild(argType: Type): bool = FSharpType.IsRecord(argType)

            member __.Build(name: string, provider: ISetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype): ISetter<'DbObject, 'Arg> = 
                let fields = FSharpType.GetRecordFields typeof<'Arg> |> Array.map (fun f -> f, f.Name)
                FieldListAssigner.build(provider, fields, prototype)

        interface IConfigurableBuilder<'Prototype, 'DbObject, string * RecordNaming> with

            member __.Build(name: string, provider: ISetterProvider<'Prototype, 'DbObject>, (prefix: string, naming: RecordNaming), prototype: 'Prototype): ISetter<'DbObject, 'Arg> = 
                let fields = 
                    [ for f in FSharpType.GetRecordFields typeof<'Arg> do
                        match naming with
                        | RecordNaming.Fields -> f, f.Name
                        | RecordNaming.Prefix -> f, sprintf "%s%s" prefix f.Name
                        | RecordNaming.Path   -> f, sprintf "%s%s" name f.Name
                    ]
                FieldListAssigner.build(provider, fields, prototype)


    type TupleBuilder<'Prototype, 'DbObject>() = 

        interface IBuilder<'Prototype, 'DbObject> with

            member __.CanBuild(argType: Type): bool = FSharpType.IsTuple argType

            member __.Build(name: string, provider: ISetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype): ISetter<'DbObject, 'Arg> =                     
                let fields = typeof<'Arg>.GetProperties() |> Array.mapi (fun i f -> f, sprintf "%s%d" name (i + 1))
                FieldListAssigner.build(provider, fields, prototype)  


    type UnionBuilder<'Prototype, 'DbObject>() = 

        member __.CreateUnionCaseSetter<'Union, 'UnionCase>(provider: ISetterProvider<'Prototype, 'DbObject>, name: string, prefix: string, caseName: string, naming: UnionNaming, fields: PropertyInfo array, prototype: 'Prototype): ISetter<'DbObject, 'Union> =             
            let fieldsHaveNames = fields |> Array.exists (fun f -> not (Regex.Match(f.Name, "Item[0-9]*").Success))
            let named = 
                [ for f in fields do
                    let fieldName1 = if fieldsHaveNames then f.Name else f.Name.Replace("Item", caseName)
                    let fieldName2 = if naming &&& UnionNaming.CaseNames <> UnionNaming.Fields && fieldsHaveNames then caseName + fieldName1 else fieldName1
                    let fieldName3 = if naming &&& UnionNaming.Prefix <> UnionNaming.Fields && naming &&& UnionNaming.Path = UnionNaming.Fields then prefix + fieldName2 else fieldName2
                    let fieldName4 = if naming &&& UnionNaming.Path <> UnionNaming.Fields then name + fieldName3 else fieldName3
                    f, fieldName4
                ]
            let unionCaseBuilder: ISetter<'DbObject, 'UnionCase> = FieldListAssigner.build(provider, named, prototype)  
            { new ISetter<'DbObject, 'Union> with
                member __.SetValue (value: 'Union, command: 'DbObject) = 
                    unionCaseBuilder.SetValue(value |> box :?> 'UnionCase, command)
                member __.SetNull(command: 'DbObject) = 
                    unionCaseBuilder.SetNull(command)
                member __.SetArtificial(command: 'DbObject) = 
                    unionCaseBuilder.SetArtificial(command)
            }

        member this.Build(name: string, prefix: string, naming: UnionNaming, provider: ISetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype): ISetter<'DbObject, 'Arg> = 
            let tagSetter = provider.Setter<string>(name, prototype)   
            let uc = Expression.Parameter(typeof<'Arg>)
            let getTag = Expression.Lambda<Func<'Arg, int>>(Expression.Property(uc, "Tag"), uc).Compile()
            let createUCSetterMethod = this.GetType().GetMethod("CreateUnionCaseSetter")
            let dbTags = 
                [ for uc in FSharpType.GetUnionCases typeof<'Arg> do                         
                    uc.Tag, (uc.GetCustomAttributes(typeof<Models.UnionCaseTagAttribute>)[0] :?> Models.UnionCaseTagAttribute).Value
                ] 
            let caseSetters = 
                [ for uc in FSharpType.GetUnionCases typeof<'Arg> do                         
                    uc.Tag, 
                    let fields = uc.GetFields()
                    if Array.isEmpty fields then
                        let emptyBuilder = UnitBuilder<'Prototype, 'DbObject>() :> IBuilder<'Prototype, 'DbObject>
                        emptyBuilder.Build<'Arg>(name, provider, prototype)
                    else
                        let gmethod = createUCSetterMethod.MakeGenericMethod(typeof<'Arg>, typeof<'Arg>.GetNestedType(uc.Name))
                        gmethod.Invoke(this, [| provider; name; prefix; uc.Name; naming; fields; prototype |]) :?> ISetter<'DbObject, 'Arg>
                ] 
            { new ISetter<'DbObject, 'Arg> with
                member __.SetValue (value: 'Arg, command: 'DbObject) = 
                    let dbTag = dbTags |> List.find (fst >> (=) (getTag.Invoke value)) |> snd
                    tagSetter.SetValue(dbTag, command)
                    for (_, setter) in caseSetters do
                        setter.SetNull(command)
                    let setter = caseSetters |> List.find (fst >> (=) (getTag.Invoke value)) |> snd
                    setter.SetValue(value, command)
                member __.SetNull(command: 'DbObject) = 
                    tagSetter.SetNull(command)
                    for (_, setter) in caseSetters do
                        setter.SetNull(command)
                member __.SetArtificial(command: 'DbObject) = 
                    tagSetter.SetArtificial(command)
                    for (_, setter) in caseSetters do
                        setter.SetArtificial(command)
            }

        interface IBuilder<'Prototype, 'DbObject> with

            member __.CanBuild(argType: Type): bool = 
                FSharpType.IsUnion argType
                    && 
                argType
                |> FSharpType.GetUnionCases
                |> Seq.forall (fun uc -> not (Seq.isEmpty (uc.GetCustomAttributes(typeof<Models.UnionCaseTagAttribute>))))

            member this.Build(name: string, provider: ISetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype): ISetter<'DbObject, 'Arg> = 
                this.Build(name, name, UnionNaming.Fields, provider, prototype)

        interface IConfigurableBuilder<'Prototype, 'DbObject, string * UnionNaming> with

            member this.Build(name: string, provider: ISetterProvider<'Prototype, 'DbObject>, (prefix: string, naming: UnionNaming), prototype: 'Prototype): ISetter<'DbObject, 'Arg> = 
                this.Build(name, prefix, naming, provider, prototype)


    type Configurator<'Prototype, 'DbObject, 'Config>(getConfig: string -> 'Config, canBuild: Type -> bool) = 

        member this.Build(name: string, provider: ISetterProvider<'Prototype,'DbObject>, config: 'Config, prototype: 'Prototype): ISetter<'DbObject,'Arg> = 
            provider.AllBuilders(typeof<'Arg>) 
            |> Seq.tryPick (function | :? IConfigurableBuilder<'Prototype, 'DbObject, 'Config> as builder when builder <> this -> Some builder | _ -> None)   
            |> Option.map (fun builder -> builder.Build<'Arg>(name, provider, config, prototype))
            |> Option.defaultWith(fun () ->  failwithf "Could not find a configurable setter builder for type: %A and config: %A" typeof<'Arg> typeof<'Config>)

        interface IBuilder<'Prototype, 'DbObject> with

            member __.CanBuild(argType: Type) = canBuild(argType)
        
            member this.Build(name: string, provider: ISetterProvider<'Prototype,'DbObject>, prototype: 'Prototype): ISetter<'DbObject,'Arg> = 
                this.Build(name, provider, getConfig(name), prototype)

        interface IConfigurableBuilder<'Prototype, 'DbObject, 'Config> with

            member this.Build(name: string, provider: ISetterProvider<'Prototype,'DbObject>, config: 'Config, prototype: 'Prototype): ISetter<'DbObject,'Arg> = 
                this.Build(name, provider, config, prototype)


    let getDefaultBuilders(): IBuilder<'Prototype, 'DbObject> list = 
        let dateOnlyToDateTime (dtOnly: DateOnly) = dtOnly.ToDateTime(TimeOnly.MinValue)
        let timeOnlyToTimeSpan (tmOnly: TimeOnly) = tmOnly.ToTimeSpan()
        [
            Converter<'Prototype, 'DbObject, _, _>(dateOnlyToDateTime)
            Converter<'Prototype, 'DbObject, _, _>(timeOnlyToTimeSpan)
            SeqItemConverter<'Prototype, 'DbObject, _, _>(dateOnlyToDateTime)
            SeqItemConverter<'Prototype, 'DbObject, _, _>(timeOnlyToTimeSpan)
            UnionBuilder<'Prototype, 'DbObject>()
            UnionSeqBuilder<'Prototype, 'DbObject>()
            EnumConverter<'Prototype, 'DbObject, char>()
            EnumConverter<'Prototype, 'DbObject, int>()
            EnumSeqConverter<'Prototype, 'DbObject, char>()
            EnumSeqConverter<'Prototype, 'DbObject, int>()
            UnitBuilder<'Prototype, 'DbObject>()
            RecordBuilder<'Prototype, 'DbObject>()
            TupleBuilder<'Prototype, 'DbObject>()
            OptionBuilder<'Prototype, 'DbObject>()
            SequenceBuilder<'Prototype, 'DbObject>()
        ]


    type GenericSetterBuilder<'Prototype, 'DbObject>() = 

        /// <summary>
        /// Creates a builder handling no parameters.
        /// </summary>
        static member Unit: BuildSetter<'Prototype, 'DbObject, Unit> = 
            fun (provider, prototype) -> provider.Setter<unit>("", prototype)

        /// <summary>
        /// Creates a builder handling simple values.
        /// </summary>
        /// <param name="name">
        /// The parameter name or prefix (for indirect parameters).
        /// </param>
        static member Simple<'Arg> (name: string): BuildSetter<'Prototype, 'DbObject, 'Arg> = 
            fun (provider, prototype) -> provider.Setter<'Arg>(name, prototype)

        /// <summary>
        /// Creates a builder handling integer values.
        /// </summary>
        /// <param name="name">
        /// The parameter name.
        /// </param>
        static member Int (name: string): BuildSetter<'Prototype, 'DbObject, int> = 
            fun (provider, prototype) -> provider.Setter<int>(name, prototype)

        /// <summary>
        /// Creates a builder handling 64-bit integer values.
        /// </summary>
        /// <param name="name">
        /// The parameter name.
        /// </param>
        static member Int64 (name: string): BuildSetter<'Prototype, 'DbObject, int64> = 
            fun (provider, prototype) -> provider.Setter<int64>(name, prototype)

        /// <summary>
        /// Creates a builder handling byte values.
        /// </summary>
        /// <param name="name">
        /// The parameter name.
        /// </param>
        static member Byte (name: string): BuildSetter<'Prototype, 'DbObject, byte> = 
            fun (provider, prototype) -> provider.Setter<byte>(name, prototype)

        /// <summary>
        /// Creates a builder handling char values.
        /// </summary>
        /// <param name="name">
        /// The parameter name.
        /// </param>
        static member Char (name: string): BuildSetter<'Prototype, 'DbObject, char> = 
            fun (provider, prototype) -> provider.Setter<char>(name, prototype)

        /// <summary>
        /// Creates a builder handling string values.
        /// </summary>
        /// <param name="name">
        /// The parameter name.
        /// </param>
        static member String (name: string): BuildSetter<'Prototype, 'DbObject, string> = 
            fun (provider, prototype) -> provider.Setter<string>(name, prototype)

        /// <summary>
        /// Creates a builder handling DateTime values.
        /// </summary>
        /// <param name="name">
        /// The parameter name.
        /// </param>
        static member DateTime (name: string): BuildSetter<'Prototype, 'DbObject, DateTime> = 
            fun (provider, prototype) -> provider.Setter<DateTime>(name, prototype)

        /// <summary>
        /// Creates a builder handling DateOnly values.
        /// </summary>
        /// <param name="name">
        /// The parameter name.
        /// </param>
        static member DateOnly (name: string): BuildSetter<'Prototype, 'DbObject, DateOnly> = 
            fun (provider, prototype) -> provider.Setter<DateOnly>(name, prototype)

        /// <summary>
        /// Creates a builder handling TimeOnly values.
        /// </summary>
        /// <param name="name">
        /// The parameter name.
        /// </param>
        static member TimeOnly (name: string): BuildSetter<'Prototype, 'DbObject, TimeOnly> = 
            fun (provider, prototype) -> provider.Setter<TimeOnly>(name, prototype)

        /// <summary>
        /// Creates a builder handling decimal values.
        /// </summary>
        /// <param name="name">
        /// The parameter name.
        /// </param>
        static member Decimal (name: string): BuildSetter<'Prototype, 'DbObject, decimal> = 
            fun (provider, prototype) -> provider.Setter<decimal>(name, prototype)

        /// <summary>
        /// Creates a builder handling float values.
        /// </summary>
        /// <param name="name">
        /// The parameter name.
        /// </param>
        static member Float (name: string): BuildSetter<'Prototype, 'DbObject, float> = 
            fun (provider, prototype) -> provider.Setter<float>(name, prototype)

        /// <summary>
        /// Creates a builder handling double values.
        /// </summary>
        /// <param name="name">
        /// The parameter name.
        /// </param>
        static member Double (name: string): BuildSetter<'Prototype, 'DbObject, double> = 
            fun (provider, prototype) -> provider.Setter<double>(name, prototype)

        /// <summary>
        /// Creates a builder handling bool values.
        /// </summary>
        /// <param name="name">
        /// The parameter name.
        /// </param>
        static member Bool (name: string): BuildSetter<'Prototype, 'DbObject, bool> = 
            fun (provider, prototype) -> provider.Setter<bool>(name, prototype)

        /// <summary>
        /// Creates a builder handling Guid values.
        /// </summary>
        /// <param name="name">
        /// The parameter name.
        /// </param>
        static member Guid (name: string): BuildSetter<'Prototype, 'DbObject, Guid> = 
            fun (provider, prototype) -> provider.Setter<Guid>(name, prototype)

        /// <summary>
        /// Creates a builder handling byte array values.
        /// </summary>
        /// <param name="name">
        /// The parameter name.
        /// </param>
        static member ByteArray (name: string): BuildSetter<'Prototype, 'DbObject, byte array> = 
            fun (provider, prototype) -> provider.Setter<byte array>(name, prototype)

        /// <summary>
        /// Creates a builder handling optional values.
        /// </summary>
        /// <param name="createUnderlyingSetter">
        /// The builder of the option unerlying type.
        /// </param>
        static member Optional<'Arg> (createUnderlyingSetter: BuildSetter<'Prototype, 'DbObject, 'Arg>) = 
            fun (provider: ISetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype) ->
                let underlyingSetter = createUnderlyingSetter(provider, prototype)
                { new ISetter<'DbObject, 'Arg option> with
                    member __.SetValue (value: 'Arg option, command: 'DbObject) = 
                        match value with
                        | Some value -> underlyingSetter.SetValue(value, command)
                        | None -> underlyingSetter.SetNull(command)
                    member __.SetNull(command: 'DbObject) = 
                        underlyingSetter.SetNull(command)
                    member __.SetArtificial(command: 'DbObject) = 
                        underlyingSetter.SetArtificial(command)
                }

        /// <summary>
        /// Creates a builder handling 2-element tuple types.
        /// </summary>
        /// <param name="createItem1Setter">
        /// The builder of the first tuple element.
        /// </param>
        /// <param name="createItem2Setter">
        /// The builder of the second tuple element.
        /// </param>
        static member Tuple<'Arg1, 'Arg2>(
                createItem1Setter: BuildSetter<'Prototype, 'DbObject, 'Arg1>, 
                createItem2Setter: BuildSetter<'Prototype, 'DbObject, 'Arg2>) = 
            fun (provider: ISetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype) ->
                let item1Setter = createItem1Setter(provider, prototype)
                let item2Setter = createItem2Setter(provider, prototype)
                { new ISetter<'DbObject, 'Arg1 * 'Arg2> with
                    member __.SetValue((val1: 'Arg1, val2: 'Arg2), command: 'DbObject) =
                        item1Setter.SetValue(val1, command)
                        item2Setter.SetValue(val2, command)
                    member __.SetNull(command: 'DbObject) = 
                        item1Setter.SetNull(command)
                        item2Setter.SetNull(command)
                    member __.SetArtificial(command: 'DbObject) = 
                        item1Setter.SetArtificial(command)
                        item2Setter.SetArtificial(command)
                }
        
        /// <summary>
        /// Creates a builder handling 2-element tuple types.
        /// </summary>
        /// <param name="name1">
        /// The column or prefix (for indirect parameters) name of the first tuple element.
        /// </param>
        /// <param name="name2">
        /// The column or prefix (for indirect parameters) name of the second tuple element.
        /// </param>
        static member Tuple<'Arg1, 'Arg2>(name1: string, name2: string) = 
            fun (provider: ISetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype) ->
                let item1Setter = provider.Setter<'Arg1>(name1, prototype) 
                let item2Setter = provider.Setter<'Arg2>(name2, prototype)
                { new ISetter<'DbObject, 'Arg1 * 'Arg2> with
                    member __.SetValue((val1: 'Arg1, val2: 'Arg2), command: 'DbObject) =
                        item1Setter.SetValue(val1, command)
                        item2Setter.SetValue(val2, command)
                    member __.SetNull(command: 'DbObject) = 
                        item1Setter.SetNull(command)
                        item2Setter.SetNull(command)
                    member __.SetArtificial(command: 'DbObject) = 
                        item1Setter.SetArtificial(command)
                        item2Setter.SetArtificial(command)
                }

        /// <summary>
        /// Creates a builder handling 3-element tuple types.
        /// </summary>
        /// <param name="createItem1Setter">
        /// The builder of the first tuple element.
        /// </param>
        /// <param name="createItem2Setter">
        /// The builder of the second tuple element.
        /// </param>
        /// <param name="createItem3Setter">
        /// The builder of the third tuple element.
        /// </param>
        static member Tuple<'Arg1, 'Arg2, 'Arg3>(
                createItem1Setter: BuildSetter<'Prototype, 'DbObject, 'Arg1>, 
                createItem2Setter: BuildSetter<'Prototype, 'DbObject, 'Arg2>, 
                createItem3Setter: BuildSetter<'Prototype, 'DbObject, 'Arg3>) = 
            fun (provider: ISetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype) ->
                let item1Setter = createItem1Setter(provider, prototype)
                let item2Setter = createItem2Setter(provider, prototype)
                let item3Setter = createItem3Setter(provider, prototype)
                { new ISetter<'DbObject, 'Arg1 * 'Arg2 * 'Arg3> with
                    member __.SetValue((val1: 'Arg1, val2: 'Arg2, val3: 'Arg3), command: 'DbObject) =
                        item1Setter.SetValue(val1, command)
                        item2Setter.SetValue(val2, command)
                        item3Setter.SetValue(val3, command)
                    member __.SetNull(command: 'DbObject) = 
                        item1Setter.SetNull(command)
                        item2Setter.SetNull(command)
                        item3Setter.SetNull(command)
                    member __.SetArtificial(command: 'DbObject) = 
                        item1Setter.SetArtificial(command)
                        item2Setter.SetArtificial(command)
                        item3Setter.SetArtificial(command)
                }
        
        /// <summary>
        /// Creates a builder handling 3-element tuple types.
        /// </summary>
        /// <param name="name1">
        /// The column or prefix (for indirect parameters) name of the first tuple element.
        /// </param>
        /// <param name="name2">
        /// The column or prefix (for indirect parameters) name of the second tuple element.
        /// </param>
        /// <param name="name3">
        /// The column or prefix (for indirect parameters) name of the third tuple element.
        /// </param>
        static member Tuple<'Arg1, 'Arg2, 'Arg3>(name1: string, name2: string, name3: string) = 
            fun (provider: ISetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype) ->
                let item1Setter = provider.Setter<'Arg1>(name1, prototype) 
                let item2Setter = provider.Setter<'Arg2>(name2, prototype)
                let item3Setter = provider.Setter<'Arg3>(name3, prototype)
                { new ISetter<'DbObject, 'Arg1 * 'Arg2 * 'Arg3> with
                    member __.SetValue((val1: 'Arg1, val2: 'Arg2, val3: 'Arg3), command: 'DbObject) =
                        item1Setter.SetValue(val1, command)
                        item2Setter.SetValue(val2, command)
                        item3Setter.SetValue(val3, command)
                    member __.SetNull(command: 'DbObject) = 
                        item1Setter.SetNull(command)
                        item2Setter.SetNull(command)
                        item3Setter.SetNull(command)
                    member __.SetArtificial(command: 'DbObject) = 
                        item1Setter.SetArtificial(command)
                        item2Setter.SetArtificial(command)
                        item3Setter.SetArtificial(command)
                }

        /// <summary>
        /// Creates a builder handling 4-element tuple types.
        /// </summary>
        /// <param name="createItem1Setter">
        /// The builder of the first tuple element.
        /// </param>
        /// <param name="createItem2Setter">
        /// The builder of the second tuple element.
        /// </param>
        /// <param name="createItem3Setter">
        /// The builder of the third tuple element.
        /// </param>
        /// <param name="createItem4Setter">
        /// The builder of the fourth tuple element.
        /// </param>
        static member Tuple<'Arg1, 'Arg2, 'Arg3, 'Arg4>(
                createItem1Setter: BuildSetter<'Prototype, 'DbObject, 'Arg1>, 
                createItem2Setter: BuildSetter<'Prototype, 'DbObject, 'Arg2>, 
                createItem3Setter: BuildSetter<'Prototype, 'DbObject, 'Arg3>, 
                createItem4Setter: BuildSetter<'Prototype, 'DbObject, 'Arg4>) = 
            fun (provider: ISetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype) ->
                let item1Setter = createItem1Setter(provider, prototype)
                let item2Setter = createItem2Setter(provider, prototype)
                let item3Setter = createItem3Setter(provider, prototype)
                let item4Setter = createItem4Setter(provider, prototype)
                { new ISetter<'DbObject, 'Arg1 * 'Arg2 * 'Arg3 * 'Arg4> with
                    member __.SetValue((val1: 'Arg1, val2: 'Arg2, val3: 'Arg3, val4: 'Arg4), command: 'DbObject) =
                        item1Setter.SetValue(val1, command)
                        item2Setter.SetValue(val2, command)
                        item3Setter.SetValue(val3, command)
                        item4Setter.SetValue(val4, command)
                    member __.SetNull(command: 'DbObject) = 
                        item1Setter.SetNull(command)
                        item2Setter.SetNull(command)
                        item3Setter.SetNull(command)
                        item4Setter.SetNull(command)
                    member __.SetArtificial(command: 'DbObject) = 
                        item1Setter.SetArtificial(command)
                        item2Setter.SetArtificial(command)
                        item3Setter.SetArtificial(command)
                        item4Setter.SetArtificial(command)
                }
        
        /// <summary>
        /// Creates a builder handling 4-element tuple types.
        /// </summary>
        /// <param name="name1">
        /// The column or prefix (for indirect parameters) name of the first tuple element.
        /// </param>
        /// <param name="name2">
        /// The column or prefix (for indirect parameters) name of the second tuple element.
        /// </param>
        /// <param name="name3">
        /// The column or prefix (for indirect parameters) name of the third tuple element.
        /// </param>
        /// <param name="name4">
        /// The column or prefix (for indirect parameters) name of the fourth tuple element.
        /// </param>
        static member Tuple<'Arg1, 'Arg2, 'Arg3, 'Arg4>(name1: string, name2: string, name3: string, name4: string) = 
            fun (provider: ISetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype) ->
                let item1Setter = provider.Setter<'Arg1>(name1, prototype) 
                let item2Setter = provider.Setter<'Arg2>(name2, prototype)
                let item3Setter = provider.Setter<'Arg3>(name3, prototype)
                let item4Setter = provider.Setter<'Arg4>(name4, prototype)
                { new ISetter<'DbObject, 'Arg1 * 'Arg2 * 'Arg3 * 'Arg4> with
                    member __.SetValue((val1: 'Arg1, val2: 'Arg2, val3: 'Arg3, val4: 'Arg4), command: 'DbObject) =
                        item1Setter.SetValue(val1, command)
                        item2Setter.SetValue(val2, command)
                        item3Setter.SetValue(val3, command)
                        item4Setter.SetValue(val4, command)
                    member __.SetNull(command: 'DbObject) = 
                        item1Setter.SetNull(command)
                        item2Setter.SetNull(command)
                        item3Setter.SetNull(command)
                        item4Setter.SetNull(command)
                    member __.SetArtificial(command: 'DbObject) = 
                        item1Setter.SetArtificial(command)
                        item2Setter.SetArtificial(command)
                        item3Setter.SetArtificial(command)
                        item4Setter.SetArtificial(command)
                }


        /// <summary>
        /// Creates a builder handling 5-element tuple types.
        /// </summary>
        /// <param name="createItem1Setter">
        /// The builder of the first tuple element.
        /// </param>
        /// <param name="createItem2Setter">
        /// The builder of the second tuple element.
        /// </param>
        /// <param name="createItem3Setter">
        /// The builder of the third tuple element.
        /// </param>
        /// <param name="createItem4Setter">
        /// The builder of the fourth tuple element.
        /// </param>
        /// <param name="createItem5Setter">
        /// The builder of the fifth tuple element.
        /// </param>
        static member Tuple<'Arg1, 'Arg2, 'Arg3, 'Arg4, 'Arg5>(
                createItem1Setter: BuildSetter<'Prototype, 'DbObject, 'Arg1>, 
                createItem2Setter: BuildSetter<'Prototype, 'DbObject, 'Arg2>, 
                createItem3Setter: BuildSetter<'Prototype, 'DbObject, 'Arg3>, 
                createItem4Setter: BuildSetter<'Prototype, 'DbObject, 'Arg4>, 
                createItem5Setter: BuildSetter<'Prototype, 'DbObject, 'Arg5>) = 
            fun (provider: ISetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype) ->
                let item1Setter = createItem1Setter(provider, prototype)
                let item2Setter = createItem2Setter(provider, prototype)
                let item3Setter = createItem3Setter(provider, prototype)
                let item4Setter = createItem4Setter(provider, prototype)
                let item5Setter = createItem5Setter(provider, prototype)
                { new ISetter<'DbObject, 'Arg1 * 'Arg2 * 'Arg3 * 'Arg4 * 'Arg5> with
                    member __.SetValue((val1: 'Arg1, val2: 'Arg2, val3: 'Arg3, val4: 'Arg4, val5: 'Arg5), command: 'DbObject) =
                        item1Setter.SetValue(val1, command)
                        item2Setter.SetValue(val2, command)
                        item3Setter.SetValue(val3, command)
                        item4Setter.SetValue(val4, command)
                        item5Setter.SetValue(val5, command)
                    member __.SetNull(command: 'DbObject) = 
                        item1Setter.SetNull(command)
                        item2Setter.SetNull(command)
                        item3Setter.SetNull(command)
                        item4Setter.SetNull(command)
                        item5Setter.SetNull(command)
                    member __.SetArtificial(command: 'DbObject) = 
                        item1Setter.SetArtificial(command)
                        item2Setter.SetArtificial(command)
                        item3Setter.SetArtificial(command)
                        item4Setter.SetArtificial(command)
                        item5Setter.SetArtificial(command)
                }
        
        /// <summary>
        /// Creates a builder handling 5-element tuple types.
        /// </summary>
        /// <param name="name1">
        /// The column or prefix (for indirect parameters) name of the first tuple element.
        /// </param>
        /// <param name="name2">
        /// The column or prefix (for indirect parameters) name of the second tuple element.
        /// </param>
        /// <param name="name3">
        /// The column or prefix (for indirect parameters) name of the third tuple element.
        /// </param>
        /// <param name="name4">
        /// The column or prefix (for indirect parameters) name of the fourth tuple element.
        /// </param>
        /// <param name="name5">
        /// The column or prefix (for indirect parameters) name of the fifth tuple element.
        /// </param>
        static member Tuple<'Arg1, 'Arg2, 'Arg3, 'Arg4, 'Arg5>(name1: string, name2: string, name3: string, name4: string, name5: string) = 
            fun (provider: ISetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype) ->
                let item1Setter = provider.Setter<'Arg1>(name1, prototype) 
                let item2Setter = provider.Setter<'Arg2>(name2, prototype)
                let item3Setter = provider.Setter<'Arg3>(name3, prototype)
                let item4Setter = provider.Setter<'Arg4>(name4, prototype)
                let item5Setter = provider.Setter<'Arg5>(name4, prototype)
                { new ISetter<'DbObject, 'Arg1 * 'Arg2 * 'Arg3 * 'Arg4 * 'Arg5> with
                    member __.SetValue((val1: 'Arg1, val2: 'Arg2, val3: 'Arg3, val4: 'Arg4, val5: 'Arg5), command: 'DbObject) =
                        item1Setter.SetValue(val1, command)
                        item2Setter.SetValue(val2, command)
                        item3Setter.SetValue(val3, command)
                        item4Setter.SetValue(val4, command)
                        item5Setter.SetValue(val5, command)
                    member __.SetNull(command: 'DbObject) = 
                        item1Setter.SetNull(command)
                        item2Setter.SetNull(command)
                        item3Setter.SetNull(command)
                        item4Setter.SetNull(command)
                        item5Setter.SetNull(command)
                    member __.SetArtificial(command: 'DbObject) = 
                        item1Setter.SetArtificial(command)
                        item2Setter.SetArtificial(command)
                        item3Setter.SetArtificial(command)
                        item4Setter.SetArtificial(command)
                        item5Setter.SetArtificial(command)
                }

        /// <summary>
        /// Creates a builder handling optional types.
        /// </summary>
        /// <param name="name">
        /// The column or prefix (for indirect parameters) name.
        /// </param>
        static member Optional<'Arg> (name: string) = 
            fun (provider: ISetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype) -> 
                provider.Setter<'Arg option>(name, prototype)

        /// <summary>
        /// Creates a builder handling record types.
        /// </summary>
        /// <param name="prefix">
        /// The prefix of parameter names representing record fields in a query.
        /// </param>
        /// <param name="naming">
        /// The enum determining how columns representing record fields are named.
        /// </param>
        /// <param name="overrides">
        /// Objects allowing to override default mappings of particular fields.
        /// </param>
        static member Record<'Arg>(?prefix: string, ?naming: RecordNaming, ?overrides: IOverride<'Prototype, 'DbObject> seq) = 
            fun (provider: ISetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype) ->
                let provider: ISetterProvider<'Prototype, 'DbObject> = 
                    match naming with 
                    | Some naming ->
                        InitialDerivedSetterProvider<'Prototype, 'DbObject, string * RecordNaming>(provider, (defaultArg prefix "", naming), defaultArg overrides []) 
                    | None ->                     
                        InitialDerivedSetterProvider<'Prototype, 'DbObject, unit>(provider, (), defaultArg overrides []) 
                provider.Setter<'Arg>(defaultArg prefix "", prototype)

        /// <summary>
        /// Creates a builder handling discriminated union types.
        /// </summary>
        /// <param name="name">
        /// The union tag column name.
        /// </param>
        /// <param name="naming">
        /// Flags determining how columns representing union values are named.
        /// </param>
        /// <param name="overrides">
        /// Objects allowing to override default mappings of particular fields.
        /// </param>
        static member Union<'Arg>(?name: string, ?naming: UnionNaming, ?overrides: IOverride<'Prototype, 'DbObject> seq) = 
            fun (provider: ISetterProvider<'Prototype, 'DbObject>, prototype: 'Prototype) ->
                let provider: ISetterProvider<'Prototype, 'DbObject> = 
                    match naming with
                    | Some naming ->
                        InitialDerivedSetterProvider<'Prototype, 'DbObject, string * UnionNaming>(provider, (defaultArg name "", naming), defaultArg overrides [])
                    | None -> 
                        InitialDerivedSetterProvider<'Prototype, 'DbObject, unit>(provider, (), defaultArg overrides [])
                provider.Setter<'Arg>(defaultArg name "", prototype)
                
