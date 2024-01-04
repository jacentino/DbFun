namespace DbFun.Core.Builders

open System
open System.Data 
open DbFun.Core

type IParamSetter<'Arg> = GenericSetters.ISetter<IDbCommand, 'Arg>

type IParamSetterProvider = GenericSetters.ISetterProvider<unit, IDbCommand>

type ParamSpecifier<'Arg> = GenericSetters.SetterSpecifier<unit, IDbCommand, 'Arg>

module ParamsImpl = 

    type IBuilder = GenericSetters.IBuilder<unit, IDbCommand>

    type SimpleBuilder() =

        member __.FindOrCreateParam(command: IDbCommand, name: string, itemIndex: int option) = 
            let name' = name + (itemIndex |> Option.map string |> Option.defaultValue "")
            let index = command.Parameters.IndexOf(name)
            if index = -1 then
                let param = command.CreateParameter()
                param.ParameterName <- name'
                command.Parameters.Add param |> ignore
                param
            else
                command.Parameters.[index] :?> IDbDataParameter 

        member __.Update(param: IDbDataParameter, value: obj) = 
            if param.Value = null || param.Value = DBNull.Value then
                param.Value <- value
            else
                failwithf "Duplicate parameter definition: %s" param.ParameterName

        member __.GetArtificialValue<'Type>(): obj = 
            if typeof<'Type> = typeof<string> then box ""
            elif typeof<'Type> = typeof<DateTime> then box DateTime.Now
            elif typeof<'Type> = typeof<byte[]> then box [||]
            elif typeof<'Type>.IsClass then null
            else box Unchecked.defaultof<'Type>

        interface IBuilder with

            member __.CanBuild (argType: Type) = Types.isSimpleType(argType)

            member this.Build<'Arg> (name: string, _, ()) = 
                { new IParamSetter<'Arg> with
                    member __.SetValue (value: 'Arg, index: int option, command: IDbCommand) = 
                        let param = this.FindOrCreateParam(command, name, index)
                        this.Update(param, value)
                    member __.SetNull(index: int option, command: IDbCommand) = 
                        let param = this.FindOrCreateParam(command, name, index)
                        this.Update(param, DBNull.Value)
                    member __.SetArtificial(index: int option, command: IDbCommand) = 
                        let param = this.FindOrCreateParam(command, name, index)
                        param.Value <- this.GetArtificialValue<'Arg>()
                }


    type SequenceIndexingBuilder() =

        static member CreateParamSetter(itemSetter: IParamSetter<'Item>, setValue: IDbCommand -> 'Collection -> unit) = 
            { new IParamSetter<'Collection> with
                member __.SetValue (items: 'Collection, _: int option, command: IDbCommand) = 
                    let offset = command.Parameters.Count
                    setValue command items
                    let names = List.init (command.Parameters.Count - offset) (fun i -> (command.Parameters[i + offset] :?> IDataParameter).ParameterName)
                    if not names.IsEmpty then
                        let baseName = names.Head.Remove(names.Head.Length - 1)
                        if names |> List.mapi (fun i name -> name, sprintf "%s%d" baseName i) |> List.forall (fun (name1, name2) -> name1 = name2) then
                            let paramNames = names |> List.map (sprintf "@%s") |> String.concat ", " |> sprintf "(%s)"
                            command.CommandText <- command.CommandText.Replace(sprintf "(@%s)" baseName, paramNames)
                member __.SetNull(index: int option, command: IDbCommand) = 
                    itemSetter.SetNull(index, command)
                member __.SetArtificial(index: int option, command: IDbCommand) = 
                    itemSetter.SetArtificial(index, command)
            }

        member __.CreateSeqSetter<'Item>(name: string, provider: IParamSetterProvider) = 
            let itemSetter = provider.Setter<'Item>(name, ())
            SequenceIndexingBuilder.CreateParamSetter(itemSetter, fun command -> Seq.iteri (fun i v -> itemSetter.SetValue(v, Some i, command)))

        member __.CreateListSetter<'Item>(name: string, provider: IParamSetterProvider) = 
            let itemSetter = provider.Setter<'Item>(name, ())
            SequenceIndexingBuilder.CreateParamSetter(itemSetter, fun command -> List.iteri (fun i v -> itemSetter.SetValue(v, Some i, command)))

        member __.CreateArraySetter<'Item>(name: string, provider: IParamSetterProvider) = 
            let itemSetter = provider.Setter<'Item>(name, ())
            SequenceIndexingBuilder.CreateParamSetter(itemSetter, fun command -> Array.iteri (fun i v -> itemSetter.SetValue(v, Some i, command)))

        interface IBuilder with

            member __.CanBuild (argType: Type) = Types.isCollectionType argType 

            member this.Build<'Arg> (name: string, provider: IParamSetterProvider, _: unit) = 
                let itemType = Types.getElementType typeof<'Arg>
                let setterName = 
                    if typeof<'Arg>.IsArray then "CreateArraySetter"
                    elif typedefof<'Arg> = typedefof<list<_>> then "CreateListSetter"
                    else "CreateSeqSetter"
                let createSetterMethod = this.GetType().GetMethod(setterName).MakeGenericMethod(itemType)
                createSetterMethod.Invoke(this, [| name; provider |]) :?> IParamSetter<'Arg>
    

    type BaseSetterProvider = GenericSetters.BaseSetterProvider<unit, IDbCommand>

    type InitialDerivedSetterProvider<'Config> = GenericSetters.InitialDerivedSetterProvider<unit, IDbCommand, 'Config>

    type DerivedSetterProvider<'Config> = GenericSetters.DerivedSetterProvider<unit, IDbCommand, 'Config>

    type UnitBuilder = GenericSetters.UnitBuilder<unit, IDbCommand>

    type SequenceBuilder = GenericSetters.SequenceBuilder<unit, IDbCommand>

    type Converter<'Source, 'Target> = GenericSetters.Converter<unit, IDbCommand, 'Source, 'Target>

    type EnumConverter<'Underlying> = GenericSetters.EnumConverter<unit, IDbCommand, 'Underlying>

    type UnionBuilder = GenericSetters.UnionBuilder<unit, IDbCommand>

    type OptionBuilder = GenericSetters.OptionBuilder<unit, IDbCommand>

    type RecordBuilder = GenericSetters.RecordBuilder<unit, IDbCommand>

    type TupleBuilder = GenericSetters.TupleBuilder<unit, IDbCommand>

    type Configurator<'Config> = GenericSetters.Configurator<unit, IDbCommand, 'Config>

    let getDefaultBuilders(): IBuilder list = SimpleBuilder() :: GenericSetters.getDefaultBuilders()


/// <summary>
/// Provides methods creating various query parameter builders.
/// </summary>
type Params() = 
    inherit GenericSetters.GenericSetterBuilder<unit, IDbCommand>()

    static member Seq (itemSpecifier: ParamSpecifier<'Item>) = 
        fun (provider: IParamSetterProvider, prototype: unit) ->
            let itemSetter = itemSpecifier(provider, prototype)
            ParamsImpl.SequenceIndexingBuilder.CreateParamSetter(itemSetter, fun command -> Seq.iteri (fun i v -> itemSetter.SetValue(v, Some i, command)))

    static member Seq<'Item> (?name: string) = 
        fun (provider: IParamSetterProvider, _: unit) ->
            let itemSetter = provider.Setter<'Item>(defaultArg name "", ())
            ParamsImpl.SequenceIndexingBuilder.CreateParamSetter(itemSetter, fun command -> Seq.iteri (fun i v -> itemSetter.SetValue(v, Some i, command)))

    static member List (itemSpecifier: ParamSpecifier<'Item>) = 
        fun (provider: IParamSetterProvider, prototype: unit) ->
            let itemSetter = itemSpecifier(provider, prototype)
            ParamsImpl.SequenceIndexingBuilder.CreateParamSetter(itemSetter, fun command -> List.iteri (fun i v -> itemSetter.SetValue(v, Some i, command)))

    static member List<'Item> (?name: string) = 
        fun (provider: IParamSetterProvider, _: unit) ->
            let itemSetter = provider.Setter<'Item>(defaultArg name "", ())
            ParamsImpl.SequenceIndexingBuilder.CreateParamSetter(itemSetter, fun command -> List.iteri (fun i v -> itemSetter.SetValue(v, Some i, command)))

    static member Array (itemSpecifier: ParamSpecifier<'Item>) = 
        fun (provider: IParamSetterProvider, prototype: unit) ->
            let itemSetter = itemSpecifier(provider, prototype)
            ParamsImpl.SequenceIndexingBuilder.CreateParamSetter(itemSetter, fun command -> Array.iteri (fun i v -> itemSetter.SetValue(v, Some i, command)))

    static member Array<'Item> (?name: string) = 
        fun (provider: IParamSetterProvider, _: unit) ->
            let itemSetter = provider.Setter<'Item>(defaultArg name "", ())
            ParamsImpl.SequenceIndexingBuilder.CreateParamSetter(itemSetter, fun command -> Array.iteri (fun i v -> itemSetter.SetValue(v, Some i, command)))

/// <summary>
/// The field-to-parameter mapping override.
/// </summary>
type ParamOverride<'Arg> = GenericSetters.Override<unit, IDbCommand, 'Arg>