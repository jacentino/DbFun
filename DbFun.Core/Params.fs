namespace DbFun.Core.Builders

open System
open System.Data 
open DbFun.Core

type IParamSetter<'Arg> = GenericSetters.ISetter<IDbCommand, 'Arg>

type IParamSetterProvider = GenericSetters.ISetterProvider<IDbConnection, IDbCommand>

type ParamSpecifier<'Arg> = GenericSetters.SetterSpecifier<IDbConnection, IDbCommand, 'Arg>

module ParamsImpl = 

    type IBuilder = GenericSetters.IBuilder<IDbConnection, IDbCommand>

    type SimpleBuilder() =

        member __.FindOrCreateParam(command: IDbCommand, name: string, itemIndex: int option) = 
            let name' = name + (itemIndex |> Option.map string |> Option.defaultValue "")
            let index = command.Parameters.IndexOf(name')
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

            member this.Build<'Arg> (name: string, _, _) = 
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

        member __.CreateSeqSetter<'Item>(name: string, provider: IParamSetterProvider, connection: IDbConnection) = 
            let itemSetter = provider.Setter<'Item>(name, connection)
            SequenceIndexingBuilder.CreateParamSetter(itemSetter, fun command -> Seq.iteri (fun i v -> itemSetter.SetValue(v, Some i, command)))

        member __.CreateListSetter<'Item>(name: string, provider: IParamSetterProvider, connection: IDbConnection) = 
            let itemSetter = provider.Setter<'Item>(name, connection)
            SequenceIndexingBuilder.CreateParamSetter(itemSetter, fun command -> List.iteri (fun i v -> itemSetter.SetValue(v, Some i, command)))

        member __.CreateArraySetter<'Item>(name: string, provider: IParamSetterProvider, connection: IDbConnection) = 
            let itemSetter = provider.Setter<'Item>(name, connection)
            SequenceIndexingBuilder.CreateParamSetter(itemSetter, fun command -> Array.iteri (fun i v -> itemSetter.SetValue(v, Some i, command)))

        interface IBuilder with

            member __.CanBuild (argType: Type) = Types.isCollectionType argType 

            member this.Build<'Arg> (name: string, provider: IParamSetterProvider, connection: IDbConnection) = 
                let itemType = Types.getElementType typeof<'Arg>
                let setterName = 
                    if typeof<'Arg>.IsArray then "CreateArraySetter"
                    elif typedefof<'Arg> = typedefof<list<_>> then "CreateListSetter"
                    else "CreateSeqSetter"
                let createSetterMethod = this.GetType().GetMethod(setterName).MakeGenericMethod(itemType)
                createSetterMethod.Invoke(this, [| name; provider; connection |]) :?> IParamSetter<'Arg>
    

    type BaseSetterProvider = GenericSetters.BaseSetterProvider<IDbConnection, IDbCommand>

    type InitialDerivedSetterProvider<'Config> = GenericSetters.InitialDerivedSetterProvider<IDbConnection, IDbCommand, 'Config>

    type DerivedSetterProvider<'Config> = GenericSetters.DerivedSetterProvider<IDbConnection, IDbCommand, 'Config>

    type UnitBuilder = GenericSetters.UnitBuilder<IDbConnection, IDbCommand>

    type SequenceBuilder = GenericSetters.SequenceBuilder<IDbConnection, IDbCommand>

    type Converter<'Source, 'Target> = GenericSetters.Converter<IDbConnection, IDbCommand, 'Source, 'Target>

    type EnumConverter<'Underlying> = GenericSetters.EnumConverter<IDbConnection, IDbCommand, 'Underlying>

    type UnionBuilder = GenericSetters.UnionBuilder<unit, IDbCommand>

    type OptionBuilder = GenericSetters.OptionBuilder<IDbConnection, IDbCommand>

    type RecordBuilder = GenericSetters.RecordBuilder<IDbConnection, IDbCommand>

    type TupleBuilder = GenericSetters.TupleBuilder<IDbConnection, IDbCommand>

    type Configurator<'Config> = GenericSetters.Configurator<IDbConnection, IDbCommand, 'Config>

    let getDefaultBuilders(): IBuilder list = SimpleBuilder() :: GenericSetters.getDefaultBuilders()


/// <summary>
/// Provides methods creating various query parameter builders.
/// </summary>
type Params() = 
    inherit GenericSetters.GenericSetterBuilder<IDbConnection, IDbCommand>()

    /// <summary>
    /// Creates a builder handling sequence parameters. The builder creates multiple command parameters with names supplemented with item index.
    /// </summary>
    /// <param name="itemSpecifier">
    /// The sequence item type specifier.
    /// </param>
    static member Seq (itemSpecifier: ParamSpecifier<'Item>) = 
        fun (provider: IParamSetterProvider, prototype: IDbConnection) ->
            let itemSetter = itemSpecifier(provider, prototype)
            ParamsImpl.SequenceIndexingBuilder.CreateParamSetter(itemSetter, fun command -> Seq.iteri (fun i v -> itemSetter.SetValue(v, Some i, command)))

    /// <summary>
    /// Creates a builder handling sequence parameters. The builder creates multiple command parameters with names supplemented with item index.
    /// </summary>
    /// <param name="name">
    /// The sequence item base name.
    /// </param>
    static member Seq<'Item> (?name: string) = 
        fun (provider: IParamSetterProvider, connection: IDbConnection) ->
            let itemSetter = provider.Setter<'Item>(defaultArg name "", connection)
            ParamsImpl.SequenceIndexingBuilder.CreateParamSetter(itemSetter, fun command -> Seq.iteri (fun i v -> itemSetter.SetValue(v, Some i, command)))

    /// <summary>
    /// Creates a builder handling list parameters. The builder creates multiple command parameters with names supplemented with item index.
    /// </summary>
    /// <param name="itemSpecifier">
    /// The list item type specifier.
    /// </param>
    static member List (itemSpecifier: ParamSpecifier<'Item>) = 
        fun (provider: IParamSetterProvider, prototype: IDbConnection) ->
            let itemSetter = itemSpecifier(provider, prototype)
            ParamsImpl.SequenceIndexingBuilder.CreateParamSetter(itemSetter, fun command -> List.iteri (fun i v -> itemSetter.SetValue(v, Some i, command)))

    /// <summary>
    /// Creates a builder handling sequence parameters. The builder creates multiple command parameters with names supplemented with item index.
    /// </summary>
    /// <param name="name">
    /// The list item name.
    /// </param>
    static member List<'Item> (?name: string) = 
        fun (provider: IParamSetterProvider, prototype: IDbConnection) ->
            let itemSetter = provider.Setter<'Item>(defaultArg name "", prototype)
            ParamsImpl.SequenceIndexingBuilder.CreateParamSetter(itemSetter, fun command -> List.iteri (fun i v -> itemSetter.SetValue(v, Some i, command)))

    /// <summary>
    /// Creates a builder handling array parameters. The builder creates multiple command parameters with names supplemented with item index.
    /// </summary>
    /// <param name="itemSpecifier">
    /// The array item type specifier.
    /// </param>
    static member Array (itemSpecifier: ParamSpecifier<'Item>) = 
        fun (provider: IParamSetterProvider, prototype: IDbConnection) ->
            let itemSetter = itemSpecifier(provider, prototype)
            ParamsImpl.SequenceIndexingBuilder.CreateParamSetter(itemSetter, fun command -> Array.iteri (fun i v -> itemSetter.SetValue(v, Some i, command)))

    /// <summary>
    /// Creates a builder handling array parameters. The builder creates multiple command parameters with names supplemented with item index.
    /// </summary>
    /// <param name="name">
    /// The array item name.
    /// </param>
    static member Array<'Item> (?name: string) = 
        fun (provider: IParamSetterProvider, prototype: IDbConnection) ->
            let itemSetter = provider.Setter<'Item>(defaultArg name "", prototype)
            ParamsImpl.SequenceIndexingBuilder.CreateParamSetter(itemSetter, fun command -> Array.iteri (fun i v -> itemSetter.SetValue(v, Some i, command)))

/// <summary>
/// The field-to-parameter mapping override.
/// </summary>
type ParamOverride<'Arg> = GenericSetters.Override<IDbConnection, IDbCommand, 'Arg>