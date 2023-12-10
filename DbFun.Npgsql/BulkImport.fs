namespace DbFun.Npgsql.Builders

open System
open Npgsql
open DbFun.Core
open DbFun.Core.Builders

module BulkImportParamsImpl = 

    type IParamSetter<'Arg> = GenericSetters.ISetter<NpgsqlBinaryImporter, 'Arg>

    type IParamSetterProvider = GenericSetters.ISetterProvider<unit, NpgsqlBinaryImporter>

    type BuildParamSetter<'Arg> = IParamSetterProvider * NpgsqlBinaryImporter -> IParamSetter<'Arg>

    type IBuilder = GenericSetters.IBuilder<unit, NpgsqlBinaryImporter>

    type SimpleBuilder() = 

        member __.GetArtificialValue<'Type>(): obj = 
            if typeof<'Type> = typeof<string> then box ""
            elif typeof<'Type> = typeof<DateTime> then box DateTime.Now
            elif typeof<'Type> = typeof<byte[]> then box [||]
            elif typeof<'Type>.IsClass then null
            else box Unchecked.defaultof<'Type>

        interface IBuilder with

            member __.CanBuild(argType: System.Type): bool = 
                Types.isSimpleType argType

            member this.Build(_: string, _: IParamSetterProvider, _: unit): IParamSetter<'Arg> = 
                { new IParamSetter<'Arg> with
                      member __.SetValue(value: 'Arg, importer: NpgsqlBinaryImporter): unit = 
                          importer.Write(value)
                      member __.SetNull(importer: NpgsqlBinaryImporter): unit = 
                          importer.WriteNull()
                      member __.SetArtificial(importer: NpgsqlBinaryImporter): unit = 
                          importer.Write(this.GetArtificialValue<'Arg>() :?> 'Arg)
                }

    let getDefaultBuilders(): IBuilder list = 
        SimpleBuilder() :: GenericSetters.getDefaultBuilders()


    type INameSetter<'Arg> = GenericSetters.ISetter<string list ref, 'Arg>

    type INameSetterProvider = GenericSetters.ISetterProvider<unit, string list ref>

    type BuildNameSetter<'Arg> = INameSetterProvider * string list ref -> INameSetter<'Arg>

    type INameBuilder = GenericSetters.IBuilder<unit, string list ref>

    type SimpleNameBuilder() = 

        interface INameBuilder with

            member __.CanBuild(argType: System.Type): bool = 
                Types.isSimpleType argType

            member this.Build(name: string, _: INameSetterProvider, _: unit): INameSetter<'Arg> = 
                { new INameSetter<'Arg> with
                      member __.SetValue(_: 'Arg, _: string list ref): unit = 
                          raise (NotImplementedException())
                      member __.SetNull(_: string list ref): unit = 
                          raise (NotImplementedException())
                      member __.SetArtificial(names: string list ref): unit = 
                          names.Value <- name :: names.Value
                }

    type Converter<'Source, 'Target> = GenericSetters.Converter<unit, NpgsqlBinaryImporter, 'Source, 'Target>

    type SeqItemConverter<'Source, 'Target> = GenericSetters.SeqItemConverter<unit, NpgsqlBinaryImporter, 'Source, 'Target>

    type NameConverter<'Source, 'Target> = GenericSetters.Converter<unit, string list ref, 'Source, 'Target>

    type NameSeqItemConverter<'Source, 'Target> = GenericSetters.SeqItemConverter<unit, string list ref, 'Source, 'Target>

    type Configurator<'Config> = GenericSetters.Configurator<unit, NpgsqlBinaryImporter, 'Config>

    type NameConfigurator<'Config> = GenericSetters.Configurator<unit, string list ref, 'Config>

    let getDefaultNameBuilders(): INameBuilder list = 
        SimpleNameBuilder() :: GenericSetters.getDefaultBuilders()


open BulkImportParamsImpl

/// <summary>
/// Bulk import cofig.
/// </summary>
type BulkImportConfig = 
    {
        ParamBuilders   : IBuilder list
        NameBuilders    : INameBuilder list
    }
    with
        /// <summary>
        /// Adds a converter mapping application values of a given type to ptoper database parameter values.
        /// </summary>
        /// <param name="convert">
        /// Function converting application values to database parameter values.
        /// </param>
        member this.AddConverter(convert: 'Source -> 'Target) = 
            { this with 
                ParamBuilders = 
                    BulkImportParamsImpl.Converter<'Source, 'Target>(convert) :: 
                    BulkImportParamsImpl.SeqItemConverter<'Source, 'Target>(convert) :: 
                    this.ParamBuilders 
                NameBuilders = 
                    BulkImportParamsImpl.NameConverter<'Source, 'Target>(convert) :: 
                    BulkImportParamsImpl.NameSeqItemConverter<'Source, 'Target>(convert) :: 
                    this.NameBuilders 
            }

        /// <summary>
        /// Adds a configurator for parameter builders of types determined by canBuild function.
        /// </summary>
        /// <param name="getConfig">
        /// Creates a configuration object.
        /// </param>
        /// <param name="canBuild">
        /// Function determining whether a given type is handled by the configurator.
        /// </param>
        member this.AddConfigurator(getConfig: string -> 'Config, canBuild: Type -> bool) = 
            { this with 
                ParamBuilders = BulkImportParamsImpl.Configurator<'Config>(getConfig, canBuild) :: this.ParamBuilders 
                NameBuilders = BulkImportParamsImpl.NameConfigurator<'Config>(getConfig, canBuild) :: this.NameBuilders 
            }

/// <summary>
/// Provides methods creating bulk import functions.
/// </summary>
type BulkImportBuilder(?config: BulkImportConfig) = 

    let builders = defaultArg (config |> Option.map (fun c -> c.ParamBuilders)) (getDefaultBuilders())
    let nameBuilders = defaultArg (config |> Option.map (fun c -> c.NameBuilders)) (getDefaultNameBuilders())

    /// <summary>
    /// Generates a function performing bulk import.
    /// </summary>
    /// <param name="name">
    /// Target table name.
    /// </param>
    member __.WriteToServer<'Record>(?name: string): 'Record seq -> DbCall<unit> = 
        let nameProvider = GenericSetters.BaseSetterProvider<unit, string list ref>(nameBuilders)
        let nameSetter = nameProvider.GetSetter<'Record>("", ())
        let fieldNames = ref List.empty<string>
        nameSetter.SetArtificial fieldNames
        let copyCommand = 
            sprintf "COPY %s (%s) FROM STDIN (FORMAT BINARY)" 
                (defaultArg name (typeof<'Record>.Name.ToLower()))
                (fieldNames.Value |> List.rev |> String.concat ", ")
        let provider = GenericSetters.BaseSetterProvider<unit, NpgsqlBinaryImporter>(builders)
        let setter = provider.GetSetter<'Record>("", ())
        fun (records: 'Record seq) (connector: IConnector) ->
            let npgcon = connector.Connection :?> NpgsqlConnection
            async {
                use importer = npgcon.BeginBinaryImport(copyCommand)
                for r in records do
                    importer.StartRow()
                    setter.SetValue(r, importer)
                importer.Complete() |> ignore
            }
            


