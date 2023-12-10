namespace DbFun.Npgsql.Builders

open System
open Npgsql
open DbFun.Core
open DbFun.Core.Builders

module BulkImportParamsImpl = 

    type IParamSetter<'Arg> = GenericSetters.ISetter<NpgsqlBinaryImporter, 'Arg>

    type IParamSetterProvider = GenericSetters.ISetterProvider<string list ref, NpgsqlBinaryImporter>

    type BuildParamSetter<'Arg> = IParamSetterProvider * string list ref -> IParamSetter<'Arg>

    type IBuilder = GenericSetters.IBuilder<string list ref, NpgsqlBinaryImporter>

    type SimpleBuilder() = 

        interface IBuilder with

            member __.CanBuild(argType: System.Type): bool = 
                Types.isSimpleType argType

            member this.Build(name: string, _: IParamSetterProvider, names: string list ref): IParamSetter<'Arg> = 
                { new IParamSetter<'Arg> with
                      member __.SetValue(value: 'Arg, importer: NpgsqlBinaryImporter): unit = 
                          importer.Write(value)
                      member __.SetNull(importer: NpgsqlBinaryImporter): unit = 
                          importer.WriteNull()
                      member __.SetArtificial(_: NpgsqlBinaryImporter): unit = 
                          names.Value <- name :: names.Value
                }

    let getDefaultBuilders(): IBuilder list = 
        SimpleBuilder() :: GenericSetters.getDefaultBuilders()


    type Converter<'Source, 'Target> = GenericSetters.Converter<string list ref, NpgsqlBinaryImporter, 'Source, 'Target>

    type SeqItemConverter<'Source, 'Target> = GenericSetters.SeqItemConverter<string list ref, NpgsqlBinaryImporter, 'Source, 'Target>

    type Configurator<'Config> = GenericSetters.Configurator<string list ref, NpgsqlBinaryImporter, 'Config>


open BulkImportParamsImpl

type BulkImportParams() = 
    inherit DbFun.Core.Builders.GenericSetters.GenericSetterBuilder<string list ref, NpgsqlBinaryImporter>()

/// <summary>
/// Bulk import cofig.
/// </summary>
type BulkImportConfig = 
    {
        ParamBuilders   : IBuilder list
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
            }

/// <summary>
/// Provides methods creating bulk import functions.
/// </summary>
type BulkImportBuilder(?config: BulkImportConfig) = 

    let builders = defaultArg (config |> Option.map (fun c -> c.ParamBuilders)) (getDefaultBuilders())

    /// <summary>
    /// Generates a function performing bulk import.
    /// </summary>
    /// <param name="tableName">
    /// The target table name.
    /// </param>
    /// <param name="setterBuilder">
    /// The parameter builder.
    /// </param>
    member __.WriteToServer<'Record>(setterBuilder: BuildParamSetter<'Record>, ?tableName: string): 'Record seq -> DbCall<unit> = 
        let fieldNames = ref List.empty<string>
        let provider = GenericSetters.BaseSetterProvider<string list ref, NpgsqlBinaryImporter>(builders)
        let setter = setterBuilder(provider, fieldNames)
        setter.SetArtificial(null)
        let copyCommand = 
            sprintf "COPY %s (%s) FROM STDIN (FORMAT BINARY)" 
                (defaultArg tableName (typeof<'Record>.Name.ToLower()))
                (fieldNames.Value |> List.rev |> String.concat ", ")
        fun (records: 'Record seq) (connector: IConnector) ->
            let npgcon = connector.Connection :?> NpgsqlConnection
            async {
                use importer = npgcon.BeginBinaryImport(copyCommand)
                for r in records do
                    importer.StartRow()
                    setter.SetValue(r, importer)
                importer.Complete() |> ignore
            }
            
    /// <summary>
    /// Generates a function performing bulk import.
    /// </summary>
    /// <param name="tableName">
    /// The target table name.
    /// </param>
    /// <param name="name">
    /// The builder name argument.
    /// </param>
    member this.WriteToServer<'Record>(?name: string, ?tableName: string): 'Record seq -> DbCall<unit> = 
        this.WriteToServer(BulkImportParams.Auto<'Record>(?name = name), ?tableName = tableName)

