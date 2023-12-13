namespace DbFun.Core.Builders

open System.Data
open DbFun.Core
open DbFun.Core.Diagnostics
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open System

/// <summary>
/// The query builder configuration data.
/// </summary>
type QueryConfig = 
    {
        CreateConnection    : unit -> IDbConnection
        ParamBuilders       : ParamsImpl.IBuilder list
        OutParamBuilders    : OutParamsImpl.IBuilder list
        RowBuilders         : RowsImpl.IBuilder list
        Timeout             : int option
        LogCompileTimeErrors: bool
        PrototypeCalls      : bool
    }
    with 
        /// <summary>
        /// Creates default configuration.
        /// </summary>
        /// <param name="createConnection">
        /// The function creating database connection (with proper connection string, but not open).
        /// </param>
        static member Default(createConnection): QueryConfig = 
            {
                CreateConnection    = createConnection
                ParamBuilders       = ParamsImpl.getDefaultBuilders()
                OutParamBuilders    = OutParamsImpl.getDefaultBuilders()
                RowBuilders         = RowsImpl.getDefaultBuilders()
                Timeout             = None
                LogCompileTimeErrors= false
                PrototypeCalls      = true
            }

        /// <summary>
        /// Adds a converter mapping application values of a given type to ptoper database parameter values.
        /// </summary>
        /// <param name="convert">
        /// Function converting application values to database parameter values.
        /// </param>
        member this.AddParamConverter(convert: 'Source -> 'Target) = 
            { this with ParamBuilders = 
                            ParamsImpl.Converter<'Source, 'Target>(convert) :: 
                            ParamsImpl.SeqItemConverter<'Source, 'Target>(convert) :: 
                            this.ParamBuilders 
            }

        /// <summary>
        /// Adds a converter mapping database values to application values.
        /// </summary>
        /// <param name="convert">
        /// Function converting database column values to application values.
        /// </param>
        member this.AddRowConverter(convert: 'Source -> 'Target) = 
            { this with 
                RowBuilders = RowsImpl.Converter<'Source, 'Target>(convert) :: this.RowBuilders
                OutParamBuilders = OutParamsImpl.Converter<'Source, 'Target>(convert) ::  this.OutParamBuilders
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
        member this.AddParamConfigurator(getConfig: string -> 'Config, canBuild: Type -> bool) = 
            { this with ParamBuilders = ParamsImpl.Configurator<'Config>(getConfig, canBuild) :: this.ParamBuilders }

        /// <summary>
        /// Adds a configurator for row builders of types determined by canBuild function.
        /// </summary>
        /// <param name="getConfig">
        /// Creates a configuration object.
        /// </param>
        /// <param name="canBuild">
        /// Function determining whether a given type is handled by the configurator.
        /// </param>
        member this.AddRowConfigurator(getConfig: string -> 'Config, canBuild: Type -> bool) = 
            { this with 
                RowBuilders = RowsImpl.Configurator<'Config>(getConfig, canBuild) :: this.RowBuilders 
                OutParamBuilders = OutParamsImpl.Configurator<'Config>(getConfig, canBuild) :: this.OutParamBuilders
            }

        /// <summary>
        /// Adds a configurator for both parameter and row builders of types determined by canBuild function.
        /// </summary>
        /// <param name="getConfig">
        /// Creates a configuration object.
        /// </param>
        /// <param name="canBuild">
        /// Function determining whether a given type is handled by the configurator.
        /// </param>
        member this.AddConfigurator(getConfig: string -> 'Config, canBuild: Type -> bool) = 
            this.AddParamConfigurator(getConfig, canBuild)
                .AddRowConfigurator(getConfig, canBuild)

        /// <summary>
        /// Allows to generate functions executing queries without discovering resultset structure using SchemaOnly calls.
        /// </summary>
        member this.DisablePrototypeCalls() = 
            { this with 
                PrototypeCalls = false
                RowBuilders = RowsImpl.NoPrototypeColumnBuilder() :: this.RowBuilders
            }

/// <summary>
/// Provides methods creating various query functions.
/// </summary>
type QueryBuilder(config: QueryConfig, ?compileTimeErrorLog: ref<CompileTimeErrorLog>) =

    let compileTimeErrorLog = 
        if config.LogCompileTimeErrors then 
            compileTimeErrorLog |> Option.orElse (Some (ref<CompileTimeErrorLog> []))
        else 
            None

    let executePrototypeQuery(commandType: CommandType, commandText: string, setParams: IDbCommand -> unit, resultReaderBuilder: IDataReader -> IResultReader<'Result>) =
        if config.PrototypeCalls then
            use connection = config.CreateConnection()
            connection.Open()
            use transaction = connection.BeginTransaction()
            use command = connection.CreateCommand()
            command.CommandType <- commandType
            command.CommandText <- commandText
            command.Transaction <- transaction
            setParams(command)
            use prototype = command.ExecuteReader(CommandBehavior.SchemaOnly)
            resultReaderBuilder(prototype)
        else
            resultReaderBuilder(null)

    let executeQuery (provider: IConnector, commandText: string, resultReader: IResultReader<'Result>, setParams: IDbCommand -> unit) = 
        async {
            use command = provider.Connection.CreateCommand()
            command.CommandType <- CommandType.Text
            command.CommandText <- commandText
            command.Transaction <- provider.Transaction
            match config.Timeout with
            | Some timeout -> command.CommandTimeout <- timeout
            | None -> ()
            setParams(command)
            use! dataReader = Executor.executeReaderAsync(command, CommandBehavior.Default)
            return! resultReader.Read(dataReader)
        }

    let executeProcedure (provider: IConnector, commandText: string, outParamGetter: IOutParamGetter<'OutParams>, resultReader: IResultReader<'Result>, setParams: IDbCommand -> unit) = 
        async {
            use command = provider.Connection.CreateCommand()
            command.CommandType <- CommandType.StoredProcedure
            command.CommandText <- commandText
            command.Transaction <- provider.Transaction
            match config.Timeout with
            | Some timeout -> command.CommandTimeout <- timeout
            | None -> ()
            setParams(command)
            outParamGetter.Create(command)
            use! dataReader = Executor.executeReaderAsync(command, CommandBehavior.Default) 
            let! result = resultReader.Read(dataReader)
            return result, outParamGetter.Get(command)
        }

    let handleException (sourcePath: string, sourceLine: int, ex: exn) = 
        match compileTimeErrorLog with
        | Some errorLog -> 
            errorLog.Value <- (sourceLine, sourcePath, ex) :: errorLog.Value
            fun _ -> 
                raise <| AggregateException("One or more exceptions occured when compiling queries.", 
                            errorLog.Value 
                            |> List.rev
                            |> List.map (fun (line, source, ex) -> CompileTimeException($"Cannot compile query in {source}, line: {line}", ex) :> exn))
        | None ->
            raise <| CompileTimeException($"Cannot compile query in {sourcePath}, line: {sourceLine}", ex)

    /// <summary>
    /// Creates query builder object with default configuration
    /// </summary>
    /// <param name="createConnection">
    /// Function creating connection, assigned with a proper connection string, but not open.
    /// </param>
    new(createConnection: unit -> IDbConnection) = 
        QueryBuilder(QueryConfig.Default(createConnection))

    /// <summary>
    /// The configuration of the query builder.
    /// </summary>
    member __.Config = config

    /// <summary>
    /// Creates new builder with the specified command timeout.
    /// </summary>
    /// <param name="timeout">
    /// The timeout value in seconds.
    /// </param>
    member __.Timeout(timeout: int) = 
        QueryBuilder({ config with Timeout = Some timeout }, ?compileTimeErrorLog = compileTimeErrorLog)

    /// <summary>
    /// Creates new builder with compile-time error logging and deferred exceptions.
    /// </summary>
    member __.LogCompileTimeErrors() = 
        QueryBuilder({ config with LogCompileTimeErrors = true }, ?compileTimeErrorLog = compileTimeErrorLog)

    /// <summary>
    /// Creates new builder generating query functions without discovering resultset structure using SchemaOnly calls.
    /// </summary>
    member __.DisablePrototypeCalls() = 
        QueryBuilder(config.DisablePrototypeCalls(), ?compileTimeErrorLog = compileTimeErrorLog)

    /// <summary>
    /// The list of compile time errors.
    /// </summary>
    member __.CompileTimeErrors = 
        match compileTimeErrorLog with
        | Some log -> log.Value |> List.rev
        | None -> []

    member __.RawCompileTimeErrorLog = compileTimeErrorLog 

    /// <summary>
    /// Builds a one arg query function based on command template.
    /// </summary>
    /// <param name="createParamSetter">
    /// The parameter builder.
    /// </param>
    /// <param name="createResultReader">
    /// The result reader builder.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="createResultReader">
    /// The result builder.
    /// </param>
    /// <param name="template">
    /// The template expansion function.
    /// </param>
    member __.TemplatedSql (
            template: 'Params option -> string,
            createParamSetter: BuildParamSetter<'Params>, 
            createResultReader: BuildResultReader<'Result>,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params -> DbCall<'Result> =         
        try
            let provider = GenericSetters.BaseSetterProvider<unit, IDbCommand>(config.ParamBuilders)
            let paramSetter = createParamSetter(provider, ())

            let rowGetterProvider = GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>(config.RowBuilders)
                       
            let createResultReader' prototype = createResultReader(rowGetterProvider, prototype)

            let resultReader = executePrototypeQuery(CommandType.Text, template(None), paramSetter.SetArtificial, createResultReader')

            fun (parameters: 'Params) (provider: IConnector) ->
                executeQuery(provider, template(Some parameters), resultReader, fun cmd -> paramSetter.SetValue(parameters, cmd))
        with ex ->
            handleException(sourcePath, sourceLine, ex)

    /// <summary>
    /// Builds a one arg query function based on command template.
    /// </summary>
    /// <param name="name">
    /// The parameter name.
    /// </param>
    /// <param name="createResultReader">
    /// The result reader builder.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="createResultReader">
    /// The result builder.
    /// </param>
    /// <param name="template">
    /// The template expansion function.
    /// </param>
    member this.TemplatedSql (
            template: 'Params option -> string,
            name: string, 
            createResultReader: BuildResultReader<'Result>,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params -> DbCall<'Result> =         
        this.TemplatedSql(template, Params.Auto<'Params>(name), createResultReader)

    /// <summary>
    /// Builds a one arg query function based on command template.
    /// </summary>
    /// <param name="name">
    /// The parameter name.
    /// </param>
    /// <param name="createResultReader">
    /// The result reader builder.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="createResultReader">
    /// The result builder.
    /// </param>
    /// <param name="template">
    /// The template expansion function.
    /// </param>
    member this.TemplatedSql (
            template: 'Params option -> string,
            [<Optional>] name: string, 
            [<Optional>] resultName: string,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params -> DbCall<'Result> =         
        this.TemplatedSql(template, Params.Auto<'Params>(name), Results.Auto(resultName))

    /// <summary>
    /// Builds a one arg query function based on raw SQL text.
    /// </summary>
    /// <param name="createParamSetter">
    /// The parameter builder.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="createResultReader">
    /// The result builder.
    /// </param>
    /// <param name="commandText">
    /// The SQL command text.
    /// </param>
    member this.Sql(
            commandText: string,
            createParamSetter: BuildParamSetter<'Params>, 
            createResultReader: BuildResultReader<'Result>,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params -> DbCall<'Result> =         
            this.TemplatedSql((fun _ -> commandText), createParamSetter, createResultReader, sourcePath, sourceLine) 

    /// <summary>
    /// Builds a one arg query function based on raw SQL text.
    /// </summary>
    /// <param name="name">
    /// The parameter name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="createResultReader">
    /// The result builder.
    /// </param>
    /// <param name="commandText">
    /// The SQL command text.
    /// </param>
    member this.Sql<'Params, 'Result> (
            commandText: string,
            name: string, createResultBuilder: BuildResultReader<'Result>,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params -> DbCall<'Result> =         
        this.Sql(commandText, Params.Auto<'Params>(name), createResultBuilder, sourcePath, sourceLine) 

    /// <summary>
    /// Builds a one arg query function based on raw SQL text.
    /// </summary>
    /// <param name="argName">
    /// The parameter name.
    /// </param>
    /// </summary>
    /// <param name="resultName">
    /// The result column name.
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="commandText">
    /// The SQL command text.
    /// </param>
    member this.Sql<'Params, 'Result> (
            commandText: string,
            [<Optional>] argName: string, [<Optional>] resultName: string,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params -> DbCall<'Result> =         
        this.Sql<'Params, 'Result>(commandText, argName, Results.Auto<'Result>(resultName), sourcePath, sourceLine) 

    /// <summary>
    /// Builds a query function with two curried args based on raw SQL text.
    /// </summary>
    /// <param name="createParamSetter1">
    /// The first parameter builder.
    /// </param>
    /// <param name="createParamSetter2">
    /// The second parameter builder.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="createResultReader">
    /// The result builder.
    /// </param>
    /// <param name="commandText">
    /// The SQL command text.
    /// </param>
    member __.Sql (
        commandText: string,
        createParamSetter1: BuildParamSetter<'Params1>, 
        createParamSetter2: BuildParamSetter<'Params2>,
        createResultReader: BuildResultReader<'Result>,
        [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
        [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
        : 'Params1 -> 'Params2 -> DbCall<'Result> = 
            try                        
                let provider = GenericSetters.BaseSetterProvider<unit, IDbCommand>(config.ParamBuilders)
                let paramSetter1 = createParamSetter1(provider, ())
                let paramSetter2 = createParamSetter2(provider, ())

                let rowGetterProvider = GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>(config.RowBuilders)
                let createResultReader' prototype = createResultReader(rowGetterProvider, prototype)

                let setArtificial(command: IDbCommand) = 
                    paramSetter1.SetArtificial(command)
                    paramSetter2.SetArtificial(command)

                let resultReader = executePrototypeQuery(CommandType.Text, commandText, setArtificial, createResultReader')

                let setParams (parameters1: 'Params1, parameters2: 'Params2) (command: IDbCommand) = 
                    paramSetter1.SetValue(parameters1, command) 
                    paramSetter2.SetValue(parameters2, command)

                fun (parameters1: 'Params1) (parameters2: 'Params2) (provider: IConnector) ->
                    executeQuery(provider, commandText, resultReader, setParams(parameters1, parameters2))
            with ex ->
                handleException(sourcePath, sourceLine, ex)

    /// <summary>
    /// Builds a query function with two curried args based on raw SQL text.
    /// </summary>
    /// <param name="name1">
    /// The first parameter name.
    /// </param>
    /// <param name="name2">
    /// The second parameter name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="createResultReader">
    /// The result builder.
    /// </param>
    /// <param name="commandText">
    /// The SQL command text.
    /// </param>
    member this.Sql<'Params1, 'Params2, 'Result> (
            commandText: string,
            name1: string, name2: string, 
            createResultReader: BuildResultReader<'Result>,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params1 -> 'Params2 -> DbCall<'Result> =         
        this.Sql(commandText, Params.Auto<'Params1>(name1), Params.Auto<'Params2>(name2), createResultReader, sourcePath, sourceLine)

    /// <summary>
    /// Builds a query function with three curried args based on raw SQL text.
    /// </summary>
    /// <param name="createParamSetter1">
    /// The first parameter builder.
    /// </param>
    /// <param name="createParamSetter2">
    /// The second parameter builder.
    /// </param>
    /// <param name="createParamSetter3">
    /// The third parameter builder.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="createResultReader">
    /// The result builder.
    /// </param>
    /// <param name="commandText">
    /// The SQL command text.
    /// </param>
    member __.Sql (createParamSetter1: BuildParamSetter<'Params1>, 
                   createParamSetter2: BuildParamSetter<'Params2>,
                   createParamSetter3: BuildParamSetter<'Params3>,
                   createResultReader: BuildResultReader<'Result>,
                   [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
                   [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
                   : string -> 'Params1 -> 'Params2 -> 'Params3 -> DbCall<'Result> = 
        fun (commandText: string) ->
            try                        
                let provider = GenericSetters.BaseSetterProvider<unit, IDbCommand>(config.ParamBuilders)
                let paramSetter1 = createParamSetter1(provider, ())
                let paramSetter2 = createParamSetter2(provider, ())
                let paramSetter3 = createParamSetter3(provider, ())

                let rowGetterProvider = GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>(config.RowBuilders)
                let createResultReader' prototype = createResultReader(rowGetterProvider, prototype)

                let setArtificial(command: IDbCommand) = 
                    paramSetter1.SetArtificial(command)
                    paramSetter2.SetArtificial(command)
                    paramSetter3.SetArtificial(command)                    

                let resultReader = executePrototypeQuery(CommandType.Text, commandText, setArtificial, createResultReader')

                let setParams (parameters1: 'Params1, parameters2: 'Params2, parameters3: 'Params3) (command: IDbCommand) = 
                    paramSetter1.SetValue(parameters1, command) 
                    paramSetter2.SetValue(parameters2, command)
                    paramSetter3.SetValue(parameters3, command)

                fun (parameters1: 'Params1) (parameters2: 'Params2) (parameters3: 'Params3) (provider: IConnector) ->
                    executeQuery(provider, commandText, resultReader, setParams(parameters1, parameters2, parameters3))
            with ex ->
                handleException(sourcePath, sourceLine, ex)

    /// <summary>
    /// Builds a query function with three curried args based on raw SQL text.
    /// </summary>
    /// <param name="name1">
    /// The first parameter name.
    /// </param>
    /// <param name="name2">
    /// The second parameter name.
    /// </param>
    /// <param name="name3">
    /// The third parameter name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="createResultReader">
    /// The result builder.
    /// </param>
    /// <param name="commandText">
    /// The SQL command text.
    /// </param>
    member this.Sql<'Params1, 'Params2, 'Params3, 'Result> (
            name1: string, name2: string, name3: string, 
            createResultReader: BuildResultReader<'Result>,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : string -> 'Params1 -> 'Params2 -> 'Params3 -> DbCall<'Result> =         
        this.Sql(Params.Auto<'Params1>(name1), Params.Auto<'Params2>(name2), Params.Auto<'Params3>(name3), createResultReader, sourcePath, sourceLine)

    /// <summary>
    /// Builds a query function with four curried args based on raw SQL text.
    /// </summary>
    /// <param name="createParamSetter1">
    /// The first parameter builder.
    /// </param>
    /// <param name="createParamSetter2">
    /// The second parameter builder.
    /// </param>
    /// <param name="createParamSetter3">
    /// The third parameter builder.
    /// </param>
    /// <param name="createParamSetter4">
    /// The fourth parameter builder.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="createResultReader">
    /// The result builder.
    /// </param>
    /// <param name="commandText">
    /// The SQL command text.
    /// </param>
    member __.Sql (createParamSetter1: BuildParamSetter<'Params1>, 
                   createParamSetter2: BuildParamSetter<'Params2>,
                   createParamSetter3: BuildParamSetter<'Params3>,
                   createParamSetter4: BuildParamSetter<'Params4>,
                   createResultReader: BuildResultReader<'Result>,
                   [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
                   [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
                   : string -> 'Params1 -> 'Params2 -> 'Params3 -> 'Params4 -> DbCall<'Result> = 
        fun (commandText: string) ->
            try                        
                let provider = GenericSetters.BaseSetterProvider<unit, IDbCommand>(config.ParamBuilders)
                let paramSetter1 = createParamSetter1(provider, ())
                let paramSetter2 = createParamSetter2(provider, ())
                let paramSetter3 = createParamSetter3(provider, ())
                let paramSetter4 = createParamSetter4(provider, ())

                let rowGetterProvider = GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>(config.RowBuilders)
                let createResultReader' prototype = createResultReader(rowGetterProvider, prototype)

                let setArtificial(command: IDbCommand) = 
                    paramSetter1.SetArtificial(command)
                    paramSetter2.SetArtificial(command)
                    paramSetter3.SetArtificial(command)                    
                    paramSetter4.SetArtificial(command)                    

                let resultReader = executePrototypeQuery(CommandType.Text, commandText, setArtificial, createResultReader')

                let setParams (parameters1: 'Params1, parameters2: 'Params2, parameters3: 'Params3, parameters4: 'Params4) (command: IDbCommand) = 
                    paramSetter1.SetValue(parameters1, command) 
                    paramSetter2.SetValue(parameters2, command)
                    paramSetter3.SetValue(parameters3, command)
                    paramSetter4.SetValue(parameters4, command)

                fun (parameters1: 'Params1) (parameters2: 'Params2) (parameters3: 'Params3) (parameters4: 'Params4) (provider: IConnector) ->
                    executeQuery(provider, commandText, resultReader, setParams(parameters1, parameters2, parameters3, parameters4))
            with ex ->
                handleException(sourcePath, sourceLine, ex)

    /// <summary>
    /// Builds a query function with four curried args based on raw SQL text.
    /// </summary>
    /// <param name="name1">
    /// The first parameter name.
    /// </param>
    /// <param name="name2">
    /// The second parameter name.
    /// </param>
    /// <param name="name3">
    /// The third parameter name.
    /// </param>
    /// <param name="name4">
    /// The fourth parameter name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="createResultReader">
    /// The result builder.
    /// </param>
    /// <param name="commandText">
    /// The SQL command text.
    /// </param>
    member this.Sql<'Params1, 'Params2, 'Params3, 'Params4, 'Result> (
            name1: string, name2: string, name3: string, name4: string, 
            createResultReader: BuildResultReader<'Result>,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : string -> 'Params1 -> 'Params2 -> 'Params3 -> 'Params4 -> DbCall<'Result> =         
        this.Sql(Params.Auto<'Params1>(name1), Params.Auto<'Params2>(name2), 
                 Params.Auto<'Params3>(name3), Params.Auto<'Params4>(name4), 
                 createResultReader,
                 sourcePath, sourceLine)

    /// <summary>
    /// Builds a query function with five curried args based on raw SQL text.
    /// </summary>
    /// <param name="createParamSetter1">
    /// The first parameter builder.
    /// </param>
    /// <param name="createParamSetter2">
    /// The second parameter builder.
    /// </param>
    /// <param name="createParamSetter3">
    /// The third parameter builder.
    /// </param>
    /// <param name="createParamSetter4">
    /// The fourth parameter builder.
    /// </param>
    /// <param name="createParamSetter5">
    /// The fifth parameter builder.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="createResultReader">
    /// The result builder.
    /// </param>
    /// <param name="commandText">
    /// The SQL command text.
    /// </param>
    member __.Sql (createParamSetter1: BuildParamSetter<'Params1>, 
                   createParamSetter2: BuildParamSetter<'Params2>,
                   createParamSetter3: BuildParamSetter<'Params3>,
                   createParamSetter4: BuildParamSetter<'Params4>,
                   createParamSetter5: BuildParamSetter<'Params5>,
                   createResultReader: BuildResultReader<'Result>,
                   [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
                   [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
                   : string -> 'Params1 -> 'Params2 -> 'Params3 -> 'Params4 -> 'Params5 -> DbCall<'Result> = 
        fun (commandText: string) ->
            try                        
                let provider = GenericSetters.BaseSetterProvider<unit, IDbCommand>(config.ParamBuilders)
                let paramSetter1 = createParamSetter1(provider, ())
                let paramSetter2 = createParamSetter2(provider, ())
                let paramSetter3 = createParamSetter3(provider, ())
                let paramSetter4 = createParamSetter4(provider, ())
                let paramSetter5 = createParamSetter5(provider, ())

                let rowGetterProvider = GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>(config.RowBuilders)
                let createResultReader' prototype = createResultReader(rowGetterProvider, prototype)

                let setArtificial(command: IDbCommand) = 
                    paramSetter1.SetArtificial(command)
                    paramSetter2.SetArtificial(command)
                    paramSetter3.SetArtificial(command)                    
                    paramSetter4.SetArtificial(command)                    
                    paramSetter5.SetArtificial(command)                    

                let resultReader = executePrototypeQuery(CommandType.Text, commandText, setArtificial, createResultReader')

                let setParams (parameters1: 'Params1, parameters2: 'Params2, parameters3: 'Params3, parameters4: 'Params4, parameters5: 'Params5) (command: IDbCommand) = 
                    paramSetter1.SetValue(parameters1, command) 
                    paramSetter2.SetValue(parameters2, command)
                    paramSetter3.SetValue(parameters3, command)
                    paramSetter4.SetValue(parameters4, command)
                    paramSetter5.SetValue(parameters5, command)

                fun (parameters1: 'Params1) (parameters2: 'Params2) (parameters3: 'Params3) (parameters4: 'Params4) (parameters5: 'Params5) (provider: IConnector) ->
                    executeQuery(provider, commandText, resultReader, setParams(parameters1, parameters2, parameters3, parameters4, parameters5))
            with ex ->
                handleException(sourcePath, sourceLine, ex)

    /// <summary>
    /// Builds a query function with five curried args based on raw SQL text.
    /// </summary>
    /// <param name="name1">
    /// The first parameter name.
    /// </param>
    /// <param name="name2">
    /// The second parameter name.
    /// </param>
    /// <param name="name3">
    /// The third parameter name.
    /// </param>
    /// <param name="name4">
    /// The fourth parameter name.
    /// </param>
    /// <param name="name5">
    /// The fifth parameter name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="createResultReader">
    /// The result builder.
    /// </param>
    /// <param name="commandText">
    /// The SQL command text.
    /// </param>
    member this.Sql<'Params1, 'Params2, 'Params3, 'Params4, 'Params5, 'Result> (
            name1: string, name2: string, name3: string, name4: string, name5: string, 
            createResultReader: BuildResultReader<'Result>,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : string -> 'Params1 -> 'Params2 -> 'Params3 -> 'Params4 -> 'Params5 -> DbCall<'Result> =         
        this.Sql(Params.Auto<'Params1>(name1), Params.Auto<'Params2>(name2), 
                 Params.Auto<'Params3>(name3), Params.Auto<'Params4>(name4), 
                 Params.Auto<'Params5>(name5), 
                 createResultReader,
                 sourcePath, sourceLine)

    /// <summary>
    /// Builds a query function with two curried args based on raw SQL text.
    /// </summary>
    /// <param name="argName1">
    /// The first parameter name.
    /// </param>
    /// <param name="argName2">
    /// The second parameter name.
    /// </param>
    /// </summary>
    /// <param name="resultName">
    /// The result column name.
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="commandText">
    /// The SQL command text.
    /// </param>
    member this.Sql<'Params1, 'Params2, 'Result> (
            commandText: string,
            [<Optional>] argName1: string, [<Optional>] argName2: string, [<Optional>] resultName: string,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params1 -> 'Params2 -> DbCall<'Result> =         
        this.Sql(commandText, Params.Auto<'Params1>(argName1), Params.Auto<'Params2>(argName2), Results.Auto<'Result>(resultName), sourcePath, sourceLine) 

    /// <summary>
    /// Builds a query function with three curried args based on raw SQL text.
    /// </summary>
    /// <param name="argName1">
    /// The first parameter name.
    /// </param>
    /// <param name="argName2">
    /// The second parameter name.
    /// </param>
    /// <param name="argName3">
    /// The third parameter name.
    /// </param>
    /// </summary>
    /// <param name="resultName">
    /// The result column name.
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="commandText">
    /// The SQL command text.
    /// </param>
    member this.Sql<'Params1, 'Params2, 'Params3, 'Result> (
            [<Optional>] argName1: string, [<Optional>] argName2: string, [<Optional>] argName3: string, [<Optional>] resultName: string,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : string -> 'Params1 -> 'Params2 -> 'Params3 -> DbCall<'Result> =         
        this.Sql(Params.Auto<'Params1>(argName1), Params.Auto<'Params2>(argName2), Params.Auto<'Params3>(argName3), Results.Auto<'Result>(resultName), sourcePath, sourceLine) 

    /// <summary>
    /// Builds a query function with four curried args based on raw SQL text.
    /// </summary>
    /// <param name="argName1">
    /// The first parameter name.
    /// </param>
    /// <param name="argName2">
    /// The second parameter name.
    /// </param>
    /// <param name="argName3">
    /// The third parameter name.
    /// </param>
    /// <param name="argName3">
    /// The fourth parameter name.
    /// </param>
    /// </summary>
    /// <param name="resultName">
    /// The result column name.
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="commandText">
    /// The SQL command text.
    /// </param>
    member this.Sql<'Params1, 'Params2, 'Params3, 'Params4, 'Result> (
            [<Optional>] argName1: string, [<Optional>] argName2: string, [<Optional>] argName3: string, [<Optional>] argName4: string, 
            [<Optional>] resultName: string,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : string -> 'Params1 -> 'Params2 -> 'Params3 -> 'Params4 -> DbCall<'Result> =         
        this.Sql(Params.Auto<'Params1>(argName1), Params.Auto<'Params2>(argName2), 
                 Params.Auto<'Params3>(argName3), Params.Auto<'Params4>(argName4), 
                 Results.Auto<'Result>(resultName), 
                 sourcePath, sourceLine) 

    /// <summary>
    /// Builds a query function with four curried args based on raw SQL text.
    /// </summary>
    /// <param name="argName1">
    /// The first parameter name.
    /// </param>
    /// <param name="argName2">
    /// The second parameter name.
    /// </param>
    /// <param name="argName3">
    /// The third parameter name.
    /// </param>
    /// <param name="argName3">
    /// The fourth parameter name.
    /// </param>
    /// </summary>
    /// <param name="resultName">
    /// The result column name.
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="commandText">
    /// The SQL command text.
    /// </param>
    member this.Sql<'Params1, 'Params2, 'Params3, 'Params4, 'Params5, 'Result> (
            [<Optional>] argName1: string, [<Optional>] argName2: string, [<Optional>] argName3: string, argName4: string, [<Optional>] argName5: string, 
            [<Optional>] resultName: string,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : string -> 'Params1 -> 'Params2 -> 'Params3 -> 'Params4 -> 'Params5 -> DbCall<'Result> =         
        this.Sql(Params.Auto<'Params1>(argName1), Params.Auto<'Params2>(argName2), 
                 Params.Auto<'Params3>(argName3), Params.Auto<'Params4>(argName4), 
                 Params.Auto<'Params5>(argName5), 
                 Results.Auto<'Result>(resultName), 
                 sourcePath, sourceLine) 

    /// <param name="procName">
    /// The stored procedure name.
    /// </param>
    /// <summary>
    /// Builds a one arg query function invoking stored procedure.
    /// </summary>
    /// <param name="createParamSetter">
    /// The parameter builder.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="createOutParamGetter">
    /// The output parameter builder.
    /// </param>
    /// <param name="createResultReader">
    /// The result builder.
    /// </param>
    member __.Proc (procName: string, 
                    createParamSetter: BuildParamSetter<'Params>,
                    createOutParamGetter: BuildOutParamGetter<'OutParams>, 
                    resultReaderBuilder: BuildResultReader<'Result>,
                    [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
                    [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
                    : 'Params -> DbCall<'Result * 'OutParams> = 
            try                        
                let provider = GenericSetters.BaseSetterProvider<unit, IDbCommand>(config.ParamBuilders)
                let paramSetter = createParamSetter(provider, ())
                        
                let outParamProvider = GenericGetters.BaseGetterProvider<unit, IDbCommand>(config.OutParamBuilders)
                let outParamGetter = createOutParamGetter(outParamProvider, ())

                let setArtificial(command: IDbCommand) = 
                    paramSetter.SetArtificial(command)
                    outParamGetter.Create(command)

                let rowGetterProvider = GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>(config.RowBuilders)
                let resultReaderBuilder' prototype = resultReaderBuilder(rowGetterProvider, prototype)

                let resultReader = executePrototypeQuery(CommandType.StoredProcedure, procName, setArtificial, resultReaderBuilder')
                fun (parameters: 'Params) (provider: IConnector) ->
                    executeProcedure(provider, procName, outParamGetter, resultReader, fun cmd -> paramSetter.SetValue(parameters, cmd))
            with ex ->
                handleException(sourcePath, sourceLine, ex)

    /// <summary>
    /// Builds a one arg query function invoking stored procedure.
    /// </summary>
    /// <param name="procName">
    /// The stored procedure name.
    /// </param>
    /// <param name="name">
    /// The parameter name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="createOutParamGetter">
    /// The output parameter builder.
    /// </param>
    /// <param name="createResultReader">
    /// The result builder.
    /// </param>
    member this.Proc<'Params, 'OutParams, 'Result> (
            procName: string,    
            name: string,
            createOutParamGetter: BuildOutParamGetter<'OutParams>, 
            resultReaderBuilder: BuildResultReader<'Result>,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params -> DbCall<'Result * 'OutParams> = 
        this.Proc(procName, Params.Auto<'Params>(name), createOutParamGetter, resultReaderBuilder, sourcePath, sourceLine)


    /// <summary>
    /// Builds a one arg query function invoking stored procedure.
    /// </summary>
    /// <param name="procName">
    /// The stored procedure name.
    /// </param>
    /// <param name="name">
    /// The parameter name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="createOutParamGetter">
    /// The output parameter builder.
    /// </param>
    /// <param name="createResultReader">
    /// The result builder.
    /// </param>
    member this.Proc<'Params, 'OutParams, 'Result> (
            procName: string,
            [<Optional>] name: string,
            [<Optional>] outParamName: string, 
            [<Optional>] resultName: string,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params -> DbCall<'Result * 'OutParams> = 
        this.Proc(procName, Params.Auto<'Params>(name), OutParams.Auto<'OutParams>(outParamName), Results.Auto<'Result>(resultName), sourcePath, sourceLine)

    /// <summary>
    /// Builds a query function with two curried args invoking stored procedure.
    /// </summary>
    /// <param name="procName">
    /// The stored procedure name.
    /// </param>
    /// <param name="createParamSetter1">
    /// The first parameter builder.
    /// </param>
    /// <param name="createParamSetter2">
    /// The second parameter builder.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="createOutParamGetter">
    /// The output parameter builder.
    /// </param>
    /// <param name="createResultReader">
    /// The result builder.
    /// </param>
    member __.Proc (procName: string, 
                    createParamSetter1: BuildParamSetter<'Params1>,
                    createParamSetter2: BuildParamSetter<'Params2>,
                    createOutParamGetter: BuildOutParamGetter<'OutParams>, 
                    resultReaderBuilder: BuildResultReader<'Result>,
                    [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
                    [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
                    : 'Params1 -> 'Params2 -> DbCall<'Result * 'OutParams> = 
        try                        
            let provider = GenericSetters.BaseSetterProvider<unit, IDbCommand>(config.ParamBuilders)
            let paramSetter1 = createParamSetter1(provider, ())
            let paramSetter2 = createParamSetter2(provider, ())
                        
            let outParamProvider = GenericGetters.BaseGetterProvider<unit, IDbCommand>(config.OutParamBuilders)
            let outParamGetter = createOutParamGetter(outParamProvider, ())

            let setArtificial(command: IDbCommand) = 
                paramSetter1.SetArtificial(command)
                paramSetter2.SetArtificial(command)
                outParamGetter.Create(command)

            let rowGetterProvider = GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>(config.RowBuilders)
            let resultReaderBuilder' prototype = resultReaderBuilder(rowGetterProvider, prototype)

            let resultReader = executePrototypeQuery(CommandType.StoredProcedure, procName, setArtificial, resultReaderBuilder')

            let setParams (parameters1: 'Params1, parameters2: 'Params2) (command: IDbCommand) = 
                paramSetter1.SetValue(parameters1, command)
                paramSetter2.SetValue(parameters2, command)

            fun (parameters1: 'Params1) (parameters2: 'Params2) (provider: IConnector) ->
                executeProcedure(provider, procName, outParamGetter, resultReader, setParams(parameters1, parameters2))
        with ex ->
            handleException(sourcePath, sourceLine, ex)

    /// <summary>
    /// Builds a query function with two curried args invoking stored procedure.
    /// </summary>
    /// <param name="procName">
    /// The stored procedure name.
    /// </param>
    /// <param name="name1">
    /// The first parameter name.
    /// </param>
    /// <param name="name2">
    /// The second parameter name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="createOutParamGetter">
    /// The output parameter builder.
    /// </param>
    /// <param name="createResultReader">
    /// The result builder.
    /// </param>
    member this.Proc<'Params1, 'Params2, 'OutParams, 'Result> (
            procName: string,
            [<Optional>] name1: string, [<Optional>] name2: string,
            [<Optional>] outParamName: string, 
            [<Optional>] resultName: string,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params1 -> 'Params2 -> DbCall<'Result * 'OutParams> = 
        this.Proc(procName, Params.Auto<'Params1>(name1), Params.Auto<'Params2>(name2), OutParams.Auto<'OutParams>(outParamName), Results.Auto<'Result>(resultName), sourcePath, sourceLine)

    /// <summary>
    /// Builds a query function with two curried args invoking stored procedure.
    /// </summary>
    /// <param name="procName">
    /// The stored procedure name.
    /// </param>
    /// <param name="name1">
    /// The first parameter name.
    /// </param>
    /// <param name="name2">
    /// The second parameter name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="createOutParamGetter">
    /// The output parameter builder.
    /// </param>
    /// <param name="createResultReader">
    /// The result builder.
    /// </param>
    member this.Proc<'Params1, 'Params2, 'OutParams, 'Result> (
            procName: string,
            name1: string, name2: string,
            createOutParamGetter: BuildOutParamGetter<'OutParams>, 
            resultReaderBuilder: BuildResultReader<'Result>,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params1 -> 'Params2 -> DbCall<'Result * 'OutParams> = 
        this.Proc(procName, Params.Auto<'Params1>(name1), Params.Auto<'Params2>(name2), createOutParamGetter, resultReaderBuilder, sourcePath, sourceLine)


    /// <summary>
    /// Builds a query function with three curried args invoking stored procedure.
    /// </summary>
    /// <param name="procName">
    /// The stored procedure name.
    /// </param>
    /// <param name="createParamSetter1">
    /// The first parameter builder.
    /// </param>
    /// <param name="createParamSetter2">
    /// The second parameter builder.
    /// </param>
    /// <param name="createParamSetter3">
    /// The third parameter builder.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="createOutParamGetter">
    /// The output parameter builder.
    /// </param>
    /// <param name="createResultReader">
    /// The result builder.
    /// </param>
    member __.Proc (commandText: string,
                    createParamSetter1: BuildParamSetter<'Params1>,
                    createParamSetter2: BuildParamSetter<'Params2>,
                    createParamSetter3: BuildParamSetter<'Params3>,
                    createOutParamGetter: BuildOutParamGetter<'OutParams>, 
                    resultReaderBuilder: BuildResultReader<'Result>,
                    [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
                    [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
                    : 'Params1 -> 'Params2 -> 'Params3 -> DbCall<'Result * 'OutParams> = 
        try                        
            let provider = GenericSetters.BaseSetterProvider<unit, IDbCommand>(config.ParamBuilders)
            let paramSetter1 = createParamSetter1(provider, ())
            let paramSetter2 = createParamSetter2(provider, ())
            let paramSetter3 = createParamSetter3(provider, ())
                        
            let outParamProvider = GenericGetters.BaseGetterProvider<unit, IDbCommand>(config.OutParamBuilders)
            let outParamGetter = createOutParamGetter(outParamProvider, ())

            let setArtificial(command: IDbCommand) = 
                paramSetter1.SetArtificial(command)
                paramSetter2.SetArtificial(command)
                paramSetter3.SetArtificial(command)
                outParamGetter.Create(command)

            let rowGetterProvider = GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>(config.RowBuilders)
            let resultReaderBuilder' prototype = resultReaderBuilder(rowGetterProvider, prototype)

            let resultReader = executePrototypeQuery(CommandType.StoredProcedure, commandText, setArtificial, resultReaderBuilder')

            let setParams (parameters1: 'Params1, parameters2: 'Params2, parameters3: 'Params3) (command: IDbCommand) = 
                paramSetter1.SetValue(parameters1, command)
                paramSetter2.SetValue(parameters2, command)
                paramSetter3.SetValue(parameters3, command)

            fun (parameters1: 'Params1) (parameters2: 'Params2) (parameters3: 'Params3) (provider: IConnector) ->
                executeProcedure(provider, commandText, outParamGetter, resultReader, setParams(parameters1, parameters2, parameters3))
        with ex ->
            handleException(sourcePath, sourceLine, ex)

    /// <summary>
    /// Builds a query function with three curried args invoking stored procedure.
    /// </summary>
    /// <param name="procName">
    /// The stored procedure name.
    /// </param>
    /// <param name="name1">
    /// The first parameter name.
    /// </param>
    /// <param name="name2">
    /// The second parameter name.
    /// </param>
    /// <param name="name3">
    /// The third parameter name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="createOutParamGetter">
    /// The output parameter builder.
    /// </param>
    /// <param name="createResultReader">
    /// The result builder.
    /// </param>
    member this.Proc<'Params1, 'Params2, 'Params3, 'OutParams, 'Result> (
            procName: string,
            name1: string, name2: string, name3: string,
            createOutParamGetter: BuildOutParamGetter<'OutParams>, 
            resultReaderBuilder: BuildResultReader<'Result>,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params1 -> 'Params2 -> 'Params3 -> DbCall<'Result * 'OutParams> = 
        this.Proc(procName, Params.Auto<'Params1>(name1), Params.Auto<'Params2>(name2), Params.Auto<'Params3>(name3), createOutParamGetter, resultReaderBuilder, sourcePath, sourceLine)

    /// <summary>
    /// Builds a query function with two curried args invoking stored procedure.
    /// </summary>
    /// <param name="procName">
    /// The stored procedure name.
    /// </param>
    /// <param name="name1">
    /// The first parameter name.
    /// </param>
    /// <param name="name2">
    /// The second parameter name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="createOutParamGetter">
    /// The output parameter builder.
    /// </param>
    /// <param name="createResultReader">
    /// The result builder.
    /// </param>
    member this.Proc<'Params1, 'Params2, 'Params3, 'OutParams, 'Result> (
                procName: string,
                [<Optional>] name1: string, [<Optional>] name2: string, [<Optional>] name3: string,
                [<Optional>] outParamName: string, 
                [<Optional>] resultName: string,
                [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
                [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
                : 'Params1 -> 'Params2 -> 'Params3 -> DbCall<'Result * 'OutParams> = 
        this.Proc(procName, 
                  Params.Auto<'Params1>(name1), Params.Auto<'Params2>(name2), Params.Auto<'Params3>(name3), 
                  OutParams.Auto<'OutParams>(outParamName), Results.Auto<'Result>(resultName), 
                  sourcePath, sourceLine)                

    /// <summary>
    /// Builds a query function with four curried args invoking stored procedure.
    /// </summary>
    /// <param name="procName">
    /// The stored procedure name.
    /// </param>
    /// <param name="createParamSetter1">
    /// The first parameter builder.
    /// </param>
    /// <param name="createParamSetter2">
    /// The second parameter builder.
    /// </param>
    /// <param name="createParamSetter3">
    /// The third parameter builder.
    /// </param>
    /// <param name="createParamSetter4">
    /// The fourth parameter builder.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="createOutParamGetter">
    /// The output parameter builder.
    /// </param>
    /// <param name="createResultReader">
    /// The result builder.
    /// </param>
    member __.Proc (procName: string,
                    createParamSetter1: BuildParamSetter<'Params1>,
                    createParamSetter2: BuildParamSetter<'Params2>,
                    createParamSetter3: BuildParamSetter<'Params3>,
                    createParamSetter4: BuildParamSetter<'Params4>,
                    createOutParamGetter: BuildOutParamGetter<'OutParams>, 
                    resultReaderBuilder: BuildResultReader<'Result>,
                    [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
                    [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
                    : 'Params1 -> 'Params2 -> 'Params3 -> 'Params4 -> DbCall<'Result * 'OutParams> = 
        try                        
            let provider = GenericSetters.BaseSetterProvider<unit, IDbCommand>(config.ParamBuilders)
            let paramSetter1 = createParamSetter1(provider, ())
            let paramSetter2 = createParamSetter2(provider, ())
            let paramSetter3 = createParamSetter3(provider, ())
            let paramSetter4 = createParamSetter4(provider, ())
                        
            let outParamProvider = GenericGetters.BaseGetterProvider<unit, IDbCommand>(config.OutParamBuilders)
            let outParamGetter = createOutParamGetter(outParamProvider, ())

            let setArtificial(command: IDbCommand) = 
                paramSetter1.SetArtificial(command)
                paramSetter2.SetArtificial(command)
                paramSetter3.SetArtificial(command)
                paramSetter4.SetArtificial(command)
                outParamGetter.Create(command)

            let rowGetterProvider = GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>(config.RowBuilders)
            let resultReaderBuilder' prototype = resultReaderBuilder(rowGetterProvider, prototype)

            let resultReader = executePrototypeQuery(CommandType.StoredProcedure, procName, setArtificial, resultReaderBuilder')

            let setParams (parameters1: 'Params1, parameters2: 'Params2, parameters3: 'Params3, parameters4: 'Params4) (command: IDbCommand) = 
                paramSetter1.SetValue(parameters1, command)
                paramSetter2.SetValue(parameters2, command)
                paramSetter3.SetValue(parameters3, command)
                paramSetter4.SetValue(parameters4, command)

            fun (parameters1: 'Params1) (parameters2: 'Params2) (parameters3: 'Params3) (parameters4: 'Params4) (provider: IConnector) ->
                executeProcedure(provider, procName, outParamGetter, resultReader, setParams(parameters1, parameters2, parameters3, parameters4))
        with ex ->
            handleException(sourcePath, sourceLine, ex)

    /// <summary>
    /// Builds a query function with four curried args invoking stored procedure.
    /// </summary>
    /// <param name="procName">
    /// The stored procedure name.
    /// </param>
    /// <param name="name1">
    /// The first parameter name.
    /// </param>
    /// <param name="name2">
    /// The second parameter name.
    /// </param>
    /// <param name="name3">
    /// The third parameter name.
    /// </param>
    /// <param name="name4">
    /// The fourth parameter name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="createOutParamGetter">
    /// The output parameter builder.
    /// </param>
    /// <param name="createResultReader">
    /// The result builder.
    /// </param>
    member this.Proc<'Params1, 'Params2, 'Params3, 'Params4, 'OutParams, 'Result> (
                procName: string,
                name1: string, name2: string, name3: string, name4: string,
                createOutParamGetter: BuildOutParamGetter<'OutParams>, 
                resultReaderBuilder: BuildResultReader<'Result>,
                [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
                [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
                : 'Params1 -> 'Params2 -> 'Params3 -> 'Params4 -> DbCall<'Result * 'OutParams> = 
        this.Proc(procName, 
                  Params.Auto<'Params1>(name1), Params.Auto<'Params2>(name2), 
                  Params.Auto<'Params3>(name3), Params.Auto<'Params4>(name4), 
                  createOutParamGetter, resultReaderBuilder,
                  sourcePath, sourceLine)
    /// <summary>
    /// Builds a query function with four curried args invoking stored procedure.
    /// </summary>
    /// <param name="procName">
    /// The stored procedure name.
    /// </param>
    /// <param name="name1">
    /// The first parameter name.
    /// </param>
    /// <param name="name2">
    /// The second parameter name.
    /// </param>
    /// <param name="name3">
    /// The third parameter name.
    /// </param>
    /// <param name="name4">
    /// The fourth parameter name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="createOutParamGetter">
    /// The output parameter builder.
    /// </param>
    /// <param name="createResultReader">
    /// The result builder.
    /// </param>
    member this.Proc<'Params1, 'Params2, 'Params3, 'Params4, 'OutParams, 'Result> (
            procName: string,
            [<Optional>] name1: string, [<Optional>] name2: string, [<Optional>] name3: string, [<Optional>] name4: string,
            [<Optional>] outParamName: string, 
            [<Optional>] resultName: string,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params1 -> 'Params2 -> 'Params3 -> 'Params4 -> DbCall<'Result * 'OutParams> = 
        this.Proc(procName,
                  Params.Auto<'Params1>(name1), Params.Auto<'Params2>(name2), 
                  Params.Auto<'Params3>(name3), Params.Auto<'Params4>(name4), 
                  OutParams.Auto<'OutParams>(outParamName), 
                  Results.Auto<'Result>(resultName), 
                  sourcePath, sourceLine)
                
    /// <summary>
    /// Builds a query function with five curried args invoking stored procedure.
    /// </summary>
    /// <param name="procName">
    /// The stored procedure name.
    /// </param>
    /// <param name="createParamSetter1">
    /// The first parameter builder.
    /// </param>
    /// <param name="createParamSetter2">
    /// The second parameter builder.
    /// </param>
    /// <param name="createParamSetter3">
    /// The third parameter builder.
    /// </param>
    /// <param name="createParamSetter4">
    /// The fourth parameter builder.
    /// </param>
    /// <param name="createParamSetter5">
    /// The fifth parameter builder.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="createOutParamGetter">
    /// The output parameter builder.
    /// </param>
    /// <param name="createResultReader">
    /// The result builder.
    /// </param>
    member __.Proc (procName: string,
                    createParamSetter1: BuildParamSetter<'Params1>,
                    createParamSetter2: BuildParamSetter<'Params2>,
                    createParamSetter3: BuildParamSetter<'Params3>,
                    createParamSetter4: BuildParamSetter<'Params4>,
                    createParamSetter5: BuildParamSetter<'Params5>,
                    createOutParamGetter: BuildOutParamGetter<'OutParams>, 
                    resultReaderBuilder: BuildResultReader<'Result>,
                    [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
                    [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
                    : 'Params1 -> 'Params2 -> 'Params3 -> 'Params4 -> 'Params5 -> DbCall<'Result * 'OutParams> = 
        try                        
            let provider = GenericSetters.BaseSetterProvider<unit, IDbCommand>(config.ParamBuilders)
            let paramSetter1 = createParamSetter1(provider, ())
            let paramSetter2 = createParamSetter2(provider, ())
            let paramSetter3 = createParamSetter3(provider, ())
            let paramSetter4 = createParamSetter4(provider, ())
            let paramSetter5 = createParamSetter5(provider, ())
                        
            let outParamProvider = GenericGetters.BaseGetterProvider<unit, IDbCommand>(config.OutParamBuilders)
            let outParamGetter = createOutParamGetter(outParamProvider, ())

            let setArtificial(command: IDbCommand) = 
                paramSetter1.SetArtificial(command)
                paramSetter2.SetArtificial(command)
                paramSetter3.SetArtificial(command)
                paramSetter4.SetArtificial(command)
                paramSetter5.SetArtificial(command)
                outParamGetter.Create(command)

            let rowGetterProvider = GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>(config.RowBuilders)
            let resultReaderBuilder' prototype = resultReaderBuilder(rowGetterProvider, prototype)

            let resultReader = executePrototypeQuery(CommandType.StoredProcedure, procName, setArtificial, resultReaderBuilder')

            let setParams (parameters1: 'Params1, parameters2: 'Params2, parameters3: 'Params3, parameters4: 'Params4, parameters5: 'Params5) (command: IDbCommand) = 
                paramSetter1.SetValue(parameters1, command)
                paramSetter2.SetValue(parameters2, command)
                paramSetter3.SetValue(parameters3, command)
                paramSetter4.SetValue(parameters4, command)
                paramSetter5.SetValue(parameters5, command)

            fun (parameters1: 'Params1) (parameters2: 'Params2) (parameters3: 'Params3) (parameters4: 'Params4) (parameters5: 'Params5) (provider: IConnector) ->
                executeProcedure(provider, procName, outParamGetter, resultReader, setParams(parameters1, parameters2, parameters3, parameters4, parameters5))
        with ex ->
            handleException(sourcePath, sourceLine, ex)

    /// <summary>
    /// Builds a query function with five curried args invoking stored procedure.
    /// </summary>
    /// <param name="procName">
    /// The stored procedure name.
    /// </param>
    /// <param name="name1">
    /// The first parameter name.
    /// </param>
    /// <param name="name2">
    /// The second parameter name.
    /// </param>
    /// <param name="name3">
    /// The third parameter name.
    /// </param>
    /// <param name="name4">
    /// The fourth parameter name.
    /// </param>
    /// <param name="name5">
    /// The fifth parameter name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="createOutParamGetter">
    /// The output parameter builder.
    /// </param>
    /// <param name="createResultReader">
    /// The result builder.
    /// </param>
    member this.Proc<'Params1, 'Params2, 'Params3, 'Params4, 'Params5, 'OutParams, 'Result> (
            procName: string,
            name1: string, name2: string, name3: string, name4: string, name5: string,
            createOutParamGetter: BuildOutParamGetter<'OutParams>, 
            resultReaderBuilder: BuildResultReader<'Result>,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params1 -> 'Params2 -> 'Params3 -> 'Params4 -> 'Params5 -> DbCall<'Result * 'OutParams> = 
        this.Proc(procName,
                  Params.Auto<'Params1>(name1), Params.Auto<'Params2>(name2), 
                  Params.Auto<'Params3>(name3), Params.Auto<'Params4>(name4), 
                  Params.Auto<'Params5>(name5),
                  createOutParamGetter,
                  resultReaderBuilder,
                  sourcePath, sourceLine)

    /// <summary>
    /// Builds a query function with five curried args invoking stored procedure.
    /// </summary>
    /// <param name="procName">
    /// The stored procedure name.
    /// </param>
    /// <param name="name1">
    /// The first parameter name.
    /// </param>
    /// <param name="name2">
    /// The second parameter name.
    /// </param>
    /// <param name="name3">
    /// The third parameter name.
    /// </param>
    /// <param name="name4">
    /// The fourth parameter name.
    /// </param>
    /// <param name="name5">
    /// The fifth parameter name.
    /// </param>
    /// <param name="sourcePath">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="sourceLine">
    /// The calling source path for diagnostic purposes.
    /// </param>
    /// <param name="createOutParamGetter">
    /// The output parameter builder.
    /// </param>
    /// <param name="createResultReader">
    /// The result builder.
    /// </param>
    member this.Proc<'Params1, 'Params2, 'Params3, 'Params4, 'Params5, 'OutParams, 'Result> (
            procName: string,
            [<Optional>] name1: string, [<Optional>] name2: string, [<Optional>] name3: string, [<Optional>] name4: string, [<Optional>] name5: string,
            [<Optional>] outParamName: string, 
            [<Optional>] resultName: string,
            [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
            [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
            : 'Params1 -> 'Params2 -> 'Params3 -> 'Params4 -> 'Params5 -> DbCall<'Result * 'OutParams> = 
        this.Proc(procName, 
                  Params.Auto<'Params1>(name1), Params.Auto<'Params2>(name2), 
                  Params.Auto<'Params3>(name3), Params.Auto<'Params4>(name4), 
                  Params.Auto<'Params5>(name5),
                  OutParams.Auto<'OutParams>(outParamName), 
                  Results.Auto<'Result>(resultName), 
                  sourcePath, sourceLine)                