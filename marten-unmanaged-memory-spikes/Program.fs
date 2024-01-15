open System
open System.Diagnostics
open System.Threading
open System.Threading.Tasks

open FSharp.Control
open FSharpPlus

open Marten
open Marten.Events


[<RequireQualifiedAccess>]
module String =
    open System.Text.RegularExpressions

    let toSnakeCase (source: string) =
        Regex.Replace(Regex.Replace(source, "(.)([A-Z][a-z]+)", "$1_$2"), "([a-z0-9])([A-Z])", "$1_$2")
        |> String.toLower

[<RequireQualifiedAccess>]
module Task =

    let getAwaiterResult (source: Task) = source.GetAwaiter().GetResult()

[<RequireQualifiedAccess>]
module Marten =

    [<RequireQualifiedAccess>]
    module UnionCaseInfo =
        open Microsoft.FSharp.Reflection

        let inline toName (source: UnionCaseInfo) = source.Name

        let inline toCase<'T> (source: UnionCaseInfo) =
            FSharpValue.MakeUnion(source, Array.zeroCreate (source.GetFields().Length)) :?> 'T

        let inline ofDu<'T> () = typeof<'T> |> FSharpType.GetUnionCases

    type MartenLogger() =

        let mutable stopwatch = Stopwatch()

        let logSession msg (session: IQuerySession) =
            printfn $"%s{msg}"
            printfn $"- Migrator.Role: {session.Database.Migrator.Role}"
            printfn $"- Migrator.DefaultSchemaName: {session.Database.Migrator.DefaultSchemaName}"
            printfn $"- Migrator.UpsertRights: {session.Database.Migrator.UpsertRights}"
            printfn "- IMartenLogger: Method \"StartSession\""
            printfn $"- Elapsed: {stopwatch.Elapsed}"

        let logCommand msg lvl sql props =
            stopwatch.Stop()
            printfn $"[%A{lvl}]: %s{msg}"

            for prop in props do
                printfn $"- %A{prop}"

            printfn $"%s{sql}"


        interface IMartenLogger with
            member this.SchemaChange(sql) =
                printfn $"%s{sql}"
                printfn "- IMartenLogger: Method \"SchemaChange\""
                printfn "- Marten: Executing DDL change"

            member this.StartSession session =
                logSession "Marten: Start Session" session
                this

        interface IMartenSessionLogger with

            member this.RecordSavedChanges(session, _) =
                logSession "Marten: Record Save Changes" session

            member this.OnBeforeExecute(command) =
                logCommand "Marten: Before Executing SQL Command" "Information" command.CommandText []
                stopwatch.Restart()

            member this.LogFailure(command, ex) =
                logCommand "Marten: Executing SQL Command Failure" "Error" command.CommandText [ ex ]

            member this.LogSuccess(command) =
                logCommand "Marten: Executing SQL Command Success" "Information" command.CommandText []


    [<RequireQualifiedAccess>]
    module DocumentStore =

        open Weasel.Core

        open JasperFx.CodeGeneration

        let forDuEvent<'Event when 'Event: not struct> (dbConnString: string) dbSchema streamIdentity =
            let events =
                UnionCaseInfo.ofDu<'Event> ()
                |> Array.map (fun unionCaseInfo ->
                    unionCaseInfo |> UnionCaseInfo.toCase |> (_.GetType()),
                    unionCaseInfo |> UnionCaseInfo.toName |> String.toSnakeCase)

            let mapEventNameType (source: IEventStoreOptions) (eventType, eventTypeName) =
                source.MapEventType(eventType, eventTypeName)

            DocumentStore.For(fun storeOptions ->
                storeOptions.Logger(ConsoleMartenLogger())
                storeOptions.Connection(dbConnString)
                storeOptions.Logger(MartenLogger())
                storeOptions.DatabaseSchemaName <- dbSchema
                storeOptions.AutoCreateSchemaObjects <- AutoCreate.None
                storeOptions.Events.StreamIdentity <- streamIdentity
                storeOptions.GeneratedCodeMode <- TypeLoadMode.Auto
                Array.iter (mapEventNameType storeOptions.Events) events)

    [<RequireQualifiedAccess>]
    module IDocumentSession =

        let readEventsTask<'Event> (source: IDocumentSession) cancellationToken streamId =
            source.Events.FetchStreamAsync(streamKey = streamId, token = cancellationToken)
            |>> (Seq.map (fun (iEvent: IEvent) -> iEvent.Data :?> 'Event) >> Seq.toList)

type SomeEvent =
    | A of String: string
    | B of {| String: string; Int32: int32 |}

let readLine prompt =
    printfn $"%s{prompt}"
    Console.ReadLine() |> ignore

type PlaygroundSettings =
    { ConnectionString: string
      Schema: string }

[<RequireQualifiedAccess>]
module Playground =

    [<Literal>]
    let DummyEmptyStreamId = "'I Am The Edge'Case"

    let private runStepTask settings step =
        task {
            printfn $"%d{step}: Start"

            use documentStore =
                (settings.ConnectionString, settings.Schema, StreamIdentity.AsString)
                |||> Marten.DocumentStore.forDuEvent<SomeEvent>

            use documentSession = documentStore.LightweightSession()

            do!
                (documentSession, CancellationToken.None, DummyEmptyStreamId)
                |||> Marten.IDocumentSession.readEventsTask<SomeEvent>
                |> Task.ignore

            printfn $"%d{step}: Stop"
        }
        :> Task

    /// Simulate multiple attempts at reading of the same non-existing stream.
    let runTask settings =
        readLine "Press <enter> to continue when you're ready"
        [| 1..200 |] |>> (runStepTask settings) |> Task.WhenAll


{ ConnectionString = "my-pg-connection-string"
  Schema = "my-pg-schema" }
|> Playground.runTask
|> Task.getAwaiterResult
