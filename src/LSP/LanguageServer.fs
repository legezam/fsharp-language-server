module LSP.LanguageServer 

open LSP.Log
open System
open System.Threading
open System.Threading.Tasks
open System.IO
open System.Text
open FSharp.Data
open Types 
open LSP.Json.Ser
open JsonExtensions
open System.Collections.Concurrent
let private jsonWriteOptions = 
    { defaultJsonWriteOptions with 
        customWriters = 
            [ writeTextDocumentSaveReason;
              writeFileChangeType;
              writeTextDocumentSyncKind;
              writeDiagnosticSeverity;
              writeTrace;
              writeInsertTextFormat;
              writeCompletionItemKind;
              writeMarkedString;
              writeDocumentHighlightKind;
              writeSymbolKind;
              writeRegisterCapability;
              writeMessageType;
              writeMarkupKind ] }

let private serializeInitializeResult = serializerFactory<InitializeResult> jsonWriteOptions
let private serializeTextEditList = serializerFactory<TextEdit list> jsonWriteOptions
let private serializeCompletionList = serializerFactory<CompletionList> jsonWriteOptions
let private serializeCompletionListOption = Option.map serializeCompletionList
let private serializeHover = serializerFactory<Hover> jsonWriteOptions
let private serializeHoverOption = Option.map serializeHover
let private serializeCompletionItem = serializerFactory<CompletionItem> jsonWriteOptions
let private serializeSignatureHelp = serializerFactory<SignatureHelp> jsonWriteOptions
let private serializeSignatureHelpOption = Option.map serializeSignatureHelp
let private serializeLocationList = serializerFactory<Location list> jsonWriteOptions
let private serializeDocumentHighlightList = serializerFactory<DocumentHighlight list> jsonWriteOptions
let private serializeSymbolInformationList = serializerFactory<SymbolInformation list> jsonWriteOptions
let private serializeCommandList = serializerFactory<Command list> jsonWriteOptions
let private serializeCodeLensList = serializerFactory<CodeLens list> jsonWriteOptions
let private serializeCodeLens = serializerFactory<CodeLens> jsonWriteOptions
let private serializeDocumentLinkList = serializerFactory<DocumentLink list> jsonWriteOptions
let private serializeDocumentLink = serializerFactory<DocumentLink> jsonWriteOptions
let private serializeWorkspaceEdit = serializerFactory<WorkspaceEdit> jsonWriteOptions
let private serializePublishDiagnostics = serializerFactory<PublishDiagnosticsParams> jsonWriteOptions
let private serializeShowMessage = serializerFactory<ShowMessageParams> jsonWriteOptions
let private serializeRegistrationParams = serializerFactory<RegistrationParams> jsonWriteOptions

let private writeClient (client: BinaryWriter, messageText: string) =
    let messageBytes = Encoding.UTF8.GetBytes(messageText)
    let headerText = sprintf "Content-Length: %d\r\n\r\n" messageBytes.Length
    let headerBytes = Encoding.UTF8.GetBytes(headerText)
    client.Write(headerBytes)
    client.Write(messageBytes)

let respond(client: BinaryWriter, requestId: int, jsonText: string) = 
    let messageText = sprintf """{"id":%d,"jsonrpc":"2.0","result":%s}""" requestId jsonText
    writeClient(client, messageText)

let private notifyClient(client: BinaryWriter, method: string, jsonText: string) = 
    let messageText = sprintf """{"jsonrpc":"2.0","method":"%s","params":%s}""" method jsonText
    writeClient(client, messageText)

let private thenMap (f: 'A -> 'B) (result: Async<'A>): Async<'B> =
    async {
        let! a = result 
        return f a
    }
let private thenSome = thenMap Some
let private thenNone(result: Async<'A>): Async<string option> = result |> thenMap (fun _ -> None)

let private notExit (message: Parser.Message) = 
    match message with 
    | Parser.NotificationMessage("exit", _) -> false 
    | _ -> true

let readMessages(receive: BinaryReader): seq<Parser.Message> = 
    let tokens = Tokenizer.tokenize(receive)
    let parse = Seq.map Parser.parseMessage tokens
    Seq.takeWhile notExit parse

type RealClient (send: BinaryWriter) = 
    interface ILanguageClient with 
        member this.PublishDiagnostics(p: PublishDiagnosticsParams): unit = 
            let json = serializePublishDiagnostics(p)
            notifyClient(send, "textDocument/publishDiagnostics", json)
        member this.ShowMessage(p: ShowMessageParams): unit = 
            let json = serializeShowMessage(p)
            notifyClient(send, "window/showMessage", json)
        member this.RegisterCapability(p: RegisterCapability): unit = 
            match p with 
            | RegisterCapability.DidChangeWatchedFiles _ -> 
                let register = {id=Guid.NewGuid().ToString(); method="workspace/didChangeWatchedFiles"; registerOptions=p}
                let message = {registrations=[register]}
                let json = serializeRegistrationParams(message)
                notifyClient(send, "client/registerCapability", json)
        member this.CustomNotification(method: string, json: JsonValue): unit = 
            let jsonString = json.ToString(JsonSaveOptions.DisableFormatting)
            notifyClient(send, method, jsonString)

type private PendingTask = 
| ProcessNotification of method: string * task: Async<unit> 
| ProcessRequest of id: int * task: Async<string option> * cancel: CancellationTokenSource
| Quit

let connect(serverFactory: ILanguageClient -> ILanguageServer, receive: BinaryReader, send: BinaryWriter) = 
    let server = serverFactory(RealClient(send))
    let processRequest: Request -> Async<string option> = function
        | Request.Initialize(p) -> 
            server.Initialize(p) |> thenMap serializeInitializeResult |> thenSome
        | Request.WillSaveWaitUntilTextDocument(p) -> 
            server.WillSaveWaitUntilTextDocument(p) |> thenMap serializeTextEditList |> thenSome
        | Request.Completion(p) -> 
            server.Completion(p) |> thenMap serializeCompletionListOption
        | Request.Hover(p) -> 
            server.Hover(p) |> thenMap serializeHoverOption |> thenMap (Option.defaultValue "null") |> thenSome
        | Request.ResolveCompletionItem(p) -> 
            server.ResolveCompletionItem(p) |> thenMap serializeCompletionItem |> thenSome 
        | Request.SignatureHelp(p) -> 
            server.SignatureHelp(p) |> thenMap serializeSignatureHelpOption |> thenMap (Option.defaultValue "null") |> thenSome
        | Request.GotoDefinition(p) -> 
            server.GotoDefinition(p) |> thenMap serializeLocationList |> thenSome
        | Request.FindReferences(p) -> 
            server.FindReferences(p) |> thenMap serializeLocationList |> thenSome
        | Request.DocumentHighlight(p) -> 
            server.DocumentHighlight(p) |> thenMap serializeDocumentHighlightList |> thenSome
        | Request.DocumentSymbols(p) -> 
            server.DocumentSymbols(p) |> thenMap serializeSymbolInformationList |> thenSome
        | Request.WorkspaceSymbols(p) -> 
            server.WorkspaceSymbols(p) |> thenMap serializeSymbolInformationList |> thenSome
        | Request.CodeActions(p) -> 
            server.CodeActions(p) |> thenMap serializeCommandList |> thenSome
        | Request.CodeLens(p) -> 
            server.CodeLens(p) |> thenMap serializeCodeLensList |> thenSome
        | Request.ResolveCodeLens(p) -> 
            server.ResolveCodeLens(p) |> thenMap serializeCodeLens |> thenSome
        | Request.DocumentLink(p) -> 
            server.DocumentLink(p) |> thenMap serializeDocumentLinkList |> thenSome
        | Request.ResolveDocumentLink(p) -> 
            server.ResolveDocumentLink(p) |> thenMap serializeDocumentLink |> thenSome
        | Request.DocumentFormatting(p) -> 
            server.DocumentFormatting(p) |> thenMap serializeTextEditList |> thenSome
        | Request.DocumentRangeFormatting(p) -> 
            server.DocumentRangeFormatting(p) |> thenMap serializeTextEditList |> thenSome
        | Request.DocumentOnTypeFormatting(p) -> 
            server.DocumentOnTypeFormatting(p) |> thenMap serializeTextEditList |> thenSome
        | Request.Rename(p) -> 
            server.Rename(p) |> thenMap serializeWorkspaceEdit |> thenSome
        | Request.ExecuteCommand(p) -> 
            server.ExecuteCommand(p) |> thenNone
        | Request.DidChangeWorkspaceFolders(p) ->
            server.DidChangeWorkspaceFolders(p) |> thenNone
        | Request.Shutdown ->
            server.Shutdown() |> thenNone

    let processNotification: Notification -> Async<unit> =  function
        | Notification.Initialized ->
            server.Initialized()
        | Notification.DidChangeConfiguration(p) -> 
            server.DidChangeConfiguration(p)
        | Notification.DidOpenTextDocument(p) -> 
            server.DidOpenTextDocument(p)
        | Notification.DidChangeTextDocument(p) -> 
            server.DidChangeTextDocument(p)
        | Notification.WillSaveTextDocument(p) -> 
            server.WillSaveTextDocument(p)
        | Notification.DidSaveTextDocument(p) -> 
            server.DidSaveTextDocument(p)
        | Notification.DidCloseTextDocument(p) -> 
            server.DidCloseTextDocument(p)
        | Notification.DidChangeWatchedFiles(p) -> 
            server.DidChangeWatchedFiles(p)
        | Notification.OtherNotification(_) ->
            async { () }
    // Read messages and process cancellations on a separate thread
    let pendingRequests = new ConcurrentDictionary<int, CancellationTokenSource>()
    let processQueue = new BlockingCollection<PendingTask>(10)
    Thread(fun () -> 
        try
            // Read all messages on the main thread
            for message in readMessages(receive) do 
                // Process cancellations immediately
                match message with 
                | Parser.NotificationMessage("$/cancelRequest", Some json) -> 
                    let id = json?id.AsInteger()
                    let stillRunning, pendingRequest = pendingRequests.TryGetValue(id)
                    if stillRunning then
                        dprintfn "Cancelling request %d" id
                        pendingRequest.Cancel()
                    else 
                        dprintfn "Request %d has already finished" id
                // Process other requests on worker thread
                | Parser.NotificationMessage(method, json) -> 
                    let n = Parser.parseNotification(method, json)
                    let task = processNotification(n)
                    processQueue.Add(ProcessNotification(method, task))
                | Parser.RequestMessage(id, method, json) -> 
                    let task = processRequest(Parser.parseRequest(method, json)) 
                    let cancel = new CancellationTokenSource()
                    processQueue.Add(ProcessRequest(id, task, cancel))
                    pendingRequests.[id] <- cancel
            processQueue.Add(Quit)
        with e -> 
            dprintfn "Exception in read thread %O" e
    ).Start()
    // Process messages on main thread
    let mutable quit = false
    while not quit do 
        match processQueue.Take() with 
        | Quit -> quit <- true
        | ProcessNotification(method, task) -> Async.RunSynchronously(task)
        | ProcessRequest(id, task, cancel) -> 
            if cancel.IsCancellationRequested then 
                dprintfn "Skipping cancelled request %d" id
            else
                try 
                    match Async.RunSynchronously(task, 0, cancel.Token) with 
                    | Some(result) -> respond(send, id, result)
                    | None -> respond(send, id, "null")
                with :? OperationCanceledException -> 
                    dprintfn "Request %d was cancelled" id
            pendingRequests.TryRemove(id) |> ignore
