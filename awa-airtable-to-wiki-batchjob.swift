import AWSLambdaRuntime
import AWSLambdaEvents
import Foundation
import AsyncHTTPClient
import NIOFoundationCompat

// MARK: - Configuration
struct SyncConfiguration {
    let airtableAPIKey: String
    let airtableBaseID: String
    let airtableTableID: String
    let outlineAPIToken: String
    let outlineTeamID: String
    let outlineDocumentID: String?
    
    init() throws {
        guard let airtableKey = ProcessInfo.processInfo.environment["AIRTABLE_API_KEY"],
              let baseID = ProcessInfo.processInfo.environment["AIRTABLE_BASE_ID"],
              let tableID = ProcessInfo.processInfo.environment["AIRTABLE_TABLE_ID"],
              let outlineToken = ProcessInfo.processInfo.environment["OUTLINE_API_TOKEN"],
              let teamID = ProcessInfo.processInfo.environment["OUTLINE_TEAM_ID"] else {
            throw SyncError.missingEnvironmentVariables
        }
        
        self.airtableAPIKey = airtableKey
        self.airtableBaseID = baseID
        self.airtableTableID = tableID
        self.outlineAPIToken = outlineToken
        self.outlineTeamID = teamID
        self.outlineDocumentID = ProcessInfo.processInfo.environment["OUTLINE_DOCUMENT_ID"]
    }
}

// MARK: - Error Handling
enum SyncError: Error, CustomStringConvertible {
    case missingEnvironmentVariables
    case airtableAPIError(String)
    case outlineAPIError(String)
    case dataTransformationError
    case httpError(Int, String)
    
    var description: String {
        switch self {
        case .missingEnvironmentVariables:
            return "Required environment variables are missing"
        case .airtableAPIError(let message):
            return "AirTable API error: \(message)"
        case .outlineAPIError(let message):
            return "Outline API error: \(message)"
        case .dataTransformationError:
            return "Failed to transform data"
        case .httpError(let code, let message):
            return "HTTP error \(code): \(message)"
        }
    }
}

// MARK: - Data Models
struct AirTableResponse: Codable {
    let records: [AirTableRecord]
    let offset: String?
}

struct AirTableRecord: Codable {
    let id: String
    let fields: [String: AirTableValue]
    let createdTime: String
}

enum AirTableValue: Codable {
    case string(String)
    case number(Double)
    case bool(Bool)
    case array([String])
    case null
    
    init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        
        if container.decodeNil() {
            self = .null
        } else if let stringValue = try? container.decode(String.self) {
            self = .string(stringValue)
        } else if let numberValue = try? container.decode(Double.self) {
            self = .number(numberValue)
        } else if let boolValue = try? container.decode(Bool.self) {
            self = .bool(boolValue)
        } else if let arrayValue = try? container.decode([String].self) {
            self = .array(arrayValue)
        } else {
            self = .null
        }
    }
    
    func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        switch self {
        case .string(let value):
            try container.encode(value)
        case .number(let value):
            try container.encode(value)
        case .bool(let value):
            try container.encode(value)
        case .array(let value):
            try container.encode(value)
        case .null:
            try container.encodeNil()
        }
    }
}

struct OutlineDocument: Codable {
    let id: String
    let title: String
    let text: String
}

struct OutlineCreateDocumentRequest: Codable {
    let title: String
    let text: String
    let teamId: String
    let publish: Bool
}

struct OutlineUpdateDocumentRequest: Codable {
    let id: String
    let text: String
    let title: String?
}

struct OutlineResponse<T: Codable>: Codable {
    let data: T?
    let ok: Bool
    let error: String?
}

// MARK: - HTTP Client Extensions
extension HTTPClient.Request {
    static func airtableRequest(
        url: String,
        apiKey: String,
        method: HTTPMethod = .GET
    ) -> HTTPClient.Request {
        var request = try! HTTPClient.Request(url: url, method: method)
        request.headers.add(name: "Authorization", value: "Bearer \(apiKey)")
        request.headers.add(name: "Content-Type", value: "application/json")
        return request
    }
    
    static func outlineRequest(
        url: String,
        apiToken: String,
        method: HTTPMethod = .POST,
        body: Data? = nil
    ) -> HTTPClient.Request {
        var request = try! HTTPClient.Request(url: url, method: method)
        request.headers.add(name: "Authorization", value: "Bearer \(apiToken)")
        request.headers.add(name: "Content-Type", value: "application/json")
        if let body = body {
            request.body = .data(body)
        }
        return request
    }
}

// MARK: - Service Classes
actor AirTableService {
    private let httpClient: HTTPClient
    private let configuration: SyncConfiguration
    
    init(httpClient: HTTPClient, configuration: SyncConfiguration) {
        self.httpClient = httpClient
        self.configuration = configuration
    }
    
    func fetchAllRecords() async throws -> [AirTableRecord] {
        var allRecords: [AirTableRecord] = []
        var offset: String?
        
        repeat {
            let records = try await fetchRecords(offset: offset)
            allRecords.append(contentsOf: records.records)
            offset = records.offset
        } while offset != nil
        
        return allRecords
    }
    
    private func fetchRecords(offset: String?) async throws -> AirTableResponse {
        var url = "https://api.airtable.com/v0/\(configuration.airtableBaseID)/\(configuration.airtableTableID)"
        if let offset = offset {
            url += "?offset=\(offset)"
        }
        
        let request = HTTPClient.Request.airtableRequest(
            url: url,
            apiKey: configuration.airtableAPIKey
        )
        
        let response = try await httpClient.execute(request: request, timeout: .seconds(30))
        
        guard response.status.code == 200 else {
            let body = response.body?.getString(at: 0, length: response.body?.readableBytes ?? 0) ?? ""
            throw SyncError.httpError(Int(response.status.code), body)
        }
        
        guard let body = response.body else {
            throw SyncError.airtableAPIError("Empty response body")
        }
        
        let data = Data(buffer: body)
        let decoder = JSONDecoder()
        
        do {
            return try decoder.decode(AirTableResponse.self, from: data)
        } catch {
            throw SyncError.airtableAPIError("Failed to decode response: \(error)")
        }
    }
}

actor OutlineService {
    private let httpClient: HTTPClient
    private let configuration: SyncConfiguration
    
    init(httpClient: HTTPClient, configuration: SyncConfiguration) {
        self.httpClient = httpClient
        self.configuration = configuration
    }
    
    func createOrUpdateDocument(title: String, content: String) async throws {
        if let documentID = configuration.outlineDocumentID {
            try await updateDocument(id: documentID, title: title, content: content)
        } else {
            try await createDocument(title: title, content: content)
        }
    }
    
    private func createDocument(title: String, content: String) async throws {
        let requestBody = OutlineCreateDocumentRequest(
            title: title,
            text: content,
            teamId: configuration.outlineTeamID,
            publish: true
        )
        
        let encoder = JSONEncoder()
        let jsonData = try encoder.encode(requestBody)
        
        let request = HTTPClient.Request.outlineRequest(
            url: "https://wiki.awa-con.com/api/documents.create",
            apiToken: configuration.outlineAPIToken,
            body: jsonData
        )
        
        let response = try await httpClient.execute(request: request, timeout: .seconds(30))
        
        guard response.status.code == 200 else {
            let body = response.body?.getString(at: 0, length: response.body?.readableBytes ?? 0) ?? ""
            throw SyncError.httpError(Int(response.status.code), body)
        }
    }
    
    private func updateDocument(id: String, title: String, content: String) async throws {
        let requestBody = OutlineUpdateDocumentRequest(
            id: id,
            text: content,
            title: title
        )
        
        let encoder = JSONEncoder()
        let jsonData = try encoder.encode(requestBody)
        
        let request = HTTPClient.Request.outlineRequest(
            url: "https://wiki.awa-con.com/api/documents.update",
            apiToken: configuration.outlineAPIToken,
            body: jsonData
        )
        
        let response = try await httpClient.execute(request: request, timeout: .seconds(30))
        
        guard response.status.code == 200 else {
            let body = response.body?.getString(at: 0, length: response.body?.readableBytes ?? 0) ?? ""
            throw SyncError.httpError(Int(response.status.code), body)
        }
    }
}

// MARK: - Data Transformation
struct MarkdownGenerator {
    static func generateMarkdown(from records: [AirTableRecord], tableName: String) -> String {
        var markdown = """
        # \(tableName) Data Export
        
        *Last updated: \(ISO8601DateFormatter().string(from: Date()))*
        
        """
        
        guard !records.isEmpty else {
            markdown += "No records found."
            return markdown
        }
        
        // Extract all unique field names
        let allFields = Set(records.flatMap { $0.fields.keys }).sorted()
        
        // Create table header
        markdown += "| " + allFields.joined(separator: " | ") + " |\n"
        markdown += "|" + Array(repeating: "---", count: allFields.count).joined(separator: "|") + "|\n"
        
        // Add data rows
        for record in records {
            let rowValues = allFields.map { fieldName in
                formatFieldValue(record.fields[fieldName])
            }
            markdown += "| " + rowValues.joined(separator: " | ") + " |\n"
        }
        
        markdown += "\n---\n*Total records: \(records.count)*\n"
        
        return markdown
    }
    
    private static func formatFieldValue(_ value: AirTableValue?) -> String {
        guard let value = value else { return "" }
        
        switch value {
        case .string(let str):
            return str.replacingOccurrences(of: "|", with: "\\|")
        case .number(let num):
            return String(num)
        case .bool(let bool):
            return bool ? "✓" : "✗"
        case .array(let arr):
            return arr.joined(separator: ", ")
        case .null:
            return ""
        }
    }
}

// MARK: - Lambda Handler
@main
struct AirTableOutlineSyncLambda: LambdaHandler {
    typealias Event = CloudWatchEvent
    typealias Output = String
    
    private let httpClient: HTTPClient
    private let configuration: SyncConfiguration
    private let airTableService: AirTableService
    private let outlineService: OutlineService
    
    init() async throws {
        self.httpClient = HTTPClient(eventLoopGroupProvider: .createNew)
        self.configuration = try SyncConfiguration()
        self.airTableService = AirTableService(
            httpClient: httpClient,
            configuration: configuration
        )
        self.outlineService = OutlineService(
            httpClient: httpClient,
            configuration: configuration
        )
    }
    
    func handle(_ event: CloudWatchEvent, context: LambdaContext) async throws -> String {
        context.logger.info("Starting AirTable to Outline sync")
        
        do {
            // Fetch data from AirTable
            context.logger.info("Fetching records from AirTable")
            let records = try await airTableService.fetchAllRecords()
            context.logger.info("Fetched \(records.count) records")
            
            // Transform to Markdown
            let tableName = configuration.airtableTableID
            let markdownContent = MarkdownGenerator.generateMarkdown(
                from: records,
                tableName: tableName
            )
            
            // Update Outline document
            context.logger.info("Updating Outline document")
            try await outlineService.createOrUpdateDocument(
                title: "\(tableName) - AirTable Export",
                content: markdownContent
            )
            
            context.logger.info("Sync completed successfully")
            return "Sync completed: \(records.count) records processed"
            
        } catch {
            context.logger.error("Sync failed: \(error)")
            throw error
        }
    }
}

// MARK: - Package.swift Dependencies
/*
// swift-tools-version:5.9
import PackageDescription

let package = Package(
    name: "AirTableOutlineSync",
    platforms: [
        .macOS(.v13)
    ],
    dependencies: [
        .package(url: "https://github.com/swift-server/swift-aws-lambda-runtime.git", from: "1.0.0"),
        .package(url: "https://github.com/swift-server/async-http-client.git", from: "1.9.0"),
    ],
    targets: [
        .executableTarget(
            name: "AirTableOutlineSync",
            dependencies: [
                .product(name: "AWSLambdaRuntime", package: "swift-aws-lambda-runtime"),
                .product(name: "AWSLambdaEvents", package: "swift-aws-lambda-runtime"),
                .product(name: "AsyncHTTPClient", package: "async-http-client"),
            ]
        ),
    ]
)
*/
