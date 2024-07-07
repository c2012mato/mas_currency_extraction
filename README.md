```mermaid
graph TD;
    style StartNode fill:#3498db,stroke:#333,stroke-width:2px;
    style EndNode fill:#3498db,stroke:#333,stroke-width:2px;
    style ProcessNode fill:#95a5a6,stroke:#333,stroke-width:2px;
    style DataNode fill:#e74c3c,stroke:#333,stroke-width:2px;
    style ExtractNode fill:#f39c12,stroke:#333,stroke-width:2px;
    
    A[Start] --> B[Initialize DataProcessor];
    B --> C([Fetch Data from API]);
    C -->D[Fill Missing Dates];
    D --> F[Adjust FX Rates];
    F -->G[Connect to BigQuery];
    G --> H([Get Latest End of Day]);
    H --> I[Prepare Final DataFrame];
    I --> J[(Upload to BigQuery)];
    J --> K[End];

    style A fill:#3498db,stroke:#333,stroke-width:2px;
    style B fill:#95a5a6,stroke:#333,stroke-width:2px;
    style C fill:#f39c12,stroke:#333,stroke-width:2px;
    style D fill:#95a5a6,stroke:#333,stroke-width:2px;
    style F fill:#95a5a6,stroke:#333,stroke-width:2px;
    style G fill:#95a5a6,stroke:#333,stroke-width:2px;
    style H fill:#f39c12,stroke:#333,stroke-width:2px;
    style I fill:#95a5a6,stroke:#333,stroke-width:2px;
    style J fill:#e74c3c,stroke:#333,stroke-width:2px;
    style K fill:#3498db,stroke:#333,stroke-width:2px;

