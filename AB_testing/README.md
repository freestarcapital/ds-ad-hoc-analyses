![all_possible_events_flowchart.png](..%2F..%2F..%2FDownloads%2Fall_possible_events_flowchart.png)
```mermaid
flowchart TD
    A[All Possible Events Flowchart - Data from 20th of August] --> B{"Which data sources are available?"}

    B -->|"Nothing Available (no ASR, no AER, no BWR, no GAM) 7.6% of Sessions"| C1[user left page before any prebid auction started]
    B -->|"ASR (no AER,no BWR, no GAM) 3.3% of Sessions"| C2[user left page before any prebid auctions ended]
    B -->|"ASR + AER (no BWR, no GAM) 11.4% of Sessions"| C3[user left page before prebid bids came back and gam called, numbers suggest most requests would have been GAM+prebid]
    B -->|"ASR + AER + GAM (no BWR) 10.8% of Sessions"| C4[96% gam and prebid auctions, but no prebid bids came back]
    B -->|"ASR + AER + BWR (no GAM) 16.6% of Sessions"| C5[prebid only requests]
    B -->|"ASR + AER + BWR + GAM" 42.5% of Sessions| C6[prebid and gam]
    B -->|"ASR + GAM only (no AER, no BWR) 6.2"| C7[gam only]

    subgraph Scenario1 ["Scenario 1: nothing available"]
        C1 --> R1["Requests = not available"]
        R1 --> I1["Impressions = not available"]
        I1 --> U1["Unfilled = not available"]
        U1 --> F1["Fill Rate = not available"]
    end

    subgraph Scenario2 ["Scenario 2: ASR available"]
        C2 --> R2["Requests = ASR requests"]
        R2 --> I2["Impressions = not available"]
        I2 --> U2["Unfilled = not available"]
        U2 --> F2["Fill Rate = not available"]
    end

    subgraph Scenario3 ["Scenario 3: AER available"]
        C3 --> R3["Requests = Method 1 using BWR impr + GAM data"]
        R3 --> I3["Impressions = GAM prebid + GAM NBF + GAM A9 - GAM house"]
        I3 --> U3["Unfilled = average of 'AER unfilled + GAM house' OR 'GAM unfilled - BWR native render + GAM house'"]
        U3 --> F3["Fill Rate = Impr / Requests = 61%"]
    end

    subgraph Scenario4 ["Scenario 4: GAM available"]
        C4 --> R4["Requests = Method 1 using BWR impr + GAM data"]
        R4 --> I4["Impressions = BWR impr + GAM A9 + GAM NBF - GAM house"]
        I4 --> U4["Unfilled = AER unfilled + GAM house"]
        U4 --> F4["Fill Rate = Impr / Requests = 27%"]
    end

    subgraph Scenario5 ["Scenario 5: BWR available"]
        C5 --> R5["Requests = Method 1 close to AER requests"]
        R5 --> I5["Impressions = BWR impr + GAM A9 + GAM NBF - GAM house"]
        I5 --> U5["Unfilled = average of 'AER unfilled + GAM house' OR 'GAM unfilled - BWR native render + GAM house'"]
        U5 --> F5["Fill Rate = Impr / Requests = 71%"]
    end

    subgraph Scenario6 ["Scenario 6: BWR + GAM available"]
        C6 --> R6["Requests = Method 2 using BWR impr + AER unfilled + GAM data"]
        R6 --> I6["Impressions = BWR impr + GAM A9 + GAM NBF - GAM house"]
        I6 --> U6["Unfilled = average of 'AER unfilled + GAM house' OR 'GAM unfilled - BWR native render + GAM house'"]
        U6 --> F6["Fill Rate = Impr / Requests = 79%"]
    end

    subgraph Scenario7 ["Scenario 7: GAM only"]
        C7 --> R7["Requests = GAM unfilled + GAM A9 + GAM NBF + GAM prebid"]
        R7 --> I7["Impressions = GAM prebid + GAM NBF + GAM A9 - GAM house"]
        I7 --> U7["Unfilled = GAM unfilled - BWR native render + GAM house"]
        U7 --> F7["Fill Rate = Impr / Requests = 57%"]
    end

    %% Dummy horizontal note at bottom
    Note(["Note: Session proportions add up to 98.4, the rest 1.6% of sessions come from different data source combinations that break the business logic. The events are missing in some tables but we still include these events in the final data."])
    
     %% Style to make the note bigger
    style Note fill:circle,stroke:,font-size:16px,font-weight:bold,font-style:italic

    