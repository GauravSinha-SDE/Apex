# Senior Data Engineer — Take-Home Assessment

## Logistics

| | |
|---|---|
| **Time limit** | 2 hours (honor system) |
| **Deliverables** | Private GitHub repo OR zip file |
| **Follow-up** | A **15-20 minute live discussion** will be scheduled after submission |
| **Format** | Written design documents, DDL/SQL/PySpark code, architecture diagrams |

> **Important**: The Loom video and live discussion are mandatory components, not optional extras. In your video, narrate your design decisions, walk through your solution, and discuss trade-offs you considered. We value your reasoning and design decisions over feature completeness. If you run short on time, document what you would do next and why.

---

## Background

Apex Manufacturing operates a beverage bottling facility with three production lines producing carbonated soft drinks, still water, and juice products. The plant runs 24/7 with approximately 200 employees.

**Current state**: Data sits in silos across multiple systems — SCADA, a PI historian, SAP ERP, a custom production database, and a laboratory information management system. Plant managers rely on weekly Excel exports and manually correlate data across systems. The VP of Operations wants to change this.

**Your task**: Apex has recently adopted **Databricks** as their data platform. They need a senior data engineer to design and build the foundation for their data lakehouse.

Please review `docs/apex_plant_context.md` for detailed information about the plant's systems, equipment, business KPIs, and terminology. The `data/` directory contains sample data exports from the plant's various source systems.

> **Note**: The provided data was exported from multiple source systems with varying conventions and quality levels. Part of your task is to identify and address inconsistencies.

---

## Part 1: Data Engineering & Self-Service Analytics (~60-75 minutes)

The VP of Operations wants plant managers to be able to ask questions about their operational data in natural language. They want to ask things like:

- "What was the OEE for Line 2 last Tuesday?"
- "Show me all equipment that had more than 3 unplanned stops this month"
- "Which quality parameter is most correlated with scrap rate on Line 1?"
- "What's the average fill weight trend for product SKU CSD-001?"
- "Which technician resolved the most critical work orders last week?"

### Task 1a: Data Architecture & Quality

Using the sample data in the `data/` directory:

1. Design a data architecture on Databricks for this dataset. Describe your catalog, schema, and table organization.
2. Write DDL statements (SQL) or PySpark code for the key tables in your architecture. Show the transformations needed to get from raw source data to analytics-ready tables.
3. Identify and document the data quality issues you find in the sample data. Describe how your architecture handles each one.

### Task 1b: Natural Language Analytics

The VP's goal is for plant managers — who are not SQL-literate — to query this data using natural language.

1. What Databricks features or tools would you use to enable this? Describe your approach.
2. What steps would you take to ensure accurate results from natural language queries? Consider the challenges specific to this dataset.
3. The word "status" appears in multiple data files but means very different things depending on context. How would you handle this and similar ambiguities so that a natural language interface produces correct answers?
4. Provide concrete examples of how you would configure or prepare the data for your chosen approach.

### Task 1c: Physical Table Design

For the highest-volume table in your architecture (hint: sensor telemetry):

1. How would you optimize physical storage and query performance? Specify your choices and justify them.
2. If this table were currently using traditional partitioning with `ZORDER`, what would your migration path look like?
3. What Delta table properties would you configure and why?

---

## Part 2: Architecture Design (~30-45 minutes)

Design the end-to-end data ingestion architecture for the Apex Manufacturing lakehouse. The plant has three distinct data flow patterns:

1. **Real-time**: Vibration sensors on Line 3 publish via MQTT to an Eclipse Mosquitto broker. 100 sensors at 10 Hz = ~1,000 messages/second. The plant wants sub-minute latency for anomaly detection.

2. **Near-real-time**: The OSIsoft PI historian collects ~2,000 tags at 1-second intervals from all three lines. This data must land in the lakehouse within 5 minutes.

3. **Batch**: SAP maintenance work orders, production schedules, and LabWare LIMS quality results are updated periodically (every 30 minutes to 4 hours depending on the system).

### Deliverables

**A. Architecture Diagram**: Create a diagram (any format — hand-drawn photo, draw.io, Miro, ASCII art) showing data flow from each source system through to analytics-ready tables. Label protocols, tools, and Databricks components used at each stage.

**B. Connector Strategy**: For each source system, specify:
- Extraction method (protocol, tool, or connector)
- Landing format in the raw layer
- Specific Databricks features used for ingestion

**C. Streaming vs. Batch Justification**: For each data source, specify whether you'd use streaming, micro-batch, or batch processing. Justify each choice.

**D. Deployment & CI/CD**: Describe how you would package and deploy these pipelines across dev, staging, and production environments. What Databricks features would you use for deployment automation? Describe your project structure and configuration approach.

**E. Data Governance**: Describe your catalog and schema naming strategy. How would you implement access control so that:
- Data engineers have full access
- Data analysts have read access to cleaned/transformed data
- Plant managers can only access analytics through the natural language interface, scoped to their production line

---

## Part 3: Short Answer (~15-20 minutes)

Answer **4 of the following 6 questions** (3-8 sentences each). Choose the questions that best showcase your expertise.

1. **Pipeline Orchestration**: When would you choose Databricks Pipelines versus standard Databricks Workflows with structured streaming for a manufacturing data platform? What are the trade-offs in cost, maintainability, and monitoring?

2. **Storage Optimization**: The `sensor_readings` table accumulates ~170 million rows per day. It's currently partitioned by `date` with `ZORDER` on `(equipment_id, sensor_tag)`. Describe your migration plan to a more modern storage optimization approach. What are the risks?

3. **Serverless Compute**: The plant is evaluating a move from classic compute to serverless for their SQL warehouses and data pipelines. What factors should they consider? Are there manufacturing-specific workloads that might NOT be suitable for serverless?

4. **Semantic Disambiguation**: A plant manager asks their natural language query interface: "What's the status of Line 2?" This could mean equipment operational status, production run status, maintenance work order status, or alarm state. How would you engineer the data and the query interface to handle this correctly?

5. **Late-Arriving Data**: The PI historian occasionally delivers data 2-6 hours late due to plant network outages and store-and-forward recovery. How does this affect your streaming pipeline design? What specific Databricks features or patterns would you use to handle this?

6. **Operational Write-Back**: Apex wants a real-time equipment dashboard where operators can acknowledge alarms and add shift notes. This requires write-back capability that a traditional lakehouse doesn't natively support. How would you architect the transactional/write-back component? Are there any newer Databricks capabilities that could help here?

---

## Submission Checklist

- [ ] **Part 1**: Data architecture design with DDL/code, data quality documentation, natural language analytics approach with concrete examples, physical table design choices
- [ ] **Part 2**: Architecture diagram, connector strategy, streaming/batch justification, deployment approach, governance design
- [ ] **Part 3**: 4 short answer responses
- [ ] **README or notes**: Any assumptions, scope decisions, or "what I'd do with more time"

---

## What We're Looking For

We evaluate submissions on:

- **Design quality**: Thoughtful architecture that fits the manufacturing context, not generic patterns
- **Databricks platform knowledge**: Familiarity with current capabilities and when to use them
- **Data engineering rigor**: Handling real-world data quality issues, not just the happy path
- **IT/OT domain awareness**: Understanding of industrial data patterns, protocols, and challenges
- **Communication**: Clear documentation of decisions, trade-offs, and reasoning
- **Pragmatism**: Realistic, deployable solutions over theoretical perfection

> Correctness and clarity matter more than completeness. A well-reasoned partial solution with clear documentation of remaining work will score higher than a rushed complete solution without explanation.
