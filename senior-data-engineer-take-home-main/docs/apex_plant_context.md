# Apex Manufacturing — Plant Context

## Company Overview

Apex Manufacturing operates a beverage bottling facility in Austin, TX, producing carbonated soft drinks, still water, and juice products under the **Acme Beverages** brand. The plant runs 24/7 across three shifts with approximately 200 employees.

## Production Lines

| Line | Products | Installed | Capacity | Notes |
|------|----------|-----------|----------|-------|
| Line 1 | Carbonated soft drinks | 2015 | 600 BPM | Oldest line, scheduled for PLC firmware upgrade Q3 2026 |
| Line 2 | Still water | 2019 | 800 BPM | Highest throughput line |
| Line 3 | Juice products | 2023 | 500 BPM | Newest line, includes MQTT-based vibration monitoring |

Each line consists of: Depalletizer, Rinser, Filler, Capper, Labeler, Case Packer, Palletizer, and shared CIP (Clean-In-Place) skids.

## OT Systems (Operational Technology)

### PLCs & SCADA
- **Rockwell ControlLogix** PLCs on all three lines, connected via EtherNet/IP
- **FactoryTalk View SE** SCADA for operator HMI screens and alarm management
- Tag naming follows a mix of ISA-5.1 conventions on Lines 1-2 and a simplified format on Line 3

### Historian
- **OSIsoft PI Data Archive** (v2023 R2) collecting ~2,000 tags at 1-second scan intervals
- Compression enabled with exception deviation of 0.5% for analog tags
- PI-to-PI interface for disaster recovery to secondary server

### IoT Sensors (Line 3 Only)
- 100 vibration sensors (accelerometers) on rotating equipment
- Publishing via **MQTT** to an **Eclipse Mosquitto** broker at 10 Hz per sensor
- Payload format: JSON with fields `sensor_id`, `timestamp`, `x_accel`, `y_accel`, `z_accel`, `temperature`
- Currently not integrated into PI — data stored in flat files on an edge gateway

## IT Systems (Information Technology)

### ERP
- **SAP S/4HANA** (hosted on Azure)
- **PM (Plant Maintenance) module** for work orders, preventive maintenance schedules, equipment master data
- **PP (Production Planning) module** for production orders and scheduling
- Data extracted via SAP BW/4HANA Open Hub or CDS views

### Production Database
- Custom **PostgreSQL 15** database for real-time production scheduling and shift management
- REST API available for integration
- Tables: `production_runs`, `shift_schedule`, `operator_assignments`

### Laboratory Information Management
- **LabWare LIMS v8** for quality testing
- Tests: fill weight, cap torque, Brix (sugar content), carbonation volumes, pH, microbiological
- Results exported as CSV to a shared network drive every 30 minutes

## Business KPIs

Plant leadership tracks the following metrics:

| KPI | Definition | Target |
|-----|-----------|--------|
| **OEE** | Availability x Performance x Quality | > 85% |
| **Scrap Rate** | Rejected units / Total units produced | < 2% |
| **MTBF** | Mean Time Between Failures (unplanned stops) | > 72 hours |
| **MTTR** | Mean Time To Repair | < 45 minutes |
| **First-Pass Yield** | Units passing QA on first check / Total units checked | > 98% |
| **CIP Cycle Time** | Duration of automated cleaning cycles | < 90 minutes |
| **Changeover Time** | Time to switch between products on a line | < 30 minutes |

## Current Pain Points

1. **Data silos**: Each system has its own reporting. Plant managers receive weekly Excel exports and manually correlate across systems.
2. **No real-time visibility**: The PI historian has the data, but only control room operators can access it via SCADA screens.
3. **Reactive maintenance**: Equipment failures are addressed after they occur. Vibration data from Line 3 is collected but not analyzed.
4. **Manual OEE calculation**: Production supervisors calculate OEE in spreadsheets using data from 3 different systems.
5. **Quality traceability**: Linking a quality defect back to specific sensor conditions during production requires hours of manual investigation.

## Glossary

| Term | Definition |
|------|-----------|
| **BPM** | Bottles Per Minute (not Beats Per Minute) |
| **CIP** | Clean-In-Place — automated cleaning of equipment without disassembly |
| **Changeover** | The process of switching a production line from one product to another |
| **Tag** | A named data point in SCADA/historian (e.g., `LINE1.FT-201.PV`) |
| **PV** | Process Value — the actual measured value of a sensor/tag |
| **SP** | Setpoint — the target value for a control loop |
| **OPC-UA** | Open Platform Communications Unified Architecture — industrial data protocol |
| **OPC Quality** | Status code indicating data reliability: 192 = Good, 0 = Bad, 64 = Uncertain |
| **ISA-5.1** | Instrumentation standard for tag naming (e.g., `TT` = Temperature Transmitter, `FT` = Flow Transmitter, `PT` = Pressure Transmitter) |
| **Historian** | Time-series database optimized for industrial sensor data (e.g., OSIsoft PI) |
| **SCADA** | Supervisory Control and Data Acquisition — industrial control system |
| **PLC** | Programmable Logic Controller — hardware that controls equipment |
| **HMI** | Human-Machine Interface — operator screen for monitoring/controlling equipment |
| **Downtime** | Period when equipment is not producing (planned or unplanned) |
| **SKU** | Stock Keeping Unit — unique product identifier |
