## MODIFIED Requirements

### Requirement: Price synchronization
The system MUST synchronize product prices with Fastchannel using SQL Server pricing rules (SNK_GET_PRECO), normalize units for UI/API comparison, and support multi-table pricing and batch prices for all eligible tables.

#### Scenario: Price sync uses SQL Server pricing
- **WHEN** a price update is processed
- **THEN** the system uses [sankhya].SNK_GET_PRECO(:nuTab, :codProd, GETDATE()) to compute the price

#### Scenario: Multi-table price sync
- **WHEN** eligible price tables are configured (AD_TIPO_FAST or PRICE_TABLE_IDS)
- **THEN** the system synchronizes prices for all eligible tables

#### Scenario: Batch prices
- **WHEN** quantity-based pricing exists in Sankhya
- **THEN** the system publishes batch prices to Fastchannel for each eligible table

#### Scenario: Unit normalization
- **WHEN** prices are displayed or compared in the UI
- **THEN** the system uses decimal currency values to avoid centavos/real mismatches
