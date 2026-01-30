## ADDED Requirements
### Requirement: Service-based Order Inclusion
The system MUST create Sankhya orders using the native service `CACSP.incluirNota` via ServiceInvoker, with HTTP fallback to `/mge/service.sbr` if ServiceInvoker is unavailable.

#### Scenario: ServiceInvoker path
- **WHEN** an order is imported
- **AND** ServiceInvoker is available
- **THEN** the order MUST be created through `CACSP.incluirNota`
- **AND** business rules from the core MUST be enforced

#### Scenario: HTTP fallback
- **WHEN** ServiceInvoker is unavailable
- **THEN** the system MUST call `/mge/service.sbr?serviceName=CACSP.incluirNota` using the same payload

### Requirement: Status Sync Toggle
The system MUST allow disabling all outbound status updates to Fastchannel via a flag in `AD_FCCONFIG`.

#### Scenario: Status sync disabled
- **WHEN** `AD_FCCONFIG.SYNC_STATUS_ENABLED = 'N'`
- **THEN** the system MUST NOT call Fastchannel for order status updates
- **AND** MUST NOT send invoice or tracking updates
- **AND** MUST NOT call `markAsSynced`

### Requirement: Per-Order Resilience
The system MUST continue processing subsequent orders when one order fails.

#### Scenario: One order fails
- **WHEN** a single order fails to import
- **THEN** remaining orders MUST still be processed
- **AND** the failed order MUST be logged with error details

## MODIFIED Requirements
### Requirement: Order Field Parity With Legacy
The system MUST map order fields to match the legacy integration.

#### Scenario: Header fields
- **WHEN** creating the order XML
- **THEN** the header MUST include `CODTIPVENDA`, `CODVEND`, `CODNAT`, `CODCENCUS`, `VLRFRETE`, and `AD_NUMFAST`
- **AND** `AD_FASTCHANNEL_ID` MUST also be recorded for compatibility

#### Scenario: Item fields
- **WHEN** creating order items
- **THEN** each item MUST include `CODPROD`, `CODVOL`, `ORIGPROD`, `VLRUNIT`, `PERCDESC`, and `QTDNEG`

### Requirement: Product Creation Prohibited
The system MUST NOT create products from Fastchannel data.

#### Scenario: Missing product mapping
- **WHEN** an item SKU/EAN does not map to `CODPROD`
- **THEN** the order MUST fail with a logged error
- **AND** no product MUST be created
