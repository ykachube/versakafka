# ADR-03: Event ID Assigned Once at First Receipt

## Status
Accepted

## Context
Event IDs are critical for deduplication and reconciliation. When should they be assigned?
1. **At client** - Client generates, proxy validates
2. **At proxy first receipt** - Proxy generates on first receive, preserves thereafter
3. **At each channel** - Each channel generates independently

## Decision
**Event ID assigned once at proxy first receipt**. The proxy:
1. Checks for existing `event-id` header
2. If missing, generates based on configured mode
3. Preserves event ID across all channels and retries
4. Never regenerates for the same logical event

## Consequences
### Positive
- Single source of truth for event identity
- Enables cross-channel deduplication
- Simplifies reconciliation (stable ID)
- Works with all event ID modes

### Negative
- Must trust proxy's event ID generation
- Client cannot override (except preserve mode)
- Requires durable storage of event ID mapping

## Implementation
- `EventIdGenerator::generate()` called in filter chain post-receive
- `EventIdMode::Preserve` keeps client header, generates if missing
- Other modes always generate on first receipt
- Event ID stored in WAL and forwarded to all channels