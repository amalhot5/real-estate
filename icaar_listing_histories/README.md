# DATA-814: Define ICAAR Listing History Mapping Strategy

Author: Arnav Malhotra

[JIRA Ticket Link](https://perchwell.atlassian.net/browse/DATAENG-814)

## Context
The Perchwell ListingHistory model consists of a sequence of dates and corresponding prices + statuses of listings on those dates. This is fundamentally different from the way the RESO data model describes listing events — it does so with top-level fields for important dates and prices.

As part of our listing ingest process, we need to bridge the gap between the two and define the logic of mapping the RESO model’s flat fields to the Perchwell ListingHistory model. We’ve achieved this in the past for CincyMLS & CRMLS by defining a matrix that buckets of all different combinations of RESO Property listing price and date field sequences and maps them to output arrays of ListingHistory objects.

While this approach was effective for CincyMLS which had relatively clean sequences of dates and well-populated prices, we need to determine if that’s the case for ICAAR. If it’s viable, we should plan to proceed with the same approach for mapping and ingesting the ListingHistory records. If not, we’ll need to identify an alternate strategy for computing ListingHistory arrays for each Babylon Listing based on the corresponding RESO Property and its HistoryTransactional data. This alternate strategy would likely need to consist of a hybrid approach relying on both HistoryTransactional and the flat RESO Property fields.
## Acceptance Criteria
### Matrix approach: 
An ICAAR version of the Listing History Matrix we created for CincyMLS is defined

For each bucket, we determine whether it’s valid based on the following questions:

Does it leave listings in a status different from what StandardStatus specifies?

Does it indicate listing activity before ListingContractDate?

Do the HistoryTransactional records for the listings in each bucket reflect the same sequence of events?

Consider: For invalid buckets, we reach into HistoryTransactional to see what actually happened for the impacted listings

### Non-matrix approach:

Conduct an analysis to confirm whether the RESO Property date/price fields or the HistoryTransactional data are gospel/source-of-truth for the actual price and status change events for listings.

Depending on which one we determine to be the source of truth and the extent to which we can rely on both, define logic for how we can marry the two to produce a complete picture of what each ListingHistory should be.
## File Descriptions

## Attached Data
    - ICAAR reso_reso_properties
### Mapping
```
Update Date -> ModificationTimestamp
Status Date-> StatusChangeTimestamp
Price Date	-> PriceChangeTimestamp
Off Market Date -> OffMarketDate
LVT Date -> TaxYear
Listing Date -> ListingContractDate
Input Date -> OriginalEntryTimestamp
Expiration Date -> ExpirationDate
Pending Date -> PurchaseContractDate
Closing Date -> CloseDate
```
### Deliverables
1. [Bucket Mapping](https://docs.google.com/spreadsheets/d/11l2cEOXnx2aqlOIi136Sfnkow40avnaPVVeHQdNjM3Q/edit#gid=1198768936)
2. [Events based on mapping](https://docs.google.com/spreadsheets/d/1qV9pAQw8zyba6QZRE_nEfi2W4T2psrS-RgHR6ZTBIz4/edit#gid=1505216398)