# Creating OMS Records from Deeds

Author: Arnav Malhotra\
Date Completed: 2024-04-08\
JIRA Ticket: [DATAENG-839](https://perchwell.atlassian.net/browse/DATAENG-839)

## Context and Background

Lightbox provides a dataset that includes every deed transfer in the state. This provides a record of every property transaction in the state, whether or not they were on-market (i.e. represented by an agent) or not. In order to figure out which deed represents an off-market sale (OMS), the deeds data needs to be deduplicated against the closings that we have received from the MLS. i.e.:
Deeds = Closings + OMS => OMS = Closings - Deeds

## File Structure

- Lightbox_Documentation: directory containing documentation from Lightbox
- v1_address_matching: directory containing first draft of deeds deduplication
- v2_parcel_matching: directory containing the second and final drafts of deed deduplication as well as notebooks containing checks
- v2_parcel_matching/oms_backfill_v3: directory containing final draft of deed deduplication

## How to Run

1. Put input data in data folder
2. Change global variables in v2_parcel_matching/oms_backfill_v3/main.py to desired threshold
3. Run main.py
4. Output will be in data/output
